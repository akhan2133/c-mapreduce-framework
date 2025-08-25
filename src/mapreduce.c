#include "interface.h"
#include <pthread.h> // for phase 2
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAP 1024

// struct for intermediate and final buffers
typedef struct {
  char key[MAX_KEY_SIZE];
  char value[MAX_VALUE_SIZE];
} kv_pair_t;

// intermediate buffer (map -> group-by)
static kv_pair_t *inter_kvs = NULL;
static size_t inter_count = 0;
static size_t inter_cap = 0;

// final buffer (reduce 0> output)
static kv_pair_t *final_kvs = NULL;
static size_t final_count = 0;
static size_t final_cap = 0;

// phase 2: protect inter_kvs with a mutex
static pthread_mutex_t inter_lock = PTHREAD_MUTEX_INITIALIZER;

// phase 3: final mutex
static pthread_mutex_t final_lock = PTHREAD_MUTEX_INITIALIZER;

// called by map() to emit one intermediate pair
int mr_emit_i(const char *key, const char *value) {
  pthread_mutex_lock(&inter_lock);
  if (inter_count >= inter_cap) {
    size_t newcap = inter_cap ? inter_cap * 2 : INITIAL_CAP;
    kv_pair_t *tmp = realloc(inter_kvs, newcap * sizeof(*tmp));
    if (!tmp) {
      pthread_mutex_unlock(&inter_lock);
      return -1;
    }
    inter_kvs = tmp;
    inter_cap = newcap;
  }
  strncpy(inter_kvs[inter_count].key, key, MAX_KEY_SIZE);
  strncpy(inter_kvs[inter_count].value, value, MAX_VALUE_SIZE);
  inter_count++;
  pthread_mutex_unlock(&inter_lock);
  return 0;
}

// called by reduce() to emit one final pair
int mr_emit_f(const char *key, const char *value) {
  pthread_mutex_lock(&final_lock);
  if (final_count >= final_cap) {
    size_t newcap = final_cap ? final_cap * 2 : INITIAL_CAP;
    kv_pair_t *tmp = realloc(final_kvs, newcap * sizeof(*tmp));
    if (!tmp) {
      pthread_mutex_unlock(&final_lock);
      return -1;
    }
    final_kvs = tmp;
    final_cap = newcap;
  }
  strncpy(final_kvs[final_count].key, key, MAX_KEY_SIZE);
  strncpy(final_kvs[final_count].value, value, MAX_VALUE_SIZE);
  final_count++;
  pthread_mutex_unlock(&final_lock);
  return 0;
}

// qsort comparator for kv_pair_t by key
static int cmp_kv(const void *a, const void *b) {
  const kv_pair_t *A = a, *B = b;
  return strcmp(A->key, B->key);
}

// phase 2: thread wrapper to call map() on slice of inputs
typedef struct {
  const struct mr_in_kv *kv_lst;
  size_t start, end;
  void (*map_fn)(const struct mr_in_kv *);
} map_task_t;

static void *map_worker(void *arg) {
  map_task_t *task = arg;
  for (size_t i = task->start; i < task->end; i++) {
    task->map_fn(&task->kv_lst[i]);
  }
  return NULL;
}

// phase 3 reduce worker
typedef struct {
  struct mr_out_kv *groups;
  size_t start, end;
  void (*reduce_fn)(const struct mr_out_kv *);
} reduce_task_t;

static void *reduce_worker(void *arg) {
  reduce_task_t *t = arg;
  for (size_t i = t->start; i < t->end; i++) {
    t->reduce_fn(&t->groups[i]);
  }
  return NULL;
}

int mr_exec(const struct mr_input *input, void (*map)(const struct mr_in_kv *),
            size_t mapper_count, void (*reduce)(const struct mr_out_kv *),
            size_t reducer_count, struct mr_output *output) {
  // reset our global buffers
  inter_count = 0;
  final_count = 0;

  // MAP stage (Phase 2)
  if (mapper_count <= 1) {
    // single threaded as before
    for (size_t i = 0; i < input->count; i++) {
      map(&input->kv_lst[i]);
    }
  } else {
    // multi-threaded: partition input into mapper_count slices
    pthread_t threads[mapper_count];
    map_task_t tasks[mapper_count];

    size_t N = input->count;
    size_t base = N / mapper_count;
    size_t rem = N % mapper_count;
    size_t offset = 0;

    for (size_t t = 0; t < mapper_count; t++) {
      size_t chunk_size = base + (t < rem ? 1 : 0);
      tasks[t].kv_lst = input->kv_lst;
      tasks[t].start = offset;
      tasks[t].end = offset + chunk_size;
      tasks[t].map_fn = map;
      pthread_create(&threads[t], NULL, map_worker, &tasks[t]);
      offset += chunk_size;
    }

    // wait for all mappers
    for (size_t t = 0; t < mapper_count; t++) {
      pthread_join(threads[t], NULL);
    }
  }

  // rest of phase 1:
  // if no intermediate data, we’re done
  if (inter_count == 0) {
    output->kv_lst = NULL;
    output->count = 0;
    return 0;
  }

  // group by-key stage
  // sort intermediate <key,value> pairs
  qsort(inter_kvs, inter_count, sizeof(kv_pair_t), cmp_kv);

  // allocate worst-case: one group per intermediate pair
  struct mr_out_kv *groups = malloc(inter_count * sizeof(*groups));
  if (!groups)
    return -1;
  size_t gcount = 0;

  // walk sorted inter_kvs and build mr_out_kv entries
  for (size_t i = 0; i < inter_count;) {
    // start a new group at i
    strncpy(groups[gcount].key, inter_kvs[i].key, MAX_KEY_SIZE);

    // count how many consecutive share this key
    size_t j = i + 1;
    while (j < inter_count && strcmp(inter_kvs[j].key, inter_kvs[i].key) == 0) {
      j++;
    }
    size_t vals = j - i;

    // allocate that many values
    groups[gcount].value = malloc(vals * sizeof(*groups[gcount].value));
    if (!groups[gcount].value) {
      free(groups);
      return -1;
    }
    groups[gcount].count = vals;

    // copy them in
    for (size_t k = 0; k < vals; k++) {
      strncpy(groups[gcount].value[k], inter_kvs[i + k].value, MAX_VALUE_SIZE);
    }
    gcount++;
    i = j;
  }

  // reduce stage (phase 3)
  if (reducer_count <= 1) {
    for (size_t i = 0; i < gcount; i++) {
      reduce(&groups[i]);
    }
  } else {
    pthread_t threads[reducer_count];
    reduce_task_t tasks[reducer_count];
    size_t R = gcount;
    size_t base = R / reducer_count;
    size_t rem = R % reducer_count;
    size_t off = 0;

    for (size_t t = 0; t < reducer_count; t++) {
      size_t sz = base + (t < rem ? 1 : 0);
      tasks[t].groups = groups;
      tasks[t].start = off;
      tasks[t].end = off + sz;
      tasks[t].reduce_fn = reduce;
      pthread_create(&threads[t], NULL, reduce_worker, &tasks[t]);
      off += sz;
    }
    for (size_t t = 0; t < reducer_count; t++) {
      pthread_join(threads[t], NULL);
    }
  }

  // clean up buffers
  for (size_t i = 0; i < gcount; i++) {
    free(groups[i].value);
  }
  free(groups);

  // final sort and pack into output
  // sort final_kys by key
  qsort(final_kvs, final_count, sizeof(kv_pair_t), cmp_kv);

  // allocate output->kv_lst
  output->kv_lst = malloc(final_count * sizeof(*output->kv_lst));
  if (!output->kv_lst)
    return -1;
  output->count = final_count;

  // copy into mr_output format (each has exactly one value)
  for (size_t i = 0; i < final_count; i++) {
    strncpy(output->kv_lst[i].key, final_kvs[i].key, MAX_KEY_SIZE);
    output->kv_lst[i].count = 1;
    output->kv_lst[i].value = malloc(sizeof(*output->kv_lst[i].value));
    strncpy(output->kv_lst[i].value[0], final_kvs[i].value, MAX_VALUE_SIZE);
  }
  return 0;
}
