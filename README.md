# C MapReduce Framework

A simple **MapReduce framework** implemented in **C** with **pthreads**.  
Supports multi-threaded execution of `map()` and `reduce()` functions, partitioning of input/intermediate data, and ordered key-value outputs.  

---

## Features

### MapReduce Pipeline
- **Map stage**: user-defined `map(key, value)` called for each input key-value pair.  
- **Group-by-key stage**: intermediate key-value pairs collected and grouped by unique key.  
- **Reduce stage**: user-defined `reduce(key, values)` called for each grouped key to produce final output.  

### Concurrency
- Executes with **multiple map and reduce threads**.  
- Input key-value pairs partitioned evenly across map threads.  
- Intermediate outputs partitioned across reduce threads.  
- Thread-safe data structures and synchronization primitives ensure correctness.  

### API
Defined in [`interface.h`](include/interface.h):
```c
// Executes MapReduce framework
void mr_exec(
    kv_pair *input,                 // input key-value pairs
    int input_size,
    void (*map)(char *key, char *val),
    void (*reduce)(char *key, char **values, int val_count),
    int num_map_threads,
    int num_reduce_threads,
    kv_pair **output,               // final output buffer
    int *output_size
);

// Emit intermediate kv pair from within map()
void mr_emit_i(char *key, char *value);


// Emit final kv pair from within reduce()
void mr_emit_f(char *key, char *value);
```

### Ordering
- Preserves input order for map().
- Groups intermediate key-value pairs by key and sorts by strcmp().
- Final outputs also sorted by key (lexicographic).

--- 

## Example
### Input
```bash
("1", "the quick brown fox")
("2", "jumps over the lazy dog")
("3", "the quick brown fox")
("4", "jumps over the lazy dog")
```

### Output (wiord count example)
```bash
("brown", "2")
("dog", "2")
("fox", "2")
("jumps", "2")
("lazy", "2")
("over", "2")
("quick", "2")
("the", "4")
```

--- 

## Build & Run
```bash
# Build with CMake
mkdir build && cd build
cmake ..
make

# Run the test executable
./mapreduce
```

--- 

## Skills Demonstrated
- C multithreading with pthreads.
- Synchronization primitives to avoid data races and deadlocks.
- Functional programming concepts: map, reduce, grouping.
- Parallel partitioning of work for efficiency.
- Memory safety and deterministic output ordering.

--- 

## Future Improvements
- Add shuffle/partition functions for more flexible key distribution.
- Add support for persistent data storage.
- Extend to handle large-scale input from files instead of in-memory arrays.

--- 

## License
This project is licensed under the [MIT LICENSE](LICENSE)
