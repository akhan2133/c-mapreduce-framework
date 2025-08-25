cmake_minimum_required(VERSION 3.5)
project(mapreduce C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# gather our .c files
file(GLOB SRC_FILES
  "${PROJECT_SOURCE_DIR}/src/*.c"
)

# build the mapreduce exec
add_executable(mapreduce ${SRC_FILES})

target_include_directories(${PROJECT_NAME} 
  PUBLIC "${PROJECT_SOURCE_DIR}/include"
)

# link in pthreads
find_package(Threads REQUIRED)
target_link_libraries(mapreduce PRIVATE Threads::Threads)
