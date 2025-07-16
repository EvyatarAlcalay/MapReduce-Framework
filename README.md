# MapReduce-Framework
Thread-safe implementation of the MapReduce model in C++ using pthreads, synchronization primitives, and performance optimization.

## About this project
This is a thread-safe, high-performance implementation of the MapReduce programming model in C++ using POSIX threads (pthreads). The framework manages the entire workflow: map, sort, shuffle, and reduce phases — enabling parallel processing of large datasets with synchronization and efficient memory management.

## Why this project?
Multi-threading and synchronization are crucial skills for systems programming and performance-critical applications. This project demonstrates my ability to:
- Design and implement concurrent algorithms
- Use pthreads and synchronization primitives effectively
- Manage complex workflows with thread safety and no memory leaks
- Apply advanced C++ concepts in a real-world scenario

## Key Features
- Full MapReduce workflow with parallel map, sort, shuffle, and reduce phases
- Thread-safe job management with progress tracking
- Modular client interface for flexible task-specific map and reduce functions
- Proper synchronization using mutexes, semaphores, and atomic operations
- Efficient memory usage and error handling

## Tech stack
- C++ (C++14/17 compatible)
- POSIX threads (pthreads)
- Synchronization: mutexes, semaphores, atomics
- Linux / POSIX environment

## How to build and run
Use the provided Makefile to build the static library:

```bash
make
```
Include the library in your project and implement your own client by extending the provided client interface.

## What you’ll learn from this project
- Multi-threaded programming in C++ with pthreads
- Handling synchronization challenges and avoiding race conditions
- Structuring large-scale parallel workflows
- Designing reusable and modular code with clear separation of framework and client logic

