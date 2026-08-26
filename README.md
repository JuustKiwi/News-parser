# News Ingest Parser

A high-performance, concurrent, zero-copy text parser built in C. 

This tool is designed as a proof-of-concept for high-throughput news media ingestion pipelines. It processes massive pipe-separated value (PSV) log files to aggregate and rank the most frequent news source domains in a fraction of a second.

## Architecture & Optimizations

To achieve maximum throughput (averaging ~8.0 GB/s on modern hardware), this parser bypasses standard buffered I/O and heap-allocation bottlenecks using several low-level systems techniques:

* **Memory Mapping :** The input file is mapped directly into the process's virtual memory space, allowing the kernel to handle demand paging and eliminating the overhead of `fread` or `getline`.
* **Zero-Copy Parsing:** Strings are never duplicated. The parser utilizes a `string_view` struct (a pointer and a length) to reference domains directly within the mapped file memory.
* **Hardware-Accelerated Scanning:** The parsing loop utilizes `memchr`, leveraging CPU SIMD instructions to scan bytes concurrently rather than evaluating characters sequentially.
* **Map-Reduce Concurrency:** Utilizing `pthreads`, the mapped memory is chunked evenly across all available CPU cores. Each thread computes a local aggregation (Map) to avoid mutex lock contention, followed by a rapid main-thread merge (Reduce).
* **O(1) Custom Hash Map:** Lookups and insertions utilize a custom open-addressing Hash Map powered by the FNV-1a hashing algorithm, ensuring optimal performance as the number of unique domains scales.

## Build Instructions

To compile the parser with maximum hardware optimizations, utilize the following GCC command:

gcc -std=c2x -O3 -march=native news-ingest-parser.c -o news -lpthread

## Usage

./news <path_to_logfile.psv>

## Expected Input Format

The parser expects a standard pipe-separated format with the domain located in the third column
[TIMESTAMP]|[ARTICLE_ID]|[SOURCE_DOMAIN]|[BYTE_SIZE]\n

I am providing a python script to generate dummy data as well. Simply run :
python3 generate_data.py