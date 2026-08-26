# News Ingest Parser

This was an university assignemnt project, but I decided to expand it a little. It's a log parser that displays the top 10 news sources.

## Build Instructions

To compile the parser with maximum hardware optimizations, utilize the following GCC command:

gcc -std=c2x -O3 -march=native news-ingest-parser.c -o news -lpthread

## Usage

```bash
./news <path_to_logfile.psv>
```

In a second terminal run

```bash
python3 generate_data.py
```

## Expected Input Format

The parser expects a standard pipe-separated format with the domain located in the third column
[TIMESTAMP]|[ARTICLE_ID]|[SOURCE_DOMAIN]|[BYTE_SIZE]\n
