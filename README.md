# DuckJSONLyzer - Universal JSONL Analyzer

## Introduction

DuckJSONLyzer is a robust and versatile tool for processing and analyzing JSONL (JSON Lines) files of any structure. DuckJSONLyzer provides valuable insights into the composition and distribution of data within JSONL files, making it an essential tool for data analysts, engineers, and scientists working with JSON-structured data. It is also helpful to analyze cardinality of data to use smart formats of some database features like Clickhouse's LowCardinality.

### Key Features:
- Dynamic schema inference
- Flexible field analysis
- Configurable output formats (TSV, CSV, JSON)
- Scalable processing using DuckDB
- Support for nested JSON structures

## Why DuckDB?

DuckJSONLyzer leverages DuckDB, an embedded analytical database, for several compelling reasons:

1. **Performance**: DuckDB is designed for analytical queries and can process large amounts of data quickly, often outperforming traditional SQL databases for read-heavy workloads.
2. **Embedded Nature**: As an embedded database, DuckDB doesn't require a separate server process, simplifying deployment and usage.
3. **Column-Oriented Storage**: This design is optimal for analytical queries, allowing for efficient aggregations and scans over large datasets.
4. **SQL Support**: DuckDB supports a wide range of SQL operations, enabling complex data manipulations and analyses.
5. **Memory Efficiency**: DuckDB can handle datasets larger than available RAM through intelligent buffer management and spilling to disk when necessary.

DuckDB can efficiently process gigabytes to terabytes of data, depending on available system resources. For extremely large datasets (multiple terabytes), you may need to consider distributed processing solutions.

## How It Works

1. **Schema Inference**: DuckJSONLyzer first analyzes a sample of the input JSONL file to infer the schema, including nested structures.
2. **Data Loading**: It processes the entire file in chunks, flattening nested structures and loading the data into a DuckDB table.
3. **Report Generation**: Finally, it generates reports for each field, counting the occurrences of each unique value.

## Input Example

A JSONL file consists of one JSON object per line. For example:

```jsonl
{"id": 1, "name": "Alice", "age": 30, "hobbies": ["reading", "swimming"]}
{"id": 2, "name": "Bob", "age": 25, "hobbies": ["gaming", "cooking"]}
{"id": 3, "name": "Charlie", "age": 35, "hobbies": ["traveling", "photography"]}
```

## Output Example

For the "age" field, the output in TSV format might look like:

```tsv
Count   Value
2       30
1       25
1       35
```

## Data Integrity

DuckJSONLyzer helps maintain data integrity by:

1. **Identifying Inconsistencies**: By analyzing value distributions, it can highlight unexpected values or patterns.
2. **Type Inference**: The schema inference process reveals the data types used in each field, helping identify type inconsistencies.
3. **Null Value Analysis**: It shows the count of null values for each field, which can indicate data completeness issues.
4. **Cardinality Assessment**: The tool helps in understanding the cardinality of each field, which can be crucial for data modeling and query optimization.

## Database Schema Design

DuckJSONLyzer is invaluable for database schema design:

1. **Field Discovery**: It uncovers all fields present in the JSONL data, including nested structures, ensuring no data is overlooked in schema design.
2. **Data Type Suggestion**: By inferring data types, it provides a starting point for choosing appropriate database column types.
3. **Cardinality Insights**: Understanding the number of unique values in each field helps in deciding on indexing strategies and choosing between normalized and denormalized designs.
4. **Nested Structure Handling**: It reveals nested structures in the data, allowing for informed decisions on whether to normalize these structures or store them as JSON/JSONB in supporting databases.

## Usage

```bash
python jsonl_analyzer.py [OPTIONS] INPUT_FILE
```

### Options:
- `--output-dir, -o`: Directory to save output files (default: current directory)
- `--fields, -f`: Fields to generate reports for (default: all fields)
- `--top-results`: Limit the number of results in each report
- `--db-file`: DuckDB database file (default: in-memory database)
- `--chunk-size`: Chunk size for processing JSONL (default: 1000)
- `--output-format`: Output format for reports (choices: tsv, csv, json; default: tsv)
- `--max-depth`: Maximum depth for nested field analysis
- `--dry-run`: Show what would be done without actually processing

## Performance and Scalability

- DuckJSONLyzer can handle large JSONL files efficiently due to chunk-based processing and DuckDB's performance.
- For very large files, consider increasing the chunk size and using a file-based DuckDB database instead of in-memory processing.
- The `max-depth` option can limit processing time for deeply nested structures at the cost of detail in the analysis.

## Best Practices

1. Start with a small sample of your data to understand the structure and adjust options accordingly.
2. Use the `--dry-run` option to preview the operation before processing large files.
3. When dealing with large files, use a file-based DuckDB database and adjust the chunk size for optimal performance.
4. Utilize the `--fields` option to focus on specific fields of interest in large datasets.

## Troubleshooting

- If you encounter memory issues, try reducing the chunk size or using a file-based DuckDB database.
- For errors related to JSON parsing, check your input file for malformed JSON objects.
- If certain fields are missing from the analysis, ensure that the `max-depth` is set high enough to capture all nested levels.

## Future Development

Potential areas for improvement include:
- Parallel processing for even faster analysis of large datasets
- More advanced statistical analyses of field values
- Integration with data visualization tools for graphical reporting