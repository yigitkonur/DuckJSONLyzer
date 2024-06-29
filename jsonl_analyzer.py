import click
import duckdb
import csv
import json
import os
from tqdm import tqdm
import logging
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def infer_schema(input_file, sample_size=1000, max_depth=None):
    schema = defaultdict(set)
    
    def process_item(item, prefix='', current_depth=0):
        if max_depth is not None and current_depth > max_depth:
            return
        for key, value in item.items():
            full_key = f"{prefix}{key}"
            if isinstance(value, dict):
                process_item(value, f"{full_key}.", current_depth + 1)
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    process_item(value[0], f"{full_key}.", current_depth + 1)
                else:
                    schema[full_key].add('array')
            else:
                schema[full_key].add(type(value).__name__)

    with tqdm(total=sample_size, desc="Inferring schema") as pbar:
        with open(input_file, 'r') as f:
            for i, line in enumerate(f):
                if i >= sample_size:
                    break
                try:
                    data = json.loads(line)
                    process_item(data)
                    pbar.update(1)
                except json.JSONDecodeError:
                    logging.warning(f"Invalid JSON on line {i+1}")

    return {k: list(v) for k, v in schema.items()}

def create_table(conn, schema):
    columns = []
    for field, types in schema.items():
        if len(types) == 1:
            sql_type = {
                'str': 'VARCHAR',
                'int': 'INTEGER',
                'float': 'FLOAT',
                'bool': 'BOOLEAN',
                'NoneType': 'VARCHAR',
                'array': 'VARCHAR'
            }.get(types[0], 'VARCHAR')
        else:
            sql_type = 'VARCHAR'
        columns.append(f"`{field}` {sql_type}")
    
    create_query = f"CREATE TABLE data ({', '.join(columns)})"
    conn.execute(create_query)

def process_jsonl(conn, input_file, chunk_size, max_depth=None):
    schema = infer_schema(input_file, max_depth=max_depth)
    create_table(conn, schema)

    def flatten_json(data, prefix='', current_depth=0):
        items = {}
        if max_depth is not None and current_depth > max_depth:
            return {prefix.rstrip('.'): json.dumps(data)}
        for key, value in data.items():
            new_key = f"{prefix}{key}"
            if isinstance(value, dict):
                items.update(flatten_json(value, f"{new_key}.", current_depth + 1))
            elif isinstance(value, list):
                items[new_key] = json.dumps(value)
            else:
                items[new_key] = value
        return items

    with open(input_file, 'r') as f:
        reader = csv.reader(f)
        chunks = []
        for row in tqdm(reader, desc="Processing JSONL"):
            try:
                data = json.loads(row[0])
                flattened = flatten_json(data)
                chunks.append(json.dumps(flattened))
                if len(chunks) >= chunk_size:
                    conn.execute("INSERT INTO data SELECT * FROM read_json_auto(?)", ['\n'.join(chunks)])
                    chunks = []
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON: {row[0][:100]}...")
        if chunks:
            conn.execute("INSERT INTO data SELECT * FROM read_json_auto(?)", ['\n'.join(chunks)])

def generate_report(conn, field, top_results=None):
    query = f"""
    SELECT COUNT(*) as count, `{field}` as value
    FROM data
    WHERE `{field}` IS NOT NULL
    GROUP BY `{field}`
    ORDER BY count DESC
    """
    if top_results:
        query += f" LIMIT {top_results}"
    return conn.execute(query).fetchall()

def write_report(data, output_file, output_format):
    if output_format == 'json':
        with open(output_file, 'w') as f:
            json.dump([dict(zip(['count', 'value'], row)) for row in data], f, indent=2)
    else:
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t' if output_format == 'tsv' else ',')
            writer.writerow(['Count', 'Value'])
            writer.writerows(data)

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--output-dir', '-o', default='.', help='Directory to save output files')
@click.option('--fields', '-f', multiple=True, help='Fields to generate reports for (default: all)')
@click.option('--top-results', type=int, help='Limit the number of results in each report')
@click.option('--db-file', default=':memory:', help='DuckDB database file')
@click.option('--chunk-size', type=int, default=1000, help='Chunk size for processing JSONL')
@click.option('--output-format', type=click.Choice(['tsv', 'csv', 'json']), default='tsv', help='Output format for reports')
@click.option('--max-depth', type=int, help='Maximum depth for nested field analysis')
@click.option('--dry-run', is_flag=True, help='Show what would be done without actually processing')
def main(input_file, output_dir, fields, top_results, db_file, chunk_size, output_format, max_depth, dry_run):
    """Process JSONL file and generate reports."""
    if dry_run:
        click.echo("Dry run mode:")
        click.echo(f"Input file: {input_file}")
        click.echo(f"Output directory: {output_dir}")
        click.echo(f"Fields: {fields or 'All'}")
        click.echo(f"Top results: {top_results or 'All'}")
        click.echo(f"Database file: {db_file}")
        click.echo(f"Chunk size: {chunk_size}")
        click.echo(f"Output format: {output_format}")
        click.echo(f"Max depth: {max_depth or 'No limit'}")
        return

    os.makedirs(output_dir, exist_ok=True)
    conn = duckdb.connect(db_file)

    try:
        process_jsonl(conn, input_file, chunk_size, max_depth)

        schema = infer_schema(input_file, max_depth=max_depth)
        fields_to_process = fields or schema.keys()

        for field in fields_to_process:
            data = generate_report(conn, field, top_results)
            output_file = os.path.join(output_dir, f"{field.replace('.', '_')}_report.{output_format}")
            write_report(data, output_file, output_format)
            logging.info(f"Generated report for field: {field}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        conn.close()

    logging.info("Processing complete.")

if __name__ == "__main__":
    main()