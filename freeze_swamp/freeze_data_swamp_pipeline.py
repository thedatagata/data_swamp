import dlt
import polars as pl
import duckdb
import os
import logging
from rich.console import Console
from rich.logging import RichHandler

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)

def execute_pipeline():
    def process(df: pl.DataFrame, chunk_size: int = 25000):
        total_rows = len(df)
        console.log(f"[yellow]Starting to process {total_rows} rows in chunks of {chunk_size}")
        for chunk in df.iter_slices(n_rows=chunk_size):
            console.log(f"[cyan]Processing chunk of {len(chunk)} rows")
            yield chunk.to_dicts()

    @dlt.resource(
        name="stg_users_dim",
        table_format="iceberg",
        primary_key="user_id"
    )
    def extract():
        console.log("[cyan]Connecting to DuckDB")
        conn = duckdb.connect('data_swamp.duckdb')
        console.log("[yellow]Executing query and converting to Polars DataFrame")
        df = conn.execute("SELECT * FROM source_data.stg_users_dim").pl()
        conn.close()
        console.log(f"[green]Retrieved {len(df)} rows from DuckDB")
        yield from process(df)

    console.log("[cyan]Initializing pipeline")
    pipeline = dlt.pipeline(
        pipeline_name='freeze_data_swamp',
        destination=dlt.destinations.filesystem(),
        dataset_name='staging'
    )
    
    console.log("[yellow]Starting pipeline run")
    return pipeline.run(extract())

if __name__ == "__main__":
    load_info = execute_pipeline()
    console.log(f"[green]Pipeline run completed with {load_info}")
