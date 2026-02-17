import argparse
import os
import tempfile
import time
import webbrowser
from io import BytesIO

import dotenv
import pandas as pd
from dune_client.client import DuneClient
from dune_client.models import ExecutionState


def get_dune_client() -> DuneClient:
    dotenv.load_dotenv()
    return DuneClient()


def get_query_results_dataframe(client: DuneClient, sql: str) -> pd.DataFrame:
    """Executes SQL on Dune, waits for completion, and returns a pandas DataFrame."""
    job = client.execute_sql(query_sql=sql)
    execution_id = job.execution_id

    # Wait for execution to complete
    while True:
        status = client.get_execution_status(job_id=execution_id)
        if status.state == ExecutionState.COMPLETED:
            break
        elif status.state in [ExecutionState.FAILED, ExecutionState.CANCELLED]:
            raise Exception(f"Query execution state: {status.state}")
        time.sleep(1)

    result_csv = client.get_execution_results_csv(job_id=execution_id)
    return pd.read_csv(BytesIO(result_csv.data.read()))


def parse_args() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(description="Run a SQL query on Dune Analytics.")
    parser.add_argument("--sql", type=str, help="Path to a SQL file to execute.")
    return parser.parse_args()


def load_sql_from_file(file_path: str) -> str:
    """Reads and validates SQL from a file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file '{file_path}' not found.")

    with open(file_path, "r") as f:
        sql_content = f.read()

    if not sql_content.strip():
        raise ValueError(f"SQL file '{file_path}' is empty.")

    return sql_content


def display_dataframe_in_web(df: pd.DataFrame):
    """Opens the DataFrame in a temporary HTML file in the browser for a GUI-like experience."""
    template_path = os.path.join(os.path.dirname(__file__), "result_template.html")
    with open(template_path, "r") as f:
        template = f.read()

    html = template.replace("{row_count}", str(len(df))).replace(
        "{table_content}", df.to_html(classes="table table-hover", index=False)
    )

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as f:
        f.write(html.encode("utf-8"))
        webbrowser.open(f"file://{os.path.abspath(f.name)}")


def main():
    args = parse_args()

    try:
        # Load SQL from file if provided, otherwise fallback to default
        if not args.sql or args.sql.split(".")[-1].lower() != "sql":
            print("No SQL file provided. Exiting.")
            return
        sql_query = load_sql_from_file(args.sql)
        dune = get_dune_client()
        if not dune:
            print("Failed to initialize Dune client.")
            return
        df = get_query_results_dataframe(dune, sql_query)
        if df.empty:
            print("No results returned from the query.")
            return
        print("Results retrieved successfully. Opening web browser.")
        display_dataframe_in_web(df)

    except (FileNotFoundError, ValueError) as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"Execution Error: {e}")


if __name__ == "__main__":
    main()
