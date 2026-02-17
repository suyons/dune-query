import time
from io import BytesIO

import dotenv
import pandas as pd
from dune_client.client import DuneClient
from dune_client.models import ExecutionState

# Constants
ETH_VOLUME_QUERY = """
SELECT 
    date_trunc('hour', et.block_time) AS block_datetime,
    round(sum(et.value / 1e18), 2) AS eth_volume,
    round(sum(et.value / 1e18) * any_value(ph.price), 2) AS usd_volume
FROM ethereum.transactions et
LEFT JOIN prices.hour ph
    ON date_trunc('hour', et.block_time) = date_trunc('hour', ph.timestamp)
    AND blockchain = 'ethereum'
    AND symbol = 'WETH'
WHERE et.block_time > now() - interval '24' hour
GROUP BY 1
ORDER BY 1 ASC;
"""


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


def main():
    dune = get_dune_client()
    if not dune:
        print("Failed to initialize Dune client.")
        return
    try:
        df = get_query_results_dataframe(dune, ETH_VOLUME_QUERY)
        print(df)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
