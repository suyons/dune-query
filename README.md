# Dune Query

Archive for querying Dune Analytics using Python. This project provides a simple interface to execute SQL queries on Dune and retrieve results as pandas DataFrames.

## Setup

1. Install dependencies:
   ```bash
   uv sync
   ```
2. Create a `.env` file and add your Dune API Key:
   ```
   cp .env.example .env
   ```
3. Then edit `.env` to include your API key:
   ```
   DUNE_API_KEY=your_api_key_here
   ```

## Running Examples

### Simple Query

Run the basic example in `main.py`:

```bash
uv run src/main.py --sql sql/example_query.sql
```
