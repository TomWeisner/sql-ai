"""Command-line entry point for running the SQL chatbot.

How to run:
    - Streamlit UI (recommended): `python -m sql_ai.main --app cem --ui streamlit`
      (or omit flags for prompts). This launches the chat UI so you can ask
      questions and see SQL/results.
    - CLI mode: `python -m sql_ai.main --engine athena --ui cli` then type your
      questions in the terminal; generated SQL and answers print to stdout.

What to expect:
    - The app composes a SqlLLM with your chosen backend (Athena/Redshift).
    - It generates SQL from your natural-language question, formats/validates it,
      executes it, and then asks Bedrock to answer using the returned data.
    - In the UI you’ll see the steps, SQL, formatting logs, data, and answer.
"""

import argparse
import os
from pathlib import Path
from typing import Sequence

from sql_ai.config import Config
from sql_ai.sql_backend.base import SqlBackend
from sql_ai.sql_backend.table import Table
from sql_ai.sql_backends.athena.athena_backend import AthenaBackend
from sql_ai.sql_backends.redshift.redshift_backend import RedshiftBackend
from sql_ai.sql_llm import SqlLLM


def build_backend(
    engine: str,
    tables: Sequence[Table],
    config: Config,
) -> SqlBackend:
    engine = engine.lower()
    if engine == "athena":
        return AthenaBackend(
            output_bucket=config.aws_athena_s3_output_bucket,
            tables=tables,
            database=config.aws_athena_database,
            catalog=config.aws_athena_catalog,
            aws_profile=config.aws_profile,
            aws_region=config.aws_region,
        )
    if engine == "redshift":
        return RedshiftBackend(
            tables=tables,
            database=config.aws_redshift_database,
            cluster_identifier=config.aws_redshift_cluster_identifier,
            workgroup_name=config.aws_redshift_workgroup_name,
            db_user=config.aws_redshift_db_user,
            secret_arn=config.aws_redshift_secret_arn,
            aws_profile=config.aws_profile,
            aws_region=config.aws_region,
        )

    raise ValueError("Supported engines: athena, redshift.")


def load_demo_llm(app_key: str) -> tuple[SqlLLM, list[Table]]:
    app_key = app_key.lower()
    if app_key == "cem":
        from sql_ai.app_objects import cem_timetable

        return cem_timetable.CEMLLM, [cem_timetable.cem_timetable_table]
    if app_key == "pixar":
        from sql_ai.app_objects import pixar_films

        return pixar_films.PixarLLM, [pixar_films.pixar_films_table]
    raise ValueError("Unknown demo app. Choose from: cem, pixar.")


def run_streamlit_app(app_key: str):
    os.environ["SQL_AI_STREAMLIT_APP"] = app_key
    from streamlit.web import bootstrap  # lazy import

    app_path = Path(__file__).resolve().parent / "streamlit" / "app.py"
    bootstrap.run(str(app_path), False, [], {})


def main():
    parser = argparse.ArgumentParser(description="Run the SQL AI CLI.")
    parser.add_argument(
        "--engine",
        default="athena",
        help="SQL engine key (athena, redshift). Ignored if --app is provided.",
    )
    parser.add_argument(
        "--app",
        choices=["cem", "pixar"],
        help="Which app to run.",
    )
    parser.add_argument(
        "--ui",
        choices=["streamlit", "cli"],
        help="Pick the interface.",
    )
    args = parser.parse_args()

    os.environ.setdefault("SQL_AI_CLI_MODE", "1")
    ui_choice = (
        args.ui
        if args.ui
        else (input("Choose interface [streamlit]/cli: ").strip().lower() or "streamlit")
    )

    app_choice = args.app
    tables: list[Table] = []
    if not app_choice:
        selection = input("Select app [cem]/pixar: ").strip().lower()
        app_choice = selection or "cem"

    if ui_choice == "streamlit":
        run_streamlit_app(app_choice)
        return

    if app_choice:
        llm, tables = load_demo_llm(app_choice)
        config = llm.config
    else:
        config = Config()
        backend = build_backend(engine=args.engine, tables=tables, config=config)
        llm = SqlLLM(config=config, backend=backend)

    print(
        f"AWS account={config.aws_account_id}\n"
        f"Profile={config.aws_profile or '<auto>'}\n"
        f"Region={config.aws_region}"
    )

    if tables:
        print("\nTables available to the LLM:")
        for table in tables:
            print(
                f"- {table.name} | catalog={table.catalog}, " f"database={table.database}"
            )

    print("\nSQL AI CLI - type your question:\n")
    while True:
        try:
            question = input("Q: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
        if not question:
            continue
        sql_result = llm.get_sql(question)
        print("\nGenerated SQL:\n", sql_result.sql)
        df = llm.run_query(sql_result.sql)
        answer, _ = llm.question_about_data(question, df)
        print("\nAnswer:\n", answer)


if __name__ == "__main__":
    main()
