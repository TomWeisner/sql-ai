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

from sql_ai.config import AthenaConfig, AwsConfig, BedrockConfig, RedshiftConfig
from sql_ai.sql_backends import SqlBackend, Table
from sql_ai.sql_backends.athena.athena_backend import AthenaBackend
from sql_ai.sql_backends.redshift.redshift_backend import RedshiftBackend
from sql_ai.sql_llm import SqlLLM


def build_backend(
    engine: str,
    tables: Sequence[Table],
    aws_config: AwsConfig,
    athena_config: AthenaConfig,
    redshift_config: RedshiftConfig,
) -> SqlBackend:
    engine = engine.lower()
    if engine == "athena":
        return AthenaBackend(
            tables=tables,
            config=athena_config,
            aws_config=aws_config,
        )
    if engine == "redshift":
        return RedshiftBackend(
            tables=tables,
            config=redshift_config,
            aws_config=aws_config,
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
    from streamlit.web import bootstrap  # lazy import

    app_path = Path(__file__).resolve().parent / "streamlit" / "entrypoint.py"
    bootstrap.run(str(app_path), False, [app_key], {})


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
        aws_config = llm.aws_config
        bedrock_config = llm.bedrock_config
    else:
        aws_config = AwsConfig()
        bedrock_config = BedrockConfig()
        athena_config = AthenaConfig()
        redshift_config = RedshiftConfig()
        backend = build_backend(
            engine=args.engine,
            tables=tables,
            aws_config=aws_config,
            athena_config=athena_config,
            redshift_config=redshift_config,
        )
        llm = SqlLLM(
            backend=backend,
            aws_config=aws_config,
            bedrock_config=bedrock_config,
        )

    print(
        f"AWS account={aws_config.account_id}\n"
        f"Profile={aws_config.profile or '<not provided>'}\n"
        f"Region={aws_config.region}"
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
