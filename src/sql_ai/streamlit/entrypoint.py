"""Streamlit app entrypoint wiring for local UI runs."""

from __future__ import annotations

import sys

from sql_ai.app_objects.cem_timetable import CEMLLM
from sql_ai.app_objects.pixar_films import PixarLLM
from sql_ai.streamlit.app import ChatbotApp


def _resolve_app_choice(app_choice: str | None = None) -> str:
    if app_choice is None:
        args = [arg for arg in sys.argv[1:] if arg != "--"]
        if args and args[0] in {"-h", "--help"}:
            print("Usage: sql-ai [cem|pixar]")
            print("Or: streamlit run src/sql_ai/streamlit/entrypoint.py -- [cem|pixar]")
            raise SystemExit(0)
        app_choice = args[0] if args else "cem"

    resolved = app_choice.lower()
    if resolved not in {"cem", "pixar"}:
        raise ValueError("Unknown demo app. Choose from: cem, pixar.")
    return resolved


def main(app_choice: str | None = None) -> None:
    app_choice = _resolve_app_choice(app_choice)
    if app_choice == "pixar":
        question = "avg length of film?"
        chatbot = ChatbotApp(
            athena_llm=PixarLLM,
            title="Pixar",
            default_question=question,
        )
    else:
        question = "how many trains between kgx and edinburgh last week?"
        chatbot = ChatbotApp(
            athena_llm=CEMLLM,
            title="CEM Timetable",
            default_question=question,
        )

    chatbot.run()


if __name__ == "__main__":
    main()
