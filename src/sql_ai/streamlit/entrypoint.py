"""Streamlit app entrypoint wiring for local UI runs."""

from __future__ import annotations

import os

from sql_ai.app_objects.cem_timetable import CEMLLM
from sql_ai.app_objects.pixar_films import PixarLLM
from sql_ai.streamlit.app import ChatbotApp


def main() -> None:
    app_choice = os.getenv("SQL_AI_STREAMLIT_APP", "cem").lower()
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
