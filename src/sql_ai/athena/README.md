# Athena based LLMs

This directory is concerned with running LLMs on structured data stored in Athena databases

The AthenaLLM class handles prompts. In turn this calls the SQLPrompt class, which can be custom implemented
with child classes if looking to provide additional/custom guidelines/context on how to build SQL queries.

## Class relationships

![Athena LLM classes](classes.excalidraw.png)

## Answering user questions

Athena LLM class process flow:
![Athena LLM class](athena_llm.excalidraw.png)

## Building SQL

SQL Prompt class process flow:
![SQL Prompt class](sql_prompting.excalidraw.png)
