import json

import pandas as pd

from sql_ai.tracking.decorator import track_step_and_log


def wrap_message_in_body(
    message: str,
    max_tokens: int = 200,
    temperature: float = 0.7,
    top_p: float = 0.9,
) -> dict:
    return {
        "messages": [{"role": "user", "content": message}],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
    }


def data_to_prompt(data: pd.DataFrame) -> str:
    if data.shape[0] == 0:
        return "No data found."

    prompt = "Here is the query result data:\n"
    for row in data.to_dict(orient="records"):
        row_text = ", ".join(f"{k}: {v}" for k, v in row.items())
        prompt += f"{row_text}\n"
    return prompt


@track_step_and_log("🏃 Running LLM model on prompt")
def call_model_direct(
    body: dict[str, str],
    bedrock_runtime_client,
    model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
    model_version: str = "bedrock-2023-05-31",
) -> str:

    if not bedrock_runtime_client:
        raise ValueError("bedrock_runtime_client is required")

    if "anthropic" in model_id and "anthropic_version" not in body:
        body["anthropic_version"] = model_version

    response = bedrock_runtime_client.invoke_model(
        modelId=model_id, body=json.dumps(body), contentType="application/json"
    )

    response_body = json.loads(response["body"].read())
    return response_body["content"][0]["text"].strip()
