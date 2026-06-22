import json
from typing import Any, TypedDict

import pandas as pd

from sql_ai.bedrock.models import Model
from sql_ai.tracking.decorator import track_step_and_log


class PromptBody(TypedDict, total=False):
    messages: list[dict]  # [{"role": "user", "content": "..."}]
    max_tokens: int
    temperature: float
    top_p: float
    anthropic_version: str


class BedrockService:
    """
    Small helper around Bedrock runtime that:
      - builds chat bodies
      - converts DataFrames to prompt text
      - calls the model and returns the first text part
    """

    def __init__(
        self,
        bedrock_runtime_client: Any,
    ) -> None:
        if not bedrock_runtime_client:
            raise ValueError("bedrock_runtime_client is required")

        self.client = bedrock_runtime_client

    # -------- Body / prompt helpers --------
    @staticmethod
    def build_body(
        message: str,
        model: Model,
    ) -> PromptBody:
        """Create a Bedrock chat body for a single-user message."""
        body = PromptBody(
            messages=[{"role": "user", "content": message}],
            max_tokens=model.max_tokens,
        )
        if model.temperature is not None:
            body["temperature"] = model.temperature
        if model.top_p is not None:
            body["top_p"] = model.top_p
        return BedrockService._ensure_provider_fields(body, model=model)

    @staticmethod
    def data_to_prompt(data: pd.DataFrame) -> str:
        """Serialize a small tabular result into a compact text snippet."""
        if data.shape[0] == 0:
            return "No data found."

        columns = list(data.columns)
        column_labels = [str(column) for column in columns]
        rename_map: dict[object, str] = {}
        if column_labels and all(label.startswith("_col") for label in column_labels):
            if len(columns) == 1:
                rename_map[columns[0]] = "result"
            else:
                rename_map = {
                    column: f"col{idx + 1}" for idx, column in enumerate(columns)
                }
        else:
            for idx, (column, label) in enumerate(zip(columns, column_labels)):
                if not label or label.startswith("_col"):
                    rename_map[column] = f"col{idx + 1}"
        if rename_map:
            data = data.rename(columns=rename_map)

        lines = ["Here is the query result data:"]
        for row in data.to_dict(orient="records"):
            lines.append(", ".join(f"{k}: {v}" for k, v in row.items()))
        return "\n".join(lines)

    # -------- Invocation --------

    @track_step_and_log("🏃 Running LLM model on prompt")
    def call(self, body: PromptBody, model: Model) -> str:
        """
        Invoke the Bedrock model and return the first text chunk.
        Pass a body built by `build_body()` (or your own dict with the same shape).
        """
        body = self._ensure_provider_fields(body, model=model)

        try:
            response = self.client.invoke_model(
                modelId=model.invoke_model_id,
                body=json.dumps(dict(body)),
                contentType="application/json",
            )
        except Exception as e:
            model_bits = [f"modelName={model.name}", f"modelId={model.invoke_model_id}"]
            if model.invoke_model_id != model.id:
                model_bits.append(f"baseModelId={model.id}")
            raise RuntimeError(
                "Bedrock invoke_model failed (" + ", ".join(model_bits) + f"): {e}"
            ) from e

        response_body = json.loads(response["body"].read())
        return response_body["content"][0]["text"].strip()

    @staticmethod
    def _ensure_provider_fields(body: PromptBody, model: Model) -> PromptBody:
        """
        Add provider-specific fields if missing (e.g., Anthropic version).
        """
        if "anthropic" in model.id and "anthropic_version" not in body:
            body["anthropic_version"] = model.version
        return body
