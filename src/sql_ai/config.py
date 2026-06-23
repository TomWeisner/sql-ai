from dataclasses import dataclass, field, replace
from typing import Literal

from sql_ai.bedrock.models import MODEL_REGISTRY, Model

# Keep this in sync with MODEL_REGISTRY keys
ModelKey = Literal[
    "claude-sonnet-3.0",
    "claude-sonnet-3.7",
    "claude-sonnet-4.5",
    "claude-opus-4.5",
    "claude-sonnet-4.6",
]


def _resolve_bedrock_model(model_key: ModelKey, inference_profile_id: str) -> Model:
    try:
        base_model = MODEL_REGISTRY[model_key]
    except KeyError as e:
        raise ValueError(
            f"model_key ({model_key}) must be one of: {allowed}"
        ) from e

    if inference_profile_id:
        return replace(base_model, invoke_id=inference_profile_id)
    return base_model


@dataclass
class AwsConfig:
    account_id: str = "382901073838"
    profile: str = ""
    region: str = "eu-west-2"


@dataclass
class BedrockConfig:
    model_key: ModelKey = "claude-sonnet-4.6"
    inference_profile_id: str = ""
    model: Model = field(init=False)
    max_tokens: int = 2000
    temperature: float = 0.9

    def __post_init__(self):
        self.refresh_model()

    def refresh_model(self) -> None:
        self.model = _resolve_bedrock_model(self.model_key, self.inference_profile_id)

    def set_model_key(self, model_key: ModelKey) -> None:
        self.model_key = model_key
        self.refresh_model()

    def set_inference_profile_id(self, value: str) -> None:
        self.inference_profile_id = value
        self.refresh_model()


@dataclass
class AthenaConfig:
    output_bucket: str = ""
    catalog: str = "awsdatacatalog"
    database: str = "default"


@dataclass
class RedshiftConfig:
    cluster_identifier: str = ""
    workgroup_name: str = ""
    database: str = "dev"
    db_user: str = ""
    secret_arn: str = ""
