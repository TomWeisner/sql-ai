import os
from dataclasses import dataclass, field
from typing import Literal

from sql_ai.bedrock.models import MODEL_REGISTRY, Model
from sql_ai.utils.utils import find_aws_profile_by_account_id

# Keep this in sync with MODEL_REGISTRY keys
ModelKey = Literal["claude-3", "claude-3.7", "claude-4.5"]


@dataclass
class Config:
    aws_profile: str = ""
    aws_account_id: str = "382901073838"
    aws_region: str = "eu-west-2"
    aws_athena_s3_output_bucket: str = ""
    aws_athena_catalog: str = "awsdatacatalog"
    aws_athena_database: str = "default"

    bedrock_model_key: ModelKey = "claude-3.7"  # callers pass a key
    bedrock_model: Model = field(init=False)  # derived, not user-set

    max_tokens: int = 2000
    temperature: float = 0.9

    def __post_init__(self):
        if not self.aws_profile:
            env_profile = os.getenv(f"SQL_AI_AWS_PROFILE_{self.aws_account_id}")
            env_profile = env_profile or os.getenv("SQL_AI_DEFAULT_AWS_PROFILE")
            self.aws_profile = (
                env_profile
                or os.getenv("SQL_AI_FAKE_AWS_PROFILE")
                or find_aws_profile_by_account_id(self.aws_account_id)
            )

        try:
            self.bedrock_model = MODEL_REGISTRY[self.bedrock_model_key]
        except KeyError as e:
            allowed = ", ".join(MODEL_REGISTRY.keys())
            raise ValueError(f"bedrock_model_key must be one of: {allowed}") from e
