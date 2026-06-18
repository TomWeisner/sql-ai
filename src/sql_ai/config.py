import os
from dataclasses import dataclass, field, replace
from typing import Literal

from sql_ai.bedrock.models import MODEL_REGISTRY, Model
from sql_ai.utils.utils import find_aws_profile_by_account_id

# Keep this in sync with MODEL_REGISTRY keys
ModelKey = Literal[
    "claude-sonnet-3",
    "claude-sonnet-3.7",
    "claude-sonnet-4.5",
    "claude-opus-4.5",
    "claude-sonnet-4.6",
]


@dataclass
class Config:
    aws_profile: str = ""
    aws_account_id: str = "382901073838"
    aws_region: str = "eu-west-2"
    aws_athena_s3_output_bucket: str = ""
    aws_athena_catalog: str = "awsdatacatalog"
    aws_athena_database: str = "default"
    aws_redshift_cluster_identifier: str = ""
    aws_redshift_workgroup_name: str = ""
    aws_redshift_database: str = "dev"
    aws_redshift_db_user: str = ""
    aws_redshift_secret_arn: str = ""

    bedrock_model_key: ModelKey = "claude-sonnet-4.6"  # callers pass a key
    bedrock_inference_profile_id: str = ""  # ID or ARN for inference profiles
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

        if not self.bedrock_inference_profile_id:
            env_profile = os.getenv("SQL_AI_BEDROCK_INFERENCE_PROFILE_ID")
            env_profile = env_profile or os.getenv("SQL_AI_BEDROCK_INFERENCE_PROFILE_ARN")
            self.bedrock_inference_profile_id = env_profile or ""

        try:
            base_model = MODEL_REGISTRY[self.bedrock_model_key]
            if self.bedrock_inference_profile_id:
                self.bedrock_model = replace(
                    base_model, invoke_id=self.bedrock_inference_profile_id
                )
            else:
                self.bedrock_model = base_model
        except KeyError as e:
            allowed = ", ".join(MODEL_REGISTRY.keys())
            raise ValueError(
                f"bedrock_model_key ({self.bedrock_model_key}) must be one of: {allowed}"
            ) from e
