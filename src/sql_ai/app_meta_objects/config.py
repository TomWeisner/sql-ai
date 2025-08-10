from dataclasses import dataclass, field

from sql_ai.utils.utils import find_aws_profile_by_account_id
from sql_ai.bedrock.models import MODEL_REGISTRY, Model


@dataclass
class Config:
    aws_account_id: str = "688357424058"
    aws_profile: str = "personal"
    aws_region: str = "eu-west-2"
    aws_athena_output_bucket: str = ""
    bedrock_model: Model = field(default_factory=lambda: MODEL_REGISTRY["claude-3.7"])
    max_tokens: int = 2000
    temperature: float = 0.9

    def __post_init__(self):
        self.aws_profile = self.aws_profile or find_aws_profile_by_account_id(
            self.aws_account_id
        )
