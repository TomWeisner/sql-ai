from dataclasses import dataclass
from typing import Optional

from sql_ai.utils.utils import find_aws_profile_by_account_id


@dataclass
class Config:
    aws_account_id: Optional[str] = "688357424058"
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = "eu-west-2"
    aws_athena_output_bucket: Optional[str] = None
    aws_bedrock_model_id: Optional[str] = "anthropic.claude-3-sonnet-20240229-v1:0"
    aws_bedrock_model_version: Optional[str] = "bedrock-2023-05-31"
    max_tokens: Optional[int] = 2000
    temperature: Optional[float] = 0.9

    def __post_init__(self):
        self.aws_profile = self.aws_profile or find_aws_profile_by_account_id(
            self.aws_account_id
        )
