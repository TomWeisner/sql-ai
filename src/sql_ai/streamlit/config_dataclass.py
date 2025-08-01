from dataclasses import dataclass

from sql_ai.utils.utils import find_aws_profile_by_account_id


@dataclass
class Config:
    aws_account_id: str = "688357424058"
    aws_profile: str = ""
    aws_region: str = "eu-west-2"
    aws_athena_output_bucket: str = ""
    aws_bedrock_model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0"
    aws_bedrock_model_version: str = "bedrock-2023-05-31"
    max_tokens: int = 2000
    temperature: float = 0.9

    def __post_init__(self):
        self.aws_profile = self.aws_profile or find_aws_profile_by_account_id(
            self.aws_account_id
        )
