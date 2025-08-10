from dataclasses import dataclass


@dataclass
class Model:
    name: str
    id: str
    region: str = "eu-west-2"
    version: str = "bedrock-2023-05-31"

    def __post_init__(self):
        self.arn = f"arn:aws:bedrock:{self.region}::foundation-model/{self.id}"


claude_3_model = Model(name="Claude 3", id="anthropic.claude-3-sonnet-20240229-v1:0")

claude_37_model = Model(name="Claude 3.7", id="anthropic.claude-3-7-sonnet-20250219-v1:0")

MODEL_REGISTRY = {
    "claude-3": claude_3_model,
    "claude-3.7": claude_37_model,
}
