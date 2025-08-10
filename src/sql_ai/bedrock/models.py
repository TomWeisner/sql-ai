from dataclasses import dataclass


@dataclass
class Model:
    name: str
    id: str
    max_tokens: int = 2000
    temperature: float = 0.9
    top_p: float = 0.9
    region: str = "eu-west-2"
    version: str = "bedrock-2023-05-31"

    def __post_init__(self):
        self.arn = f"arn:aws:bedrock:{self.region}::foundation-model/{self.id}"
        assert 1 <= self.max_tokens <= 10000


claude_3_model = Model(name="Claude 3", id="anthropic.claude-3-sonnet-20240229-v1:0")

claude_37_model = Model(name="Claude 3.7", id="anthropic.claude-3-7-sonnet-20250219-v1:0")

MODEL_REGISTRY = {
    "claude-3": claude_3_model,
    "claude-3.7": claude_37_model,
}
