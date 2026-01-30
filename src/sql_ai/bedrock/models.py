from dataclasses import dataclass
from typing import Optional


@dataclass
class Model:
    name: str
    id: str
    invoke_id: str = ""
    max_tokens: int = 2000
    temperature: Optional[float] = 0.9
    top_p: Optional[float] = 0.9
    region: str = "eu-west-2"
    version: str = "bedrock-2023-05-31"

    def __post_init__(self):
        self.arn = f"arn:aws:bedrock:{self.region}::foundation-model/{self.id}"
        assert 1 <= self.max_tokens <= 10000
        if self.temperature is not None:
            assert 0 <= self.temperature <= 1
        if self.top_p is not None:
            assert 0 <= self.top_p <= 1

    @property
    def invoke_model_id(self) -> str:
        return self.invoke_id or self.id


claude_sonnet_3_model = Model(
    name="Claude Sonnet 3", id="anthropic.claude-3-sonnet-20240229-v1:0"
)

claude_sonnet_37_model = Model(
    name="Claude Sonnet 3.7", id="anthropic.claude-3-7-sonnet-20250219-v1:0"
)

claude_sonnet_45_model = Model(
    name="Claude Sonnet 4.5",
    id="global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    top_p=None,
)

claude_opus_45_model = Model(
    name="Claude Opus 4.5",
    id="global.anthropic.claude-opus-4-5-20251101-v1:0",
    top_p=None,
)

MODEL_REGISTRY = {
    "claude-sonnet-3": claude_sonnet_3_model,
    "claude-sonnet-3.7": claude_sonnet_37_model,
    "claude-sonnet-4.5": claude_sonnet_45_model,
    "claude-opus-4.5": claude_opus_45_model,
}
