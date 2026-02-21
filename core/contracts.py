from typing import Literal
from pydantic import BaseModel, HttpUrl


class SourceCreate(BaseModel):
    name: str
    url: HttpUrl
    connector_type: Literal["web", "rss"] = "web"
