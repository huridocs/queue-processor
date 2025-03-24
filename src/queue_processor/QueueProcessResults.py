from typing import Optional, Any

from pydantic import BaseModel


class QueueProcessResults(BaseModel):
    results: Optional[dict[str, Any]] = None
    delete_message: bool = True
    invisibility_timeout: int = 30
