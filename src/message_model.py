from pydantic import BaseModel, field_validator
from typing import List, Union, Any, Optional, Dict
from datetime import datetime


class Event(BaseModel):
    name: str
    payload: Union[Dict[str, Any], List[Dict[str,Union[str, Any]]]]
    tags: Optional[str] = None

class Message(BaseModel):
    event: Event
    service: str
    accepted_timestamp: datetime
    created_timestamp: datetime
    uid: str
