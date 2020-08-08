from mypy_extensions import TypedDict
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


class UserData(TypedDict):
    id: str
    roles: List[str]
    groups: List[str]
    headers: Dict[str, str]
    data: Dict[str, Any]


class SerializedRequest(TypedDict):
    url: str
    method: str
    headers: Dict[str, str]
    annotations: Dict[str, Any]
    container_url: Optional[str]
    user: Optional[UserData]
