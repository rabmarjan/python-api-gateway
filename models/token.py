from datetime import datetime
from typing import Dict, Optional
import time
import jwt
from fastapi import HTTPException

class TokenPayload:
    """Token payload model"""
    user_id: str
    username: str
    roles: list
    exp: int
    
    def __init__(self, data: Dict):
        self.user_id = data.get("user_id")
        self.username = data.get("username")
        self.roles = data.get("roles", [])
        self.exp = data.get("exp")
