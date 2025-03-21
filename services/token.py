import time
from typing import Dict, Optional
import jwt
from fastapi import HTTPException
from ..models.token import TokenPayload

class TokenManager:
    """JWT token validation and management"""
    def __init__(self, secret_key: str, algorithm: str):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.cache: Dict[str, tuple] = {}  # (payload, expiry)
    
    def validate_token(self, token: str) -> Dict:
        """Validate JWT token and return payload"""
        now = time.time()
        
        # Check cache first
        if token in self.cache and self.cache[token][1] > now:
            return self.cache[token][0]

        try:
            # Decode token
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Cache the result
            exp = payload.get("exp", now + 300)  # Default 5 min if no exp
            self.cache[token] = (payload, exp)
            
            # Clean expired tokens periodically
            if len(self.cache) > 100:  # Arbitrary threshold
                self.clean_expired_tokens()
                
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
    
    def clean_expired_tokens(self) -> None:
        """Remove expired tokens from cache"""
        now = time.time()
        self.cache = {k: v for k, v in self.cache.items() if v[1] > now}
