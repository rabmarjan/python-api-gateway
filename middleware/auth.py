from fastapi import Request, HTTPException
from ..services.token import TokenManager

async def authenticate_user(request: Request, token_manager: TokenManager, skip_paths=("login", "health")):
    """JWT Authentication Middleware"""
    # Skip authentication for specific paths
    path = request.url.path.strip("/")
    if any(path.startswith(skip_path) for skip_path in skip_paths):
        return None
        
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401, 
            detail="Missing or invalid Authorization header"
        )

    token = auth_header.split(" ")[1]
    payload = token_manager.validate_token(token)
    request.state.user = payload
    return payload