from fastapi import Depends, Request
from functools import lru_cache
from .config import Settings, settings
from .services.token import TokenManager
from .middleware.auth import authenticate_user

@lru_cache
def get_settings():
    """Get application settings"""
    return settings

def get_token_manager(settings: Settings = Depends(get_settings)):
    """Get token manager dependency"""
    return TokenManager(settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)

def get_auth_dependency(token_manager: TokenManager = Depends(get_token_manager)):
    """Get authentication dependency"""
    async def auth_dependency(request: Request):
        token_manager = request.app.state.token_manager
        return await authenticate_user(request, token_manager)
    return auth_dependency

def get_gateway_controller(request: Request):
    return request.app.state.gateway_controller

def get_admin_controller(request: Request):
    return request.app.state.admin_controller

def get_health_controller(request: Request):
    return request.app.state.health_controller