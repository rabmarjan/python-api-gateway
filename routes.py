from fastapi import APIRouter, Depends, Request
import asyncio
from slowapi import Limiter
from slowapi.util import get_remote_address
from .controllers.gateway import GatewayController
from .controllers.admin import AdminController
from .controllers.health import HealthController
from .dependencies import get_settings, get_auth_dependency, get_gateway_controller, get_admin_controller, get_health_controller
from .config import settings

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Create routers
gateway_router = APIRouter()
admin_router = APIRouter(prefix="/admin")
health_router = APIRouter()

# Health check route
@health_router.get("/health", include_in_schema=False, response_model=None)
async def health_check(
    health_controller: HealthController = Depends(get_health_controller)
):
    """Health check endpoint for the API gateway"""
    return await health_controller.check_health()

# Admin routes
@admin_router.post("/reset-circuit/{service_name}", include_in_schema=False, response_model=None)
async def reset_circuit(
    service_name: str,
    admin_controller: AdminController = Depends(get_admin_controller)
):
    """Manually reset circuit breaker for a service"""
    return await admin_controller.reset_circuit_breaker(service_name)

# Gateway routes
@gateway_router.get("/{full_path:path}", operation_id="gateway_get", response_model=None)
@limiter.limit(settings.RATE_LIMIT)
async def gateway_get(
    full_path: str,
    request: Request,
    gateway_controller: GatewayController = Depends(get_gateway_controller),
    user = Depends(get_auth_dependency())
):
    """Gateway handler for GET requests"""
    return await gateway_controller.process_request(full_path, request)

@gateway_router.post("/{full_path:path}", operation_id="gateway_post", response_model=None)
@limiter.limit(settings.RATE_LIMIT)
async def gateway_post(
    full_path: str,
    request: Request,
    gateway_controller: GatewayController = Depends(get_gateway_controller),
    user = Depends(get_auth_dependency())
):
    """Gateway handler for POST requests"""
    response = await gateway_controller.process_request(full_path, request)
    asyncio.create_task(gateway_controller.invalidate_cache(full_path))
    return response

@gateway_router.put("/{full_path:path}", operation_id="gateway_put", response_model=None)
@limiter.limit(settings.RATE_LIMIT)
async def gateway_put(
    full_path: str,
    request: Request,
    gateway_controller: GatewayController = Depends(get_gateway_controller),
    user = Depends(get_auth_dependency())
):
    """Gateway handler for PUT requests"""
    response = await gateway_controller.process_request(full_path, request)
    asyncio.create_task(gateway_controller.invalidate_cache(full_path))
    return response

@gateway_router.delete("/{full_path:path}", operation_id="gateway_delete", response_model=None)
@limiter.limit(settings.RATE_LIMIT)
async def gateway_delete(
    full_path: str,
    request: Request,
    gateway_controller: GatewayController = Depends(get_gateway_controller),
    user = Depends(get_auth_dependency())
):
    """Gateway handler for DELETE requests"""
    response = await gateway_controller.process_request(full_path, request)
    asyncio.create_task(gateway_controller.invalidate_cache(full_path))
    return response

@gateway_router.patch("/{full_path:path}", operation_id="gateway_patch", response_model=None)
@limiter.limit(settings.RATE_LIMIT)
async def gateway_patch(
    full_path: str,
    request: Request,
    gateway_controller: GatewayController = Depends(get_gateway_controller),
    user = Depends(get_auth_dependency())
):
    """Gateway handler for PATCH requests"""
    response = await gateway_controller.process_request(full_path, request)
    asyncio.create_task(gateway_controller.invalidate_cache(full_path))
    return response
