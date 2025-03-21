import redis.asyncio as redis
from fastapi import FastAPI, Request
from starlette.middleware.trustedhost import TrustedHostMiddleware
from .config import settings
from .services.cache import CacheManager
from .services.http_client import HttpClientManager
from .services.token import TokenManager
from .controllers.gateway import GatewayController
from .controllers.admin import AdminController
from .controllers.health import HealthController
from .middleware.logging import log_request_middleware
from .routes import gateway_router, admin_router, health_router, limiter
from .dependencies import get_gateway_controller, get_admin_controller, get_health_controller

def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    app = FastAPI(title="API Gateway", description="Microservices API Gateway")
    
    # Set up rate limiter
    app.state.limiter = limiter
    
    # Add middleware
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.ALLOWED_HOSTS)
    app.middleware("http")(log_request_middleware)
    
    # Add routers
    app.include_router(gateway_router)
    app.include_router(admin_router)
    app.include_router(health_router)
    
    # Event handlers
    @app.on_event("startup")
    async def startup_event():
        # Initialize Redis client
        app.state.redis = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            db=0,
            decode_responses=False
        )
        
        # Initialize services
        app.state.cache_manager = CacheManager(app.state.redis)
        app.state.token_manager = TokenManager(settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
        app.state.http_client = HttpClientManager(settings.REQUEST_TIMEOUT)
        await app.state.http_client.initialize()
        
        # Initialize controllers
        app.state.gateway_controller = GatewayController(
            app.state.cache_manager,
            app.state.http_client,
            settings.ROUTE_MAP,
            settings.CACHE_TTL
        )
        app.state.admin_controller = AdminController(app.state.http_client)
        app.state.health_controller = HealthController(
            app.state.redis,
            app.state.http_client,
            settings.ROUTE_MAP
        )
        app.dependency_overrides[get_gateway_controller] = lambda: app.state.gateway_controller
        app.dependency_overrides[get_admin_controller] = lambda: app.state.admin_controller
        app.dependency_overrides[get_health_controller] = lambda: app.state.health_controller
    @app.on_event("shutdown")
    async def shutdown_event():
        # Close Redis connection
        if hasattr(app.state, "redis"):
            await app.state.redis.close()
        
        # Close HTTP client
        if hasattr(app.state, "http_client"):
            await app.state.http_client.close()
    
    # Dependencies
    # @app.dependency_overrides[GatewayController]
    # def get_gateway_controller():
    #     return app.state.gateway_controller
    
    # @app.dependency_overrides[AdminController]
    # def get_admin_controller():
    #     return app.state.admin_controller
    
    # @app.dependency_overrides[HealthController]
    # def get_health_controller():
    #     return app.state.health_controller
    
    return app

# Create the application instance
app = create_app()

# Run the application
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)