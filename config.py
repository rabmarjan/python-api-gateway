import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application configuration settings"""
    # Redis configuration
    REDIS_HOST: str = os.getenv("REDIS_HOST", "10.0.6.26")
    REDIS_PORT: int = os.getenv("REDIS_PORT", 6379)
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "marjan")
    
    # JWT configuration
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "your_secret_key")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    
    # Cache configuration
    CACHE_TTL: int = os.getenv("CACHE_TTL", 60)  # seconds
    
    # HTTP client configuration
    REQUEST_TIMEOUT: int = os.getenv("REQUEST_TIMEOUT", 30)  # seconds
    RETRY_COUNT: int = os.getenv("RETRY_COUNT", 3)
    RETRY_BACKOFF: float = os.getenv("RETRY_BACKOFF", 0.5)
    CIRCUIT_RESET_TIMEOUT: int = os.getenv("CIRCUIT_RESET_TIMEOUT", 30)  # seconds
    
    # Rate limiting
    RATE_LIMIT: str = os.getenv("RATE_LIMIT", "10/minute")
    
    # Microservices URLs
    ROUTE_MAP: dict = {
        "auth": os.getenv("AUTH_SERVICE_URL", "http://localhost:8000"),
        "items": os.getenv("ITEMS_SERVICE_URL", "http://localhost:8001"),
    }
    
    # Trusted hosts
    ALLOWED_HOSTS: list = ["*"]  # Restrict in production
    
    class Config:
        env_file = ".env"

settings = Settings()