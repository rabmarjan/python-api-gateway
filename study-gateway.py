import io
import asyncio
import pickle
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Union, Any

import httpx
import jwt
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware
from slowapi import Limiter
from slowapi.util import get_remote_address
import redis.asyncio as redis

# Configuration - Better to use environment variables in production
REDIS_HOST = '10.0.6.26'
REDIS_PORT = 6379
REDIS_PASSWORD = 'marjan'
JWT_SECRET_KEY = "your_secret_key"  # Should be in environment variables
JWT_ALGORITHM = "HS256"
CACHE_TTL = 60  # seconds
REQUEST_TIMEOUT = 30  # seconds
RETRY_COUNT = 3
RETRY_BACKOFF = 0.5
CIRCUIT_RESET_TIMEOUT = 30  # seconds to wait before attempting to reset circuit breaker

# Microservices URLs - Consider using service discovery in production
ROUTE_MAP = {
    "auth": "http://localhost:8000",  # Authentication Service
 #   "users": "http://localhost:8000",  # User Service
    "items": "http://localhost:8001",  # First Microservice
 #   "orders": "http://localhost:8002",  # Another Microservice
}

# Initialize FastAPI app
app = FastAPI(title="API Gateway", description="Microservices API Gateway")

# Rate Limiting Setup
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

# Cache implementation
class CacheManager:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.local_cache: Dict[str, tuple] = {}  # (data, expiry)
        self.local_ttl = 10  # Local cache TTL in seconds

    async def get(self, key: str) -> Optional[Any]:
        # Check local cache first for better performance
        now = time.time()
        if key in self.local_cache and self.local_cache[key][1] > now:
            return self.local_cache[key][0]

        # Fall back to Redis
        cached_data = await self.redis.get(key)
        if cached_data:
            data = pickle.loads(cached_data)
            # Update local cache
            self.local_cache[key] = (data, now + self.local_ttl)
            return data
        return None

    async def set(self, key: str, value: Any, ttl: int = CACHE_TTL) -> None:
        serialized_data = pickle.dumps(value)
        # Set in Redis
        await self.redis.setex(key, ttl, serialized_data)
        # Update local cache
        self.local_cache[key] = (value, time.time() + min(ttl, self.local_ttl))

    async def invalidate(self, pattern: str) -> None:
        # Clear local cache entries that match the pattern
        keys_to_remove = [k for k in self.local_cache if pattern in k]
        for k in keys_to_remove:
            del self.local_cache[k]
        
        # Clear Redis cache
        keys = await self.redis.keys(f"*{pattern}*")
        if keys:
            await self.redis.delete(*keys)

# JWT Token manager
class TokenManager:
    def __init__(self, secret_key: str, algorithm: str):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.cache: Dict[str, tuple] = {}  # (payload, expiry)
    
    def validate_token(self, token: str) -> Dict:
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
            
            # Clean expired tokens periodically (basic cleanup)
            if len(self.cache) > 100:  # Arbitrary threshold
                self.clean_expired_tokens()
                
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
    
    def clean_expired_tokens(self) -> None:
        now = time.time()
        self.cache = {k: v for k, v in self.cache.items() if v[1] > now}

# HTTP Client with circuit breaker
class HttpClientManager:
    def __init__(self, timeout: int = REQUEST_TIMEOUT):
        self.client: Optional[httpx.AsyncClient] = None
        self.timeout = timeout
        self.service_status: Dict[str, Dict] = {}  # Track service health
        self.circuit_reset_timeout = CIRCUIT_RESET_TIMEOUT
    
    async def initialize(self):
        self.client = httpx.AsyncClient(timeout=self.timeout)
    
    async def close(self):
        if self.client:
            await self.client.aclose()
    
    def get_service_info(self, service_base: str) -> Dict:
        """Get or initialize service info"""
        if service_base not in self.service_status:
            self.service_status[service_base] = {
                "status": "closed",  # Circuit state: closed (normal), open (failed), half-open (testing)
                "failures": 0,
                "last_failure": 0,
                "last_success": time.time(),
                "recovery_time": self.circuit_reset_timeout,
                "retry_attempt": 0
            }
        return self.service_status[service_base]
    
    def reset_service_state(self, service_base: str) -> None:
        """Reset service to healthy state"""
        service_info = self.get_service_info(service_base)
        service_info["status"] = "closed"
        service_info["failures"] = 0
        service_info["last_success"] = time.time()
        service_info["retry_attempt"] = 0
        service_info["recovery_time"] = self.circuit_reset_timeout  # Reset to default
        self.service_status[service_base] = service_info
        print(f"Circuit breaker for {service_base} reset to CLOSED state")
            
    async def request(self, method: str, url: str, headers: Dict = None, 
                      data: bytes = None, retries: int = RETRY_COUNT, 
                      backoff_factor: float = RETRY_BACKOFF) -> httpx.Response:
        """Send HTTP request with circuit breaker and retry logic"""
        if not self.client:
            await self.initialize()
        
        try:
            service_base = url.split('/')[2]  # Extract domain/hostname
        except IndexError:
            service_base = "unknown_service"
        
        service_info = self.get_service_info(service_base)
        now = time.time()
        
        # Always attempt to reset a circuit that's been open for too long
        if service_info["status"] == "open":
            time_since_failure = now - service_info["last_failure"]
            if time_since_failure >= service_info["recovery_time"]:
                print(f"Attempting recovery for {service_base} after {time_since_failure:.1f}s")
                service_info["status"] = "half-open"
                service_info["retry_attempt"] += 1
        
        # For debugging
        print(f"Service {service_base} status: {service_info['status']}")
        
        # Attempt the request with retries
        for attempt in range(max(1, retries)):
            # If circuit is completely open and we're not in recovery mode, fail fast
            if service_info["status"] == "open" and service_info["status"] != "half-open":
                raise HTTPException(
                    status_code=503, 
                    detail=f"Service {service_base} is unavailable. Will retry in {service_info['recovery_time']}s"
                )
            
            try:
                # Make the request
                response = await self.client.request(
                    method=method, url=url, headers=headers, content=data
                )
                
                # If we get here with a non-5xx status, service is working
                if response.status_code < 500:
                    # Success! Reset circuit if it was open or half-open
                    if service_info["status"] in ["open", "half-open"]:
                        self.reset_service_state(service_base)
                    return response
                
                # Handle 5xx server errors
                service_info["failures"] += 1
                print(f"Service {service_base} returned {response.status_code}. Failure count: {service_info['failures']}")
                
            except httpx.RequestError as e:
                service_info["failures"] += 1
                print(f"Network error for {service_base}: {e}. Attempt {attempt + 1}/{retries}")
            
            # Trip the circuit breaker if too many failures
            if service_info["failures"] >= 3:  # Threshold for opening circuit
                service_info["status"] = "open"
                service_info["last_failure"] = now
                
                # Exponential backoff for recovery time based on retry attempts
                if service_info["retry_attempt"] > 0:
                    service_info["recovery_time"] = min(
                        self.circuit_reset_timeout * (2 ** service_info["retry_attempt"]), 
                        300  # Max 5 minutes
                    )
                
                self.service_status[service_base] = service_info
                
                # Only break if we've exhausted our attempts
                if attempt == retries - 1:
                    print(f"Circuit OPEN for {service_base}. Too many failures.")
                    raise HTTPException(
                        status_code=503, 
                        detail=f"Service {service_base} is unavailable after {service_info['failures']} failures"
                    )
            
            # Apply backoff before retry
            if attempt < retries - 1:
                wait_time = backoff_factor * (2 ** attempt)
                print(f"Waiting {wait_time:.2f}s before retry {attempt + 1}")
                await asyncio.sleep(wait_time)
        
        # This is reached only if all retries failed but didn't trigger circuit breaker
        raise HTTPException(status_code=502, detail=f"Service {service_base} unavailable after {retries} attempts")

# Initialize services
@app.on_event("startup")
async def startup_event():
    # Initialize Redis client
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=0,
        decode_responses=False
    )
    
    # Initialize cache manager
    app.state.cache_manager = CacheManager(app.state.redis)
    
    # Initialize JWT token manager
    app.state.token_manager = TokenManager(JWT_SECRET_KEY, JWT_ALGORITHM)
    
    # Initialize HTTP client manager
    app.state.http_client = HttpClientManager()
    await app.state.http_client.initialize()

@app.on_event("shutdown")
async def shutdown_event():
    # Close Redis connection
    if hasattr(app.state, "redis"):
        await app.state.redis.close()
    
    # Close HTTP client
    if hasattr(app.state, "http_client"):
        await app.state.http_client.close()

# Dependency for JWT authentication
async def authenticate_user(request: Request, skip_paths=("login", "health")):
    """JWT Authentication Dependency"""
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
    payload = app.state.token_manager.validate_token(token)
    request.state.user = payload
    return payload

# Request logging middleware
async def log_request_middleware(request: Request, call_next):
    """Log request information"""
    start_time = time.time()
    path = request.url.path
    method = request.method
    
    # Process the request
    response = await call_next(request)
    
    # Log request details
    process_time = time.time() - start_time
    status_code = response.status_code
    print(f"{method} {path} {status_code} {process_time:.4f}s")
    
    return response

app.middleware("http")(log_request_middleware)

# Add TrustedHost middleware
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])  # Restrict in production

# Background task for logging
async def log_request_body(body: bytes):
    """Log request body asynchronously"""
    try:
        # Limit the logged data size
        decoded_body = body[:1024].decode()
        print(f"Request Body: {decoded_body}")
    except Exception as e:
        print(f"Error logging request body: {e}")

# Helper for proxying requests to microservices
async def proxy_request(request: Request, service_url: str):
    """Forward request to microservice with caching for GET requests"""
    req_method = request.method
    path = request.url.path
    req_url = f"{service_url}{path}"
    
    # Prepare headers (forward needed headers)
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "connection", "content-length")
    }
    
    # Cache key based on URL and any query parameters
    cache_key = f"cache:{path}{request.url.query}"
    
    # Try cache for GET requests
    if req_method == "GET":
        cached_response = await app.state.cache_manager.get(cache_key)
        if cached_response:
            return JSONResponse(content=cached_response)
    
    # For POST/PUT requests, read body
    body = await request.body() if req_method in ("POST", "PUT", "PATCH") else None
    
    # If body logging is needed for non-GET requests
    if body and req_method != "GET":
        asyncio.create_task(log_request_body(body))
    
    # Forward request to microservice
    response = await app.state.http_client.request(
        method=req_method,
        url=req_url,
        headers=headers,
        data=body
    )
    
    # Cache GET responses
    if req_method == "GET" and response.status_code == 200:
        try:
            response_data = response.json()
            await app.state.cache_manager.set(cache_key, response_data)
            return JSONResponse(content=response_data)
        except Exception as e:
            # If JSON parsing fails, return streaming response
            print(f"Error parsing JSON: {e}")
    
    # For non-GET requests or failed JSON parsing, return StreamingResponse
    return StreamingResponse(
        io.BytesIO(response.content),
        status_code=response.status_code,
        headers=dict(response.headers)
    )

# Invalidate cache for non-GET requests
async def invalidate_related_cache(path: str):
    """Invalidate cache entries related to a modified resource"""
    base_path = path.split("/")[0]  # Get resource type
    await app.state.cache_manager.invalidate(base_path)

# Manual circuit breaker reset endpoint (for admin use)
@app.post("/admin/reset-circuit/{service_name}", include_in_schema=False)
async def reset_circuit_breaker(service_name: str):
    """Manually reset circuit breaker for a service"""
    http_client = app.state.http_client
    
    # Find services matching the name
    matches = [base for base in http_client.service_status if service_name in base]
    
    if not matches:
        raise HTTPException(status_code=404, detail=f"No circuit found for service: {service_name}")
    
    for service_base in matches:
        http_client.reset_service_state(service_base)
    
    return {"message": f"Reset circuit breaker for {len(matches)} services"}

# Define separate routes for each HTTP method
@app.get("/{full_path:path}", operation_id="gateway_get")
@limiter.limit("10/minute")
async def gateway_get(full_path: str, request: Request, user=Depends(authenticate_user)):
    """Gateway handler for GET requests"""
    return await process_gateway_request(full_path, request)

@app.post("/{full_path:path}", operation_id="gateway_post")
@limiter.limit("10/minute")
async def gateway_post(full_path: str, request: Request, user=Depends(authenticate_user)):
    """Gateway handler for POST requests"""
    response = await process_gateway_request(full_path, request)
    asyncio.create_task(invalidate_related_cache(full_path))
    return response

@app.put("/{full_path:path}", operation_id="gateway_put")
@limiter.limit("10/minute")
async def gateway_put(full_path: str, request: Request, user=Depends(authenticate_user)):
    """Gateway handler for PUT requests"""
    response = await process_gateway_request(full_path, request)
    asyncio.create_task(invalidate_related_cache(full_path))
    return response

@app.delete("/{full_path:path}", operation_id="gateway_delete")
@limiter.limit("10/minute")
async def gateway_delete(full_path: str, request: Request, user=Depends(authenticate_user)):
    """Gateway handler for DELETE requests"""
    response = await process_gateway_request(full_path, request)
    asyncio.create_task(invalidate_related_cache(full_path))
    return response

@app.patch("/{full_path:path}", operation_id="gateway_patch")
@limiter.limit("15/minute")
async def gateway_patch(full_path: str, request: Request, user=Depends(authenticate_user)):
    """Gateway handler for PATCH requests"""
    response = await process_gateway_request(full_path, request)
    asyncio.create_task(invalidate_related_cache(full_path))
    return response

# Common request processing function
async def process_gateway_request(full_path: str, request: Request):
    """Common function to process all gateway requests"""
    # Determine target microservice
    service_url = None
    if full_path.startswith("items"):
        service_url = ROUTE_MAP["items"]
    elif full_path == "login" or full_path.startswith("auth"):
        service_url = ROUTE_MAP["auth"]
    elif full_path.startswith("admin/reset-circuit"):
        # This will be handled by the dedicated endpoint
        pass
    else:
        # Default to auth server, but consider a 404 for unknown paths
        service_url = ROUTE_MAP["auth"]
    
    # Forward request to microservice
    return await proxy_request(request, service_url)

# Health check endpoint
@app.get("/health", include_in_schema=False)
async def health_check():
    """Health check endpoint for the API gateway"""
    # Check Redis connection
    redis_ok = False
    try:
        ping = await app.state.redis.ping()
        redis_ok = ping
    except Exception as e:
        print(f"Redis health check failed: {e}")
    
    # Check microservices health
    services_status = {}
    http_client = app.state.http_client
    circuit_status = {}
    
    # Get circuit breaker status
    for service_base, info in http_client.service_status.items():
        circuit_status[service_base] = {
            "status": info["status"],
            "failures": info["failures"],
            "last_failure": datetime.fromtimestamp(info["last_failure"]).isoformat() if info["last_failure"] else None,
            "recovery_time": info["recovery_time"]
        }
    
    # Test connections to services
    for name, url in ROUTE_MAP.items():
        try:
            health_url = f"{url}/health"
            response = await http_client.request("GET", health_url, retries=1)
            services_status[name] = {
                "status": "up" if response.status_code == 200 else "degraded",
                "statusCode": response.status_code
            }
        except Exception as e:
            services_status[name] = {
                "status": "down",
                "error": str(e)
            }
    
    status = {
        "status": "healthy" if redis_ok and all(s["status"] == "up" for s in services_status.values()) else "degraded",
        "redis": "connected" if redis_ok else "disconnected",
        "services": services_status,
        "circuits": circuit_status,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return status
