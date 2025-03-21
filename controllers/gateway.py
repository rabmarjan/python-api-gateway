import io
import asyncio
from fastapi import Request
from fastapi.responses import StreamingResponse, JSONResponse
from ..services.cache import CacheManager
from ..services.http_client import HttpClientManager
from ..middleware.logging import log_request_body

class GatewayController:
    """Controller for handling gateway requests"""
    def __init__(self, cache_manager: CacheManager, http_client: HttpClientManager, route_map: dict, cache_ttl: int):
        self.cache_manager = cache_manager
        self.http_client = http_client
        self.route_map = route_map
        self.cache_ttl = cache_ttl
    
    async def proxy_request(self, request: Request, service_url: str):
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
            cached_response = await self.cache_manager.get(cache_key)
            if cached_response:
                return JSONResponse(content=cached_response)
        
        # For POST/PUT requests, read body
        body = await request.body() if req_method in ("POST", "PUT", "PATCH") else None
        
        # If body logging is needed for non-GET requests
        if body and req_method != "GET":
            asyncio.create_task(log_request_body(body))
        
        # Forward request to microservice
        response = await self.http_client.request(
            method=req_method,
            url=req_url,
            headers=headers,
            data=body
        )
        
        # Cache GET responses
        if req_method == "GET" and response.status_code == 200:
            try:
                response_data = response.json()
                await self.cache_manager.set(cache_key, response_data, self.cache_ttl)
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
    
    async def process_request(self, full_path: str, request: Request):
        """Process gateway request and route to appropriate service"""
        # Determine target microservice
        service_url = None
        if full_path.startswith("items"):
            service_url = self.route_map["items"]
        elif full_path == "login" or full_path.startswith("auth"):
            service_url = self.route_map["auth"]
        else:
            # Default to auth server, but consider a 404 for unknown paths
            service_url = self.route_map["auth"]
        
        # Forward request to microservice
        return await self.proxy_request(request, service_url)
    
    async def invalidate_cache(self, path: str):
        """Invalidate cache entries related to a modified resource"""
        base_path = path.split("/")[0]  # Get resource type
        await self.cache_manager.invalidate(base_path)
