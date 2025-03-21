import time
import asyncio
from fastapi import Request

async def log_request_body(body: bytes):
    """Log request body asynchronously"""
    try:
        # Limit the logged data size
        decoded_body = body[:1024].decode()
        print(f"Request Body: {decoded_body}")
    except Exception as e:
        print(f"Error logging request body: {e}")

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
