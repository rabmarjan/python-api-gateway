import time
import asyncio
from typing import Dict, Optional
import httpx
from fastapi import HTTPException

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    def __init__(self, reset_timeout: int = 30):
        self.status = "closed"  # closed (normal), open (failed), half-open (testing)
        self.failures = 0
        self.last_failure = 0
        self.last_success = time.time()
        self.recovery_time = reset_timeout
        self.retry_attempt = 0
        self.reset_timeout = reset_timeout

    def record_success(self):
        """Record a successful request"""
        self.status = "closed"
        self.failures = 0
        self.last_success = time.time()
        self.retry_attempt = 0
        self.recovery_time = self.reset_timeout

    def record_failure(self):
        """Record a failed request"""
        self.failures += 1
        if self.failures >= 3:  # Threshold for opening circuit
            self.status = "open"
            self.last_failure = time.time()
            # Exponential backoff for recovery time
            if self.retry_attempt > 0:
                self.recovery_time = min(
                    self.reset_timeout * (2 ** self.retry_attempt), 
                    300  # Max 5 minutes
                )

    def should_allow_request(self) -> bool:
        """Check if request should be allowed"""
        now = time.time()
        
        # Always attempt to reset a circuit that's been open for too long
        if self.status == "open":
            time_since_failure = now - self.last_failure
            if time_since_failure >= self.recovery_time:
                self.status = "half-open"
                self.retry_attempt += 1
                return True
            return False
        
        return True

class HttpClientManager:
    """HTTP client manager with circuit breaker"""
    def __init__(self, timeout: int = 30):
        self.client: Optional[httpx.AsyncClient] = None
        self.timeout = timeout
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
    
    async def initialize(self):
        self.client = httpx.AsyncClient(timeout=self.timeout)
    
    async def close(self):
        if self.client:
            await self.client.aclose()
    
    def get_circuit_breaker(self, service_base: str) -> CircuitBreaker:
        """Get or create circuit breaker for a service"""
        if service_base not in self.circuit_breakers:
            self.circuit_breakers[service_base] = CircuitBreaker()
        return self.circuit_breakers[service_base]
    
    def reset_circuit_breaker(self, service_base: str) -> None:
        """Reset circuit breaker to healthy state"""
        circuit = self.get_circuit_breaker(service_base)
        circuit.record_success()
        print(f"Circuit breaker for {service_base} reset to CLOSED state")
    
    async def request(self, method: str, url: str, headers: Dict = None, 
                      data: bytes = None, retries: int = 3, 
                      backoff_factor: float = 0.5) -> httpx.Response:
        """Send HTTP request with circuit breaker and retry logic"""
        if not self.client:
            await self.initialize()
        
        try:
            service_base = url.split('/')[2]  # Extract domain/hostname
        except IndexError:
            service_base = "unknown_service"
        
        circuit = self.get_circuit_breaker(service_base)
        
        # For debugging
        print(f"Service {service_base} status: {circuit.status}")
        
        # Attempt the request with retries
        for attempt in range(max(1, retries)):
            # Check if circuit allows request
            if not circuit.should_allow_request():
                raise HTTPException(
                    status_code=503, 
                    detail=f"Service {service_base} is unavailable. Will retry in {circuit.recovery_time}s"
                )
            
            try:
                # Make the request
                response = await self.client.request(
                    method=method, url=url, headers=headers, content=data
                )
                
                # If we get here with a non-5xx status, service is working
                if response.status_code < 500:
                    # Success! Reset circuit if it was open or half-open
                    if circuit.status in ["open", "half-open"]:
                        circuit.record_success()
                    return response
                
                # Handle 5xx server errors
                circuit.record_failure()
                print(f"Service {service_base} returned {response.status_code}. Failure count: {circuit.failures}")
                
            except httpx.RequestError as e:
                circuit.record_failure()
                print(f"Network error for {service_base}: {e}. Attempt {attempt + 1}/{retries}")
            
            # Check if circuit is now open
            if circuit.status == "open" and attempt == retries - 1:
                print(f"Circuit OPEN for {service_base}. Too many failures.")
                raise HTTPException(
                    status_code=503, 
                    detail=f"Service {service_base} is unavailable after {circuit.failures} failures"
                )
            
            # Apply backoff before retry
            if attempt < retries - 1:
                wait_time = backoff_factor * (2 ** attempt)
                print(f"Waiting {wait_time:.2f}s before retry {attempt + 1}")
                await asyncio.sleep(wait_time)
        
        # This is reached only if all retries failed but didn't trigger circuit breaker
        raise HTTPException(status_code=502, detail=f"Service {service_base} unavailable after {retries} attempts")
