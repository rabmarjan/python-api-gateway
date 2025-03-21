from datetime import datetime
from ..services.http_client import HttpClientManager

class HealthController:
    """Controller for health check operations"""
    def __init__(self, redis_client, http_client: HttpClientManager, route_map: dict):
        self.redis_client = redis_client
        self.http_client = http_client
        self.route_map = route_map
    
    async def check_health(self):
        """Check health of all services"""
        # Check Redis connection
        redis_ok = False
        try:
            ping = await self.redis_client.ping()
            redis_ok = ping
        except Exception as e:
            print(f"Redis health check failed: {e}")
        
        # Check microservices health
        services_status = {}
        
        # Get circuit breaker status
        circuit_status = {}
        for service_base, circuit in self.http_client.circuit_breakers.items():
            circuit_status[service_base] = {
                "status": circuit.status,
                "failures": circuit.failures,
                "last_failure": datetime.fromtimestamp(circuit.last_failure).isoformat() if circuit.last_failure else None,
                "recovery_time": circuit.recovery_time
            }
        
        # Test connections to services
        for name, url in self.route_map.items():
            try:
                health_url = f"{url}/health"
                response = await self.http_client.request("GET", health_url, retries=1)
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
