from fastapi import HTTPException
from ..services.http_client import HttpClientManager

class AdminController:
    """Controller for admin operations"""
    def __init__(self, http_client: HttpClientManager):
        self.http_client = http_client
    
    async def reset_circuit_breaker(self, service_name: str):
        """Reset circuit breaker for a service"""
        # Find services matching the name
        matches = [base for base in self.http_client.circuit_breakers if service_name in base]
        
        if not matches:
            raise HTTPException(status_code=404, detail=f"No circuit found for service: {service_name}")
        
        for service_base in matches:
            self.http_client.reset_circuit_breaker(service_base)
        
        return {"message": f"Reset circuit breaker for {len(matches)} services"}
