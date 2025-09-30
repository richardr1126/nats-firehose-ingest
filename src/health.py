"""Health check server for monitoring service status."""

import asyncio
from typing import Dict, Any, Optional

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
import uvicorn
import structlog

from .config import settings

logger = structlog.get_logger(__name__)


class HealthCheckServer:
    """FastAPI-based health check server that runs in its own thread."""
    
    def __init__(self, service):
        self.service = service
        self.app = FastAPI(
            title="NATS Firehose Ingest Health",
            description="Health check and metrics for NATS firehose ingest service",
            version="1.0.0"
        )
        self.server: Optional[uvicorn.Server] = None
        
        self._setup_routes()
    
    def _setup_routes(self) -> None:
        """Setup health check routes."""
        
        @self.app.get("/health")
        async def health_check():
            """Basic health check endpoint."""
            try:
                # Call synchronous health check method
                is_healthy = self.service.perform_health_check()
                status_code = 200 if is_healthy else 503
                
                return JSONResponse(
                    content={
                        "status": "healthy" if is_healthy else "unhealthy",
                        "service": settings.service_name
                    },
                    status_code=status_code
                )
            except Exception as e:
                logger.error("Health check endpoint failed", error=str(e))
                return JSONResponse(
                    content={
                        "status": "unhealthy",
                        "error": str(e),
                        "service": settings.service_name
                    },
                    status_code=503
                )
        
        @self.app.get("/health/detailed")
        async def detailed_health():
            """Detailed health status with component information."""
            try:
                # Call synchronous method to get health status
                health_status = self.service.get_health_status()
                status_code = 200 if health_status["status"] == "healthy" else 503
                
                return JSONResponse(
                    content=health_status,
                    status_code=status_code
                )
            except Exception as e:
                logger.error("Detailed health check failed", error=str(e))
                return JSONResponse(
                    content={
                        "status": "unhealthy",
                        "error": str(e),
                        "service": settings.service_name
                    },
                    status_code=503
                )
        
        @self.app.get("/metrics")
        async def metrics():
            """Prometheus-style metrics endpoint."""
            try:
                # Call synchronous method to get health status
                health_status = self.service.get_health_status()
                
                # Generate Prometheus metrics format
                metrics_lines = []
                
                # Service metrics
                service_stats = health_status.get("service_stats", {})
                metrics_lines.extend([
                    f"# HELP nats_firehose_operations_total Total operations processed",
                    f"# TYPE nats_firehose_operations_total counter",
                    f"nats_firehose_operations_total {service_stats.get('operations_processed', 0)}",
                    
                    f"# HELP nats_firehose_posts_published_total Total posts published to NATS",
                    f"# TYPE nats_firehose_posts_published_total counter",
                    f"nats_firehose_posts_published_total {service_stats.get('posts_published', 0)}",
                    
                    f"# HELP nats_firehose_publish_errors_total Total publish errors",
                    f"# TYPE nats_firehose_publish_errors_total counter",
                    f"nats_firehose_publish_errors_total {service_stats.get('publish_errors', 0)}",
                    
                    f"# HELP nats_firehose_uptime_seconds Service uptime in seconds",
                    f"# TYPE nats_firehose_uptime_seconds gauge",
                    f"nats_firehose_uptime_seconds {service_stats.get('uptime_seconds', 0)}",
                ])
                
                # NATS metrics
                nats_stats = health_status.get("components", {}).get("nats", {}).get("stats", {})
                metrics_lines.extend([
                    f"# HELP nats_connected NATS connection status",
                    f"# TYPE nats_connected gauge",
                    f"nats_connected {1 if nats_stats.get('connected', False) else 0}",
                    
                    f"# HELP nats_publish_count_total Total NATS publishes",
                    f"# TYPE nats_publish_count_total counter",
                    f"nats_publish_count_total {nats_stats.get('publish_count', 0)}",
                    
                    f"# HELP nats_error_count_total Total NATS errors",
                    f"# TYPE nats_error_count_total counter",
                    f"nats_error_count_total {nats_stats.get('error_count', 0)}",
                ])
                
                # Firehose metrics
                firehose_stats = health_status.get("components", {}).get("firehose", {}).get("stats", {})
                metrics_lines.extend([
                    f"# HELP firehose_messages_processed_total Total firehose messages processed",
                    f"# TYPE firehose_messages_processed_total counter",
                    f"firehose_messages_processed_total {firehose_stats.get('messages_processed', 0)}",
                    
                    f"# HELP firehose_posts_extracted_total Total posts extracted from firehose",
                    f"# TYPE firehose_posts_extracted_total counter",
                    f"firehose_posts_extracted_total {firehose_stats.get('posts_extracted', 0)}",
                    
                    f"# HELP firehose_messages_per_second Current firehose message rate",
                    f"# TYPE firehose_messages_per_second gauge",
                    f"firehose_messages_per_second {firehose_stats.get('messages_per_second', 0)}",
                ])
                
                metrics_text = "\n".join(metrics_lines) + "\n"
                
                return Response(
                    content=metrics_text,
                    media_type="text/plain; version=0.0.4; charset=utf-8"
                )
                
            except Exception as e:
                logger.error("Metrics endpoint failed", error=str(e))
                return JSONResponse(
                    content={"error": str(e)},
                    status_code=500
                )
        
        @self.app.get("/ready")
        async def readiness_check():
            """Kubernetes readiness probe endpoint."""
            try:
                # More strict check for readiness
                is_ready = (
                    self.service.running and
                    self.service.nats_client.connected and
                    self.service.firehose_client and
                    self.service.firehose_client.running
                )
                
                status_code = 200 if is_ready else 503
                
                return JSONResponse(
                    content={
                        "status": "ready" if is_ready else "not_ready",
                        "service": settings.service_name
                    },
                    status_code=status_code
                )
            except Exception as e:
                logger.error("Readiness check failed", error=str(e))
                return JSONResponse(
                    content={
                        "status": "not_ready",
                        "error": str(e),
                        "service": settings.service_name
                    },
                    status_code=503
                )
    
    async def start(self) -> None:
        """Start the health check server in the current async context."""
        try:
            config = uvicorn.Config(
                app=self.app,
                host="0.0.0.0",
                port=settings.health_check_port,
                log_level=settings.log_level.lower(),
                access_log=False  # Reduce noise in logs
            )
            
            self.server = uvicorn.Server(config)
            
            logger.info("Health check server starting", 
                       port=settings.health_check_port)
            
            # This will run the server in the current async context
            await self.server.serve()
            
        except Exception as e:
            logger.error("Failed to start health check server", error=str(e))
            raise
    
    async def stop(self) -> None:
        """Stop the health check server."""
        if self.server:
            self.server.should_exit = True
            logger.info("Health check server stop requested")
            
            # Give it a moment to shut down gracefully
            await asyncio.sleep(1.0)
        
        logger.info("Health check server stopped")