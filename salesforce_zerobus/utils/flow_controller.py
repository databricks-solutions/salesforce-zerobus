"""
Flow Controller for Salesforce Pub/Sub API semaphore management.
Centralizes flow control logic with enhanced safety and monitoring.
"""

import logging
import threading
import time
from typing import Optional, Callable


class FlowController:
    """
    Enhanced flow controller for Salesforce Pub/Sub API streams.
    Provides thread-safe semaphore management with timeout protection,
    error recovery, and comprehensive monitoring.
    """
    
    def __init__(self, 
                 semaphore_count: int = 1,
                 acquire_timeout: float = 30.0,
                 max_consecutive_timeouts: int = 3,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize flow controller.
        
        Args:
            semaphore_count: Initial semaphore count (usually 1 for binary semaphore)
            acquire_timeout: Timeout in seconds for semaphore acquire operations
            max_consecutive_timeouts: Max consecutive timeouts before attempting recovery
            logger: Optional logger instance
        """
        self.semaphore = threading.Semaphore(semaphore_count)
        self.acquire_timeout = acquire_timeout
        self.max_consecutive_timeouts = max_consecutive_timeouts
        self.logger = logger or logging.getLogger(__name__)
        
        # Monitoring state
        self.consecutive_timeouts = 0
        self.total_acquires = 0
        self.total_releases = 0
        self.total_timeouts = 0
        self.total_recoveries = 0
        self.lock = threading.Lock()
        
        # Health monitoring
        self.last_successful_acquire = time.time()
        self.last_successful_release = time.time()
        
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Safely acquire semaphore with timeout protection and recovery.
        
        Args:
            timeout: Override default timeout for this acquire
            
        Returns:
            True if acquired successfully, False if timeout/recovery failed
        """
        effective_timeout = timeout or self.acquire_timeout
        
        with self.lock:
            self.total_acquires += 1
        
        acquired = self.semaphore.acquire(timeout=effective_timeout)
        
        if acquired:
            with self.lock:
                self.consecutive_timeouts = 0
                self.last_successful_acquire = time.time()
            
            self.logger.debug(f"Semaphore acquired successfully (timeout: {effective_timeout}s)")
            return True
        else:
            with self.lock:
                self.consecutive_timeouts += 1
                self.total_timeouts += 1
            
            # Only warn after multiple timeouts - single timeouts are normal for idle streams
            if self.consecutive_timeouts >= 2:
                self.logger.warning(
                    f"Multiple semaphore timeouts #{self.consecutive_timeouts} "
                    f"(waited {effective_timeout}s). Stream may be experiencing issues or high latency."
                )
            else:
                self.logger.info(
                    f"Semaphore timeout #{self.consecutive_timeouts} "
                    f"(waited {effective_timeout}s). Normal for idle streams - aligned with Salesforce 60s requirement."
                )
            
            # Attempt recovery if too many consecutive timeouts
            if self.consecutive_timeouts >= self.max_consecutive_timeouts:
                return self._attempt_recovery()
            
            return False
    
    def release(self) -> bool:
        """
        Safely release semaphore with error handling.
        
        Returns:
            True if released successfully, False on error
        """
        try:
            self.semaphore.release()
            
            with self.lock:
                self.total_releases += 1
                self.last_successful_release = time.time()
            
            self.logger.debug("Semaphore released successfully - ready for next fetch")
            return True
            
        except ValueError as e:
            self.logger.warning(f"Attempted to release semaphore beyond maximum value: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error releasing semaphore: {e}")
            return False
    
    def _attempt_recovery(self) -> bool:
        """
        Attempt to recover from deadlock by forcing semaphore release.
        
        Returns:
            True if recovery attempt was made, False if recovery failed
        """
        with self.lock:
            self.total_recoveries += 1
        
        self.logger.error(
            f"Flow controller deadlock detected after {self.max_consecutive_timeouts} "
            f"consecutive timeouts. Attempting recovery..."
        )
        
        try:
            # Force release to attempt recovery
            self.semaphore.release()
            self.logger.info("Recovery successful: forced semaphore release")
            
            with self.lock:
                self.consecutive_timeouts = 0
            
            return True
            
        except ValueError:
            self.logger.error("Recovery failed: semaphore was already at maximum value")
            return False
        except Exception as e:
            self.logger.error(f"Recovery failed with error: {e}")
            return False
    
    def get_health_status(self) -> dict:
        """
        Get current health status and statistics.
        
        Returns:
            Dictionary with health metrics
        """
        current_time = time.time()
        
        with self.lock:
            return {
                'total_acquires': self.total_acquires,
                'total_releases': self.total_releases,
                'total_timeouts': self.total_timeouts,
                'total_recoveries': self.total_recoveries,
                'consecutive_timeouts': self.consecutive_timeouts,
                'seconds_since_last_acquire': current_time - self.last_successful_acquire,
                'seconds_since_last_release': current_time - self.last_successful_release,
                'is_healthy': self.consecutive_timeouts < self.max_consecutive_timeouts,
                'timeout_rate': self.total_timeouts / max(self.total_acquires, 1)
            }
    
    def reset_stats(self):
        """Reset all statistics counters."""
        with self.lock:
            self.total_acquires = 0
            self.total_releases = 0
            self.total_timeouts = 0
            self.total_recoveries = 0
            self.consecutive_timeouts = 0
            self.last_successful_acquire = time.time()
            self.last_successful_release = time.time()
        
        self.logger.info("Flow controller statistics reset")
    
    def log_health_report(self):
        """Log a comprehensive health report."""
        status = self.get_health_status()
        
        self.logger.info(
            f"Flow Controller Health Report: "
            f"Acquires: {status['total_acquires']}, "
            f"Releases: {status['total_releases']}, "
            f"Timeouts: {status['total_timeouts']}, "
            f"Recoveries: {status['total_recoveries']}, "
            f"Timeout Rate: {status['timeout_rate']:.2%}, "
            f"Healthy: {status['is_healthy']}"
        )
    
    def __enter__(self):
        """Context manager support for safe acquire/release."""
        success = self.acquire()
        if not success:
            raise RuntimeError("Failed to acquire semaphore")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support for safe acquire/release."""
        self.release()
        return False  # Don't suppress exceptions


class FlowControlledFetchStream:
    """
    A wrapper that provides flow-controlled fetch request streaming
    with integrated health monitoring.
    """
    
    def __init__(self, 
                 flow_controller: FlowController,
                 request_factory: Callable,
                 health_report_interval: int = 100):
        """
        Initialize flow-controlled fetch stream.
        
        Args:
            flow_controller: FlowController instance
            request_factory: Function that creates fetch requests
            health_report_interval: How often to log health reports (every N requests)
        """
        self.flow_controller = flow_controller
        self.request_factory = request_factory
        self.health_report_interval = health_report_interval
        self.request_count = 0
    
    def __iter__(self):
        """Iterate over fetch requests with flow control."""
        while True:
            # Acquire semaphore with timeout protection
            if self.flow_controller.acquire():
                self.request_count += 1
                
                # Periodic health reporting
                if self.request_count % self.health_report_interval == 0:
                    self.flow_controller.log_health_report()
                
                # Yield the fetch request
                yield self.request_factory()
            else:
                # Failed to acquire semaphore - stream might be stuck
                self.flow_controller.logger.error("Failed to acquire semaphore - breaking fetch stream")
                break