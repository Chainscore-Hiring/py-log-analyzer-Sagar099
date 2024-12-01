import pytest
import asyncio
from coordinator import Coordinator
from worker import Worker

@pytest.mark.asyncio
async def test_normal_processing():
    """Test normal log processing"""
    coordinator = Coordinator(port=8000)
    worker = Worker(port=9000, worker_id="worker1", coordinator_url="http://localhost:8000")
    await coordinator.start()
    await worker.start()

    # Simulate log file processing
    results = await worker.process_chunk("test_vectors/logs/normal.log", 0, 1024)
    assert results["request_count"] > 0

@pytest.mark.asyncio
async def test_worker_failure():
    """Test recovery from worker failure"""
    coordinator = Coordinator(port=8000)
    await coordinator.handle_worker_failure("worker1")
    assert "worker1" not in coordinator.workers

@pytest.mark.asyncio
async def test_malformed_logs():
    """Test handling of malformed logs"""
    worker = Worker(port=9000, worker_id="worker1", coordinator_url="http://localhost:8000")
    results = await worker.process_chunk("test_vectors/logs/malformed.log", 0, 1024)
    assert "error_count" in results
