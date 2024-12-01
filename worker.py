import argparse
import asyncio
import json
import aiohttp
from typing import Dict


class Worker:
    """Processes log chunks and reports results"""

    
    def __init__(self, port: int, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port

    async def start(self) -> None:
        """Register with the coordinator and start processing"""
        await self.register_with_coordinator()
        await self.heartbeat()

    async def register_with_coordinator(self) -> None:
        """Register this worker with the coordinator"""
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{self.coordinator_url}/register_worker",
                json={"worker_id": self.worker_id}
            )
            print(f"Worker {self.worker_id} registered with coordinator.")

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of the log file"""
        metrics = {"error_count": 0, "total_response_time": 0, "request_count": 0}
        with open(filepath, 'r') as file:
            file.seek(start)
            lines = file.read(size).splitlines()

            for line in lines:
                parts = line.split()
                if len(parts) < 4:
                    continue
                timestamp, level, *message = parts
                if level == "ERROR":
                    metrics["error_count"] += 1
                elif "processed in" in line:
                    time_ms = int(message[-1].replace("ms", ""))
                    metrics["total_response_time"] += time_ms
                    metrics["request_count"] += 1

        return metrics

    async def send_results(self, metrics: Dict) -> None:
        """Send processing results back to the coordinator"""
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{self.coordinator_url}/submit_results",
                json={"worker_id": self.worker_id, "metrics": metrics}
            )
            print(f"Worker {self.worker_id} sent results to coordinator.")

    async def heartbeat(self) -> None:
        """Send periodic heartbeats to the coordinator"""
        while True:
            await asyncio.sleep(10)
            print(f"Worker {self.worker_id} sending heartbeat.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Worker")
    parser.add_argument("--port", type=int, default=9000, help="Worker port")
    parser.add_argument("--id", type=str, required=True, help="Worker ID")
    parser.add_argument("--coordinator", type=str, required=True, help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    asyncio.run(worker.start())

    
    def start(self) -> None:
        """Start worker server"""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        pass

    async def process_chunk(self, filepath: str, start: int, size: int) -> dict:
        """Process a chunk of log file and return metrics"""
        pass

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    worker.start()
