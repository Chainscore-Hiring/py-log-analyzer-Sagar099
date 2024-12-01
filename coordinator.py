import argparse
import asyncio
import json
from aiohttp import web
from typing import Dict, List

class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int):
        self.port = port
        self.workers = {}
        self.results = {}
        self.task_queue = asyncio.Queue()
        self.app = web.Application()
        self.app.add_routes([
            web.post('/register_worker', self.register_worker),
            web.post('/submit_results', self.submit_results)
        ])
        print(f"Coordinator initialized on port {port}.")

    async def start(self) -> None:
        """Start the coordinator server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        print(f"Coordinator running on http://localhost:{self.port}")
        await site.start()

    async def distribute_work(self, filepath: str) -> None:
        """Split file and assign chunks to workers"""
        file_size = self.get_file_size(filepath)
        chunk_size = file_size // len(self.workers)

        start = 0
        for worker_id in self.workers.keys():
            end = start + chunk_size
            await self.task_queue.put((worker_id, filepath, start, chunk_size))
            start = end

        # Distribute remaining lines to the last worker
        if start < file_size:
            await self.task_queue.put((worker_id, filepath, start, file_size - start))

    async def register_worker(self, request: web.Request) -> web.Response:
        """Register a worker with the coordinator"""
        data = await request.json()
        worker_id = data['worker_id']
        self.workers[worker_id] = {'status': 'idle'}
        print(f"Worker {worker_id} registered.")
        return web.Response(text="Worker registered")

    async def submit_results(self, request: web.Request) -> web.Response:
        """Receive results from workers"""
        data = await request.json()
        self.results[data['worker_id']] = data['metrics']
        print(f"Received results from {data['worker_id']}.")
        return web.Response(text="Results received")

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        print(f"Worker {worker_id} failed. Reassigning tasks...")
        # Reassign the failed worker's tasks to other workers
        pass

    def get_file_size(self, filepath: str) -> int:
        """Get the size of a file"""
        with open(filepath, 'r') as file:
            file.seek(0, 2)
            return file.tell()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    asyncio.run(coordinator.start())

