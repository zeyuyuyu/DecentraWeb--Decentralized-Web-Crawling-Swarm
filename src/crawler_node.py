import asyncio
import random
from typing import List
from .web_crawler import WebCrawler

class CrawlerNode:
    def __init__(self, node_id: str, peer_nodes: List[str]):
        self.node_id = node_id
        self.peer_nodes = peer_nodes
        self.crawler = WebCrawler()
        self.task_queue = asyncio.Queue()
        self.running_tasks = set()

    async def run(self):
        await asyncio.gather(
            self.distribute_tasks(),
            self.execute_tasks(),
        )

    async def distribute_tasks(self):
        while True:
            task = await self.task_queue.get()
            await self.assign_task(task)
            self.task_queue.task_done()

    async def assign_task(self, task):
        if len(self.running_tasks) < 10:
            self.running_tasks.add(task)
            await self.crawler.crawl(task)
            self.running_tasks.remove(task)
        else:
            await self.offload_task(task)

    async def offload_task(self, task):
        peer_node = random.choice(self.peer_nodes)
        await peer_node.add_task(task)

    async def add_task(self, task):
        await self.task_queue.put(task)

    async def execute_tasks(self):
        while True:
            if not self.task_queue.empty():
                task = await self.task_queue.get()
                await self.assign_task(task)
                self.task_queue.task_done()
            await asyncio.sleep(1)
