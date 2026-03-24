import asyncio
import random
from typing import List
from .web_crawler import WebCrawler
from .consensus_manager import ConsensusManager

class CrawlerNode:
    def __init__(self, node_id: str, initial_urls: List[str]):
        self.node_id = node_id
        self.crawler = WebCrawler()
        self.consensus_manager = ConsensusManager(node_id)
        self.crawl_queue = initial_urls

    async def run(self):
        while self.crawl_queue:
            url = self.crawl_queue.pop(0)
            try:
                pages = await self.crawler.crawl(url)
                await self.consensus_manager.propose_pages(pages)
                self.crawl_queue.extend(pages)
            except Exception as e:
                print(f'Error crawling {url}: {e}')

            await asyncio.sleep(random.uniform(1, 5))

        await self.consensus_manager.shutdown()

class ConsensusManager:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.peers = []
        self.page_proposals = {}

    async def propose_pages(self, pages: List[str]):
        for page in pages:
            if page not in self.page_proposals:
                self.page_proposals[page] = {self.node_id: 1}
                await self.broadcast_proposal(page)
            else:
                self.page_proposals[page][self.node_id] = self.page_proposals[page].get(self.node_id, 0) + 1
                if self.page_proposals[page][self.node_id] >= len(self.peers) // 2 + 1:
                    await self.finalize_page(page)

    async def broadcast_proposal(self, page: str):
        for peer in self.peers:
            await peer.receive_proposal(page, self.node_id)

    async def receive_proposal(self, page: str, proposer_id: str):
        if page not in self.page_proposals:
            self.page_proposals[page] = {proposer_id: 1}
            await self.broadcast_proposal(page)
        else:
            self.page_proposals[page][proposer_id] = self.page_proposals[page].get(proposer_id, 0) + 1
            if self.page_proposals[page][proposer_id] >= len(self.peers) // 2 + 1:
                await self.finalize_page(page)

    async def finalize_page(self, page: str):
        print(f'Finalizing page: {page}')
        # Add page to the global crawl index
        self.page_proposals.pop(page)

    async def shutdown(self):
        await asyncio.gather(*[peer.shutdown() for peer in self.peers])
