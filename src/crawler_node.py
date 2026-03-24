import asyncio
import aiohttp
from typing import Set, Dict
import json
import random
from dataclasses import dataclass
from datetime import datetime

@dataclass
class CrawlResult:
    url: str
    timestamp: datetime
    content: str
    links: Set[str]
    metadata: Dict

class CrawlerNode:
    def __init__(self, node_id: str, bootstrap_peers: list[str] = None):
        self.node_id = node_id
        self.peers = set(bootstrap_peers or [])
        self.crawled_urls = set()
        self.results = []
        self.is_running = False

    async def start(self):
        self.is_running = True
        await asyncio.gather(
            self.peer_discovery_loop(),
            self.crawl_loop()
        )

    async def peer_discovery_loop(self):
        while self.is_running:
            for peer in list(self.peers):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(f'{peer}/peers') as resp:
                            if resp.status == 200:
                                new_peers = await resp.json()
                                self.peers.update(new_peers)
                except Exception as e:
                    self.peers.remove(peer)
            await asyncio.sleep(60)

    async def crawl_loop(self):
        async with aiohttp.ClientSession() as session:
            while self.is_running:
                if not self.peers:
                    await asyncio.sleep(5)
                    continue

                # Get work from random peer
                peer = random.choice(list(self.peers))
                try:
                    async with session.get(f'{peer}/next_url') as resp:
                        if resp.status == 200:
                            url = await resp.text()
                            if url not in self.crawled_urls:
                                await self.crawl_url(session, url)
                except Exception:
                    self.peers.remove(peer)
                await asyncio.sleep(1)

    async def crawl_url(self, session: aiohttp.ClientSession, url: str):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    links = self.extract_links(content)
                    
                    result = CrawlResult(
                        url=url,
                        timestamp=datetime.utcnow(),
                        content=content,
                        links=links,
                        metadata={
                            'status': resp.status,
                            'headers': dict(resp.headers)
                        }
                    )
                    
                    self.results.append(result)
                    self.crawled_urls.add(url)
                    
                    # Share results with peers
                    await self.broadcast_result(result)
        except Exception as e:
            print(f'Error crawling {url}: {str(e)}')

    def extract_links(self, content: str) -> Set[str]:
        # TODO: Implement link extraction
        return set()

    async def broadcast_result(self, result: CrawlResult):
        payload = {
            'url': result.url,
            'timestamp': result.timestamp.isoformat(),
            'links': list(result.links)
        }
        
        for peer in list(self.peers):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f'{peer}/result',
                        json=payload
                    ) as resp:
                        if resp.status != 200:
                            self.peers.remove(peer)
            except Exception:
                self.peers.remove(peer)

    async def stop(self):
        self.is_running = False
