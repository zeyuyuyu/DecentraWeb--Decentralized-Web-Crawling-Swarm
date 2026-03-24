import os
import asyncio
from decentralized_swarm import SwarmAgent
from decentralized_governance import GovernanceProtocol

# Core logic for DecentraWeb
def main():
    # Initialize the swarm agent
    agent = SwarmAgent()
    
    # Join the decentralized swarm
    agent.join_swarm()
    
    # Participate in the decentralized governance protocol
    governance_protocol = GovernanceProtocol()
    governance_protocol.participate(agent)
    
    # Start the crawling and content aggregation process
    agent.start_crawling()
    
    # Run the event loop
    asyncio.run(agent.run())

if __name__ == "__main__":
    main()
