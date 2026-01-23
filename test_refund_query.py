import sys
sys.path.insert(0, 'usecases/amazon-returns-refunds-agent')

from refund_agent import agent

query = "I bought a Kindle Book three days ago by accident in India. I want to get a refund, what date is the ETA if I request it now?"
response = agent(query)
print(response)
