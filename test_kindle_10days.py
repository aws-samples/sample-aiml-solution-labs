"""
Test query: Can I return Kindle eBook I bought accidentally 10 days ago?
"""
import sys
sys.path.append('usecases/amazon-returns-refunds-agent')

from refund_agent import agent

query = "Can I return Kindle eBook I bought accidentally 10 days ago?"

print("Query:", query)
print("\n" + "="*70 + "\n")

response = agent(query)
print("Response:", response)
