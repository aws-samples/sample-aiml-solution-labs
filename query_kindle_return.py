"""Query the refund agent about Kindle eBook return after 10 days"""
import sys
import os

# Set AWS profile if needed
os.environ['AWS_PROFILE'] = os.environ.get('AWS_PROFILE', 'default')

sys.path.append('usecases/amazon-returns-refunds-agent')

from refund_agent import agent

query = "Can I return Kindle eBook I bought accidentally 10 days ago?"

print(f"Query: {query}\n")
print("="*70 + "\n")

try:
    response = agent(query)
    print(f"Answer:\n{response}")
except Exception as e:
    print(f"Error: {e}")
