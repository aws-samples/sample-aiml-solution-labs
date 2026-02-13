import boto3

ssm = boto3.client('ssm', region_name='us-east-1')
ec2 = boto3.client('ec2', region_name='us-east-1')

# Get all regions from EC2
regions = ec2.describe_regions()['Regions']

for region in regions:
    region_code = region['RegionName']
    
    # Get long name from SSM
    try:
        long_name = ssm.get_parameter(
            Name=f'/aws/service/global-infrastructure/regions/{region_code}/longName'
        )['Parameter']['Value']
        print(f"{long_name}: {region_code}")
    except:
        print(f"Unknown: {region_code}")
