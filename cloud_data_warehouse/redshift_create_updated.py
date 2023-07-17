import boto3
import configparser
import json
import time
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read("dwh.cfg")

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_DB_NAME = config.get("DWH", "DWH_DB_NAME")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_PORT = config.get("DWH", "DWH_DB_PORT")


def create_redshift_cluster():
    """
    Create a Redshift cluster on AWS and configure necessary resources.

    This function creates an IAM role, Redshift cluster, and updates the security group
    to allow incoming traffic.

    Args:
        None

    Returns:
        None
    """

    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )
    ec2 = boto3.resource(
        "ec2",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    # Create Redshift IAM role
    try:
        print("1.1 Creating a new IAM Role")
        dwh_role = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
        role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    except ClientError as e:
        print("Error creating or configuring IAM role:", str(e))
        return

    # Create Redshift cluster
    try:
        print("Creating Redshift cluster...")
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[role_arn],
        )
    except ClientError as e:
        print("Error creating Redshift cluster:", str(e))
        return

    # Wait for the cluster to become available
    try:
        print("Waiting for Redshift cluster to become available...")
        waiter = redshift.get_waiter("cluster_available")
        waiter.wait(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        print("Redshift cluster created and available")
    except ClientError as e:
        print("Error waiting for cluster availability:", str(e))

    # Update security group to allow incoming traffic
    try:
        vpc = ec2.Vpc(id=response["Cluster"]["VpcId"])
        default_sg = list(vpc.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
        print("Security group updated to allow incoming traffic.")
    except ClientError as e:
        print("Error updating security group:", str(e))


# Call the function to create the Redshift cluster
create_redshift_cluster()
