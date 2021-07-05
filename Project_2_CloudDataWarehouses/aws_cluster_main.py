import configparser
import pandas as pd
import boto3
import json
import time

KEY                    = None
SECRET                 = None

DWH_CLUSTER_TYPE       = None
DWH_NUM_NODES          = None
DWH_NODE_TYPE          = None

DWH_CLUSTER_IDENTIFIER = None
DWH_DB                 = None
DWH_DB_USER            = None
DWH_DB_PASSWORD        = None
DWH_PORT               = None

DWH_IAM_ROLE_NAME      = None


def config_parse_file():
    """This function parses dwh.cfg file for needed config attributes
    
    Params:
    
    Returns: None
    """
    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
        DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
        DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME

    print("Parsing the config file...")
    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
        DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
        DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

        DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

        DWH_DB = config.get("CLUSTER", "DB_NAME")
        DWH_DB_USER = config.get("CLUSTER", "DB_USER")
        DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
        DWH_PORT = config.get("CLUSTER", "DB_PORT")


def create_iam_role(iam):
    """This function creates an IAM Role
    
    Params:
    
    Returns: dwhRole 
    """
    global DWH_IAM_ROLE_NAME
    dwhRole = None
    try:
        print('Step 1: Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
        dwhRole = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    return dwhRole


def attach_iam_role_policy(iam):
    """This function attaches "AmazonS3ReadOnlyAccess" role policy to the created IAM role
    
    Params:
    
    Returns: boolean; if policy creation is successful, or not
    """
    global DWH_IAM_ROLE_NAME
    print('Step 2: Attaching "AmazonS3ReadOnlyAccess" policy')
    return iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode'] == 200


def get_iam_role_arn(iam):
    """This function calls an ARN string 
    
    Params: IAM resource client
    
    Returns: string
    """
    global DWH_IAM_ROLE_NAME
    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


def start_cluster_creation(redshift, roleArn):
    """This function creates a redshift cluster
    
    Params:
        redshift: redshift resource client
        roleArn: ARN
    
    Returns: boolean, if cluster has been successfully created, else False
    """
    global DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, \
        DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD
    print("2. Creating redshift cluster")
    try:
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]
        )
        print(f"Redshift cluster creation function returned status code: {response['ResponseMetadata']['HTTPStatusCode']}")
        return response['ResponseMetadata']['HTTPStatusCode'] == 200
    except Exception as e:
        print(e)
    return False


def config_persist_cluster_infos(redshift):
    """This function writes cluster endpoint and ARN to dwh.cfg
    
    Params: redshift resource client
    
    Returns: None
    """
    global DWH_CLUSTER_IDENTIFIER
    print("Writing cluster address and ARN to dwh.cfg ...")

    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    config = configparser.ConfigParser()

    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", cluster_props['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)

    config_parse_file()


def get_redshift_cluster_status(redshift):
    """This function Returns redshift cluster status
    
    Params: redshift resource client
    
    Returns: str
    """
    global DWH_CLUSTER_IDENTIFIER
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus']
    return cluster_status.lower()


def check_cluster_creation(redshift):
    """This function checks the availability, of a cluster
    
    Params: redshift client resource
    
    Returns: boolean
    """
    if get_redshift_cluster_status(redshift) == 'available':
        return True
    return False


def destroy_redshift_cluster(redshift):
    """This function deletes a cluster
    
    Params: redshift client resource
    
    Returns: None
    """
    global DWH_CLUSTER_IDENTIFIER
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


def aws_open_redshift_port(ec2, redshift):
    """This function opens a Redshift port on the VPC security groups
   
    Params:
        ec2: EC2 client resource
        redshift: redshift client resource
    
    Returns:None
    """
    global DWH_CLUSTER_IDENTIFIER, DWH_PORT
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        all_security_groups = list(vpc.security_groups.all())
        print(all_security_groups)
        defaultSg = all_security_groups[1]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def aws_resource(name, region):
    """This function creates AWS client resource
   
    Params: 
        name: name of the resource
        region: The region of the resource
    
    Returns:
    """
    global KEY, SECRET
    return boto3.resource(name, region_name=region, aws_access_key_id=KEY, aws_secret_access_key=SECRET)


def aws_client(service, region):
    """This function creates AWS client
   
    Params: 
        service: service
        region: region of the service
    
    Returns:
    """
    global KEY, SECRET
    return boto3.client(service, aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=region)

def main():
    config_parse_file()

    iam = aws_client('iam', "us-east-2")
    redshift = aws_client('redshift', "us-east-2")

    create_iam_role(iam)
    attach_iam_role_policy(iam)
    roleArn = get_iam_role_arn(iam)

    clusterCreationStarted = start_cluster_creation(redshift, roleArn)

    if clusterCreationStarted:
        print("The cluster is being created.")

if __name__ == '__main__':
    main()
