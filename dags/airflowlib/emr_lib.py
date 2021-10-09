import boto3, json, pprint, requests, textwrap, time, logging
import requests
from datetime import datetime
import configparser
from airflow.models import Variable
import os




dag_config = Variable.get("dag_config", deserialize_json=True)

KEY = Variable.get("KEY")
SECRET = Variable.get("SECRT")
region_name = dag_config["region_name"]

os.environ["AWS_ACCESS_KEY_ID"]=KEY
os.environ["AWS_SECRET_ACCESS_KEY"]=SECRET

# emr object

emr = boto3.client('emr',region_name =region_name,
                       aws_access_key_id =KEY,
                       aws_secret_access_key =SECRET
                      )


# redshift object

redshift = boto3.client('redshift',
                       region_name =region_name,
                       aws_access_key_id =KEY,
                       aws_secret_access_key =SECRET
                       )

#

def create_default_security_group():
    # Vpc_id ='vpc-9c5635f7' is the default vpc for the specified aws region
    """
        This Method is used to set the default security
        groups with inbound rules.

    """


    ec2 = boto3.resource('ec2',
                       region_name = region_name,
                       aws_access_key_id = KEY,
                       aws_secret_access_key = SECRET
                       )

    vpc = ec2.Vpc(id='vpc-9c5635f7')
    defaultSg1 = list(vpc.security_groups.all())[0]
    defaultSg = list(vpc.security_groups.all())[1]
    defaultSg2 = list(vpc.security_groups.all())[2]

    try:
        defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='tcp',
        FromPort=22,
        ToPort=22)
    except:
        logging.info("{} From Port:{} To Port:{} already exist.".format(defaultSg.group_name,22,22))
    try:
        defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='tcp',
        FromPort=80,
        ToPort=80)
    except:
        logging.info("{} From Port:{} To Port:{} already exist.".format(defaultSg.group_name,80,80))
    try:
        defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='tcp',
        FromPort=8998,
        ToPort=8998)
    except:
        logging.info("{} From Port:{} To Port:{} already exist.".format(defaultSg.group_name,8998,8998))
    try:
        defaultSg2.authorize_ingress(
        GroupName= defaultSg2.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=5439,
        ToPort=5439)
    except:
        logging.info("{} From Port:{} To Port:{} already exist.".format(defaultSg2.group_name,5439,5439))


    return [defaultSg.group_name,defaultSg1.group_name,defaultSg2.group_name]



def get_security_group_id(group_name):
    # Meethod used to get the security group id's
    client = boto3.client('ec2', region_name=region_name,aws_access_key_id = KEY,
                       aws_secret_access_key = SECRET)
    response = client.describe_security_groups(GroupNames=group_name)
    return response['SecurityGroups'][0]['GroupId']



def create_cluster(InstancePrfName,rolename,cluster_name='Airflow-' + str(datetime.now()), release_label='emr-5.9.0',master_instance_type='m4.xlarge', num_core_nodes=1, core_node_instance_type='m4.xlarge'):

    """
        creating an Elastic Map Reduce(EMR) cluster
        with Elastic compute cloud(EC2) engine.
    """

    group_names = create_default_security_group()
    EMR_master = [group_names[0]]
    EMR_slave = [group_names[1]]
    logging.info('emr master group_name == {}, emr slave group_name == {}'.format(EMR_master,EMR_slave))
    emr_master_security_group_id = get_security_group_id(EMR_master)
    emr_slave_security_group_id = get_security_group_id(EMR_slave)
    cluster_response = emr.run_job_flow(
        Name= cluster_name,
        ReleaseLabel= release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'charlesKey',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        VisibleToAllUsers=True,
        JobFlowRole=InstancePrfName,
        ServiceRole=rolename,
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'livy' },
            { 'Name': 'zeppelin' }
        ]
    )


    return cluster_response['JobFlowId']





def create_redshfit_cluster(roleArn,db_name,redshiftCluster_id,db_user,db_password,node_count,cluster_type='multi-node',node_type='dc2.large'):

    """
        creating a redshift cluster
        with postgreSql engine.
    """

    try:
        response =redshift.create_cluster(DBName=db_name,
                                      ClusterIdentifier= redshiftCluster_id,
                                      ClusterType= cluster_type,
                                      NodeType= node_type,
                                      MasterUsername = db_user,
                                      MasterUserPassword=db_password,
                                      NumberOfNodes = int(node_count),
                                      IamRoles =[roleArn]
                                     )

    except:
        logging.info("redshift cluster failed to create. {},{},{},{},{},{}".format(redshiftCluster_id,node_count,
        db_name,roleArn,db_user,redshift))




def wait_for_emr_cluster_creation(cluster_id):
    # emr wait method until cluster is up and ready
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def wait_for_redshift_creation(redshiftCluster_id):
    # redshift wait method until cluster is up and ready
    waiter = redshift.get_waiter('cluster_available')
    waiter.wait(ClusterIdentifier = redshiftCluster_id)


def list_clusters():
    # emr list cluster method for clusters in waiting, running or starting state.
    response = emr.list_clusters(CreatedAfter=datetime(2021, 1, 1),CreatedBefore=datetime.now(),
                ClusterStates=['WAITING','RUNNING','STARTING'])
    if len(response['Clusters']) >= 1:
        return response['Clusters'][0]['Id']
    else:
        return 'None'



def get_redshift_endpoint(redshiftCluster_id):
    # get cluster endpoint method
    endpoint = redshift.describe_clusters(ClusterIdentifier=redshiftCluster_id)['Clusters'][0]["Endpoint"]['Address']
    """
    try:
        ec2 = boto3.resource('ec2',
                           region_name = region_name,
                           aws_access_key_id = KEY,
                           aws_secret_access_key = SECRET
                           )
        print( "setting up ingress security")
        vpcid =redshift.describe_clusters(ClusterIdentifier=redshiftCluster_id)['Clusters'][0]["VpcId"]
        vpc = ec2.Vpc(id=vpcid)
        defaultSg = list(vpc.security_groups.all())[2]
        print("defaultSg = ",defaultSg)

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(5439),
            ToPort=int(5439)
            )
        print( "completed setting up ingress security")
    except Exception as e:
        print("security ingress rule failed", e)
    """
    return endpoint


def delete_redshift(redshiftCluster_id):
    # redshift wait method until cluster is deleted
    try:
        redshift.delete_cluster(ClusterIdentifier=redshiftCluster_id,SkipFinalClusterSnapshot=True)
        redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=redshiftCluster_id)
    except:
        logging.info("cluster already terminated")

def terminate_emr_cluster(cluster_id):
    # emr wait method until cluster is terminated
    try:
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        emr.get_waiter('cluster_terminated').wait(ClusterId=cluster_id)
    except:
        logging.info("cluster already terminated")

def get_emr_cluster_dns(cluster_id):
    # get the master_dns of the master node
    response = emr.describe_cluster(ClusterId=cluster_id)
    master_dns = response['Cluster']['MasterPublicDnsName']
    return master_dns

def create_spark_session(master_dns, kind='pyspark'):
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    data = {'kind':'pyspark'}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    print("this is the response =", response.json())
    return response.headers



def wait_for_idle_session(master_dns, response_headers):
    # wait for the session to be idle or ready for job submission
    status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    print("this is the session_url = ",session_url)
    while status != 'idle':
        time.sleep(10)
        status_response = requests.get(session_url, headers=response_headers)
        logging.info(status_response.json())
        status = status_response.json()['state']
        print("this is the status = ", status)
        logging.info('Session status:{}'.format(status))
    return session_url



def submit_statement(session_url,file_path):
    # Submits the script with a post request to the Livy server
    statements_url = session_url + '/statements'
    with open('{}'.format(file_path),'r') as f:
        code = f.read()
    data = {'code': code}
    response = requests.post(statements_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    logging.info(response.json())
    return response



def kill_spark_session(session_url):
    # kill the running session
    requests.delete(session_url, headers={'Content-Type': 'application/json'})



def track_statement_progress(master_dns, response_headers):
    # track statement progress of submitted jobs
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location'].split('/statements', 1)[0]
    # Poll the status of the submitted python code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Statement status: ' + statement_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            logging.info(line)

        if 'progress' in statement_response.json():
            logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
        logging.info('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            logging.info(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    logging.info('Final Statement Status: ' + final_statement_status)
