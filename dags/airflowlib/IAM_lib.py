import boto3
import json
import requests
import logging
import configparser
from datetime import datetime
import numpy as np
import time
from airflow.models import Variable





dag_config = Variable.get("dag_config", deserialize_json=True)
KEY = Variable.get("KEY")
SECRET = Variable.get("SECRT")
region_name = dag_config["region_name"]


# iam:  identity and access management (iam), authentication to impersonate user
iam = boto3.client('iam',aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET,
                     region_name = region_name
                     )


def redshift_role(Rname):
    """
        Description:

            This function creates an identity and access management (iam)
            client role and assign roles or duties to various aws resource.
            AmazonS3ReadOnlyAccess role is assigned.
            The role identifier name is used to identfy this role.

        Arguments:
            Rname: Role identifier name.

        Returns:
            roleArn: the role Amazon resource name
    """

    try:
        role_response = iam.create_role(
                Path = '/',
                RoleName = Rname,
                Description = 'Allows Redshift clusters to call AWS services on your behalf.',
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                         'Version': '2012-10-17'}))


        iam.attach_role_policy(RoleName=Rname,
                      PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                      )['ResponseMetadata']['HTTPStatusCode']


        roleArn= iam.get_role(RoleName=Rname)['Role']['Arn']

    except:
        logging.info('{} already exist Exception.'.format(Rname))
        roleArn = iam.get_role(RoleName =Rname)['Role']['Arn']
        return roleArn
    else:
        return roleArn



def delete_redshift_role(Rname):
    # deleting the created role
    try:
        iam.detach_role_policy(RoleName = Rname, PolicyArn ='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        iam.delete_role(RoleName = Rname)
    except:
        logging.info("couldn't delete_redshift_role {} : NOT Successfull.".format(Rname))
    else:
        logging.info('delete_redshift_role, {} : Successfull'.format(Rname))

def create_role(Rname):

    """
        Description:

            This function creates an identity and access management (iam)
            client role and assign roles or duties to various aws resource.
            AmazonS3FullAccess role and AmazonElasticMapReduceFullAccess is assigned.
            The role identifier name is used to identfy this role.

        Arguments:
            Rname: Role identifier name.

        Returns:
            roleArn: the role Amazon resource name
    """

    try:

        role_response = iam.create_role(
                        Path = '/',
                        RoleName = Rname,
                        Description = 'Allows resource clusters to call AWS services on your behalf.',
                        AssumeRolePolicyDocument = json.dumps(
                            {'Statement': [{'Action':'sts:AssumeRole',
                               'Effect': 'Allow',
                               'Principal': {'Service':['redshift.amazonaws.com','s3.amazonaws.com','elasticmapreduce.amazonaws.com']}}],
                                 'Version': '2012-10-17'}),
                        MaxSessionDuration =39600)

        iam.attach_role_policy(RoleName=Rname,
        PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess')['ResponseMetadata']['HTTPStatusCode']
        iam.attach_role_policy(RoleName=Rname,
        PolicyArn = 'arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess')['ResponseMetadata']\
        ['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=Rname)['Role']['Arn']

    except:
        logging.info('{} already exist Exception.'.format(Rname))
        roleArn = iam.get_role(RoleName=Rname)['Role']['Arn']
        return roleArn
    else:
        return roleArn




def delete_created_role(Rname):
    # delete created role
    try:
        iam.detach_role_policy(RoleName = Rname, PolicyArn ='arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess')
        iam.detach_role_policy(RoleName = Rname, PolicyArn ='arn:aws:iam::aws:policy/AmazonS3FullAccess')
        iam.delete_role(RoleName = Rname)
    except:
        logging.info("couldn't delete_created_role {} : NOT Successfull.".format(Rname))
    else:
        logging.info('delete_created_role, {} : Successfull'.format(Rname))



def create_EMREC2role(Rname):

    """
        Description:

            This function creates an identity and access management (iam)
            client role and assign roles or duties to various aws resource.
            AmazonElasticMapReduceforEC2Role and AmazonS3FullAccess role is assigned. The role identifier
            name identifies this role.

        Arguments:
            Rname: Role identifier name.

        Returns:
            roleArn: the role Amazon resource name.
    """

    try:

        role_response = iam.create_role(
                        Path = '/',
                        RoleName = Rname,
                        Description = 'Default policy for the Amazon Elastic MapReduce for EC2 service role',
                        AssumeRolePolicyDocument = json.dumps({
                            "Version": "2012-10-17",
                        "Statement": [
                            {
                            "Effect": "Allow",
                            'Principal': {'Service':['s3.amazonaws.com','ec2.amazonaws.com','elasticmapreduce.amazonaws.com']},
                                "Action": ["sts:AssumeRole",
                                    ]}
                            ]}),MaxSessionDuration =39600)

        iam.attach_role_policy(RoleName = Rname,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')['ResponseMetadata']['HTTPStatusCode']
        iam.attach_role_policy(RoleName = Rname,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role')['ResponseMetadata']\
        ['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=Rname)['Role']['Arn']
    except:
        logging.info('{} already exist Exception.'.format(Rname))
        roleArn = iam.get_role(RoleName=Rname)['Role']['Arn']
        return roleArn
    else:
        return roleArn



def delete_created_EMREC2role(Rname):
    # delete created role
    try:
        iam.detach_role_policy(RoleName= Rname,PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role')
        iam.detach_role_policy(RoleName=Rname,PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
        iam.delete_role(RoleName=Rname)
    except:
        logging.info('delete_created_EMREC2role {} : NOT Successfull.'.format(Rname))
    else:
        logging.info('delete_created_EMREC2role {} : Successfull'.format(Rname))




def create_Attach_instanceProfileForEc2(InstancePrfName,Rname):
    # create instance profile for Ec2 clusters. used to add roles to the cluster
    try:
        iam.create_instance_profile(InstanceProfileName = InstancePrfName,Path='/')
        response =iam.add_role_to_instance_profile(InstanceProfileName=InstancePrfName,
                RoleName=Rname)
    except:
        logging.info('instanceProfile {} : not successfull : 404'.format(InstancePrfName))
    else:
        statuscode = response['ResponseMetadata']['HTTPStatusCode']
        logging.info("instanceProfile {} successfull: {}".format(InstancePrfName,statuscode))





def delete_instanceProfile(InstancePrfName,Rname):
    # delete the instance profile
    try:
        iam.remove_role_from_instance_profile(InstanceProfileName=InstancePrfName,
                    RoleName=Rname)
        iam.delete_instance_profile(InstanceProfileName=InstancePrfName)
    except:
        logging.info('delete_instanceProfile {} not successfull.'.format(InstancePrfName))
    else:
        logging.info('delete_InstanceProfile {} successfully'.format(InstancePrfName))
