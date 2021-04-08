import json
import boto3
import os
import sys
import subprocess
print('Loading function')


def lambda_handler(event, context):
    
    client = boto3.client('emr', region_name='us-east-1')
    # TODO: get current month and append to title
    # Change code to loop through each region
    
    response = client.run_job_flow(
    Name="Customer Segmentation Refresh",
    LogUri='s3n://aws-logs-934181291337-us-east-1/elasticmapreduce/',
    ReleaseLabel='emr-5.24.0',
    Instances={
        'InstanceGroups': [
            {'Name': 'Master',
            'InstanceRole': 'MASTER',
            'Market':'ON_DEMAND',
            'InstanceType': 'r4.8xlarge',
            'InstanceCount': 1,
            'EbsConfiguration': {"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":256,"VolumeType":"gp2"},"VolumesPerInstance":1}]}},
            {'Name': 'Slave',
            'InstanceRole': 'CORE',
            'Market':'ON_DEMAND',
            'InstanceType': 'r4.4xlarge',
            'InstanceCount': 4,
            'EbsConfiguration': {"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":256,"VolumeType":"gp2"},"VolumesPerInstance":1}]}},
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2KeyName': 'ec2_private_2019q2',
        'Ec2SubnetId': 'subnet-00e0692d',
        'EmrManagedMasterSecurityGroup': 'sg-2b045656',
        'EmrManagedSlaveSecurityGroup': 'sg-25045658',
        'ServiceAccessSecurityGroup': 'sg-2704565a',
        'AdditionalMasterSecurityGroups': [
            'sg-a2d2f9df',
        ],
        'AdditionalSlaveSecurityGroups': [
            'sg-a2d2f9df',
        ]
    },
    Applications=[
        {
            'Name': 'Spark'
        }
    ],
    Configurations=[
        {
            "Classification":"emrfs-site","Properties":{"fs.s3.consistent.retryPeriodSeconds":"10","fs.s3.consistent":"true","fs.s3.consistent.retryCount":"5","fs.s3.consistent.metadata.tableName":"EmrFSMetadata"}
        },
        {
            "Classification":"spark-defaults","Properties":{"spark.executor.memory":"36G","spark.driver.memory":"15G","spark.driver.maxResultSize":"15G","spark.rdd.compress":"true","spark.serializer":"org.apache.spark.serializer.KryoSerializer","spark.executor.cores":"5","spark.executor.instances":"11","spark.sql.tungsten.enabled":"true"}
        },
        {
            "Classification":"yarn-site","Properties":{"yarn.nodemanager.pmem-check-enabled":"false","yarn.nodemanager.vmem-pmem-ratio":"4","yarn.nodemanager.vmem-check-enabled":"false"}
        }
    ],
    BootstrapActions=[
        {
            'Name': 'Load bootstrap',
            'ScriptBootstrapAction': {
                'Path': 's3://studiosresearch-applications/bootstrap/install_requirements.sh',
            }
        },
    ],
    Steps=[
        {
            'Name': 'Run FIRST - Segmentation Distances',
            'ActionOnFailure': 'CANCEL_AND_WAIT', # TERMINATE
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://studiosresearch-applications/run_first_seg_distances.py",
                         "--marketplace_name", "CA", "--marketplace_id", "1367890", "--end_date", "20200331", 
                         "--monthly_refresh_rate", "3", "--yearly_cadence", "3", "--base_label", "1201"]
            }
        },
        {
            'Name': 'Run LAST - Segmentation Other',
            'ActionOnFailure': 'CANCEL_AND_WAIT', # TERMINATE
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://studiosresearch-applications/run_last_other_seg_files.py",
                         "--marketplace_name", "CA", "--marketplace_id", "1367890", "--end_date", "20200331", 
                         "--monthly_refresh_rate", "3", "--yearly_cadence", "3", "--base_label", "1201"]
        }
    }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole', #'EC2-studiosresearch',
    ServiceRole='EMR_DefaultRole',
#     SecurityConfiguration='prod-security-SSE-TLS',
    ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
    Tags=[
        {
            'Key': 'emrClusterName',
            'Value': 'AL_EMR' 
        }
    ]
)
    
    print(response)
    if response:
        return "Started Cluster Successfully"
    else:
        return "Failed to start cluster"
    