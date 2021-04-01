#!/usr/bin/env python3
import json
import signal
import argparse
import subprocess
import re
import os
# import pandas as pd
from pprint import pprint
from math import floor

## removed the dependency on pandas, which servers no numerical computing purpose in this code
# df_ec2_specs = pd.DataFrame([
#     ['d2.8xlarge', 244, 36],
#     ['m3.2xlarge',  30,  8],
#     ['m4.xlarge',   16,  4],
#     ['m4.16xlarge', 25, 64],
#     ['c3.8xlarge',  60, 32],
#     ['c4.8xlarge',  60, 36],
#     ['r3.8xlarge', 244, 32],
#     ['r4.4xlarge', 122, 16],
#     ['r4.8xlarge', 244, 32],
#     ['r4.16xlarge',488, 64]
#     ],
#     columns = ['instance', 'mem', 'core'])
# df_ec2_specs.set_index('instance', inplace=True)

# Global constants
df_ec2_specs = {
  'd2.8xlarge' : {'mem_g':244, 'core':36},
  'm3.2xlarge' : {'mem_g': 30, 'core': 8},
  'm4.xlarge'  : {'mem_g': 16, 'core': 4},
  'm4.16xlarge': {'mem_g': 25, 'core':64},
  'c3.8xlarge' : {'mem_g': 60, 'core':32},
  'c4.8xlarge' : {'mem_g': 60, 'core':36},
  'r3.8xlarge' : {'mem_g':244, 'core':32},
  'r4.4xlarge' : {'mem_g':122, 'core':16},
  'r4.8xlarge' : {'mem_g':244, 'core':32},
  'r4.16xlarge': {'mem_g':488, 'core':64}
}
private_key = 'ec2_private_2019q2'

def make_spark_config(master_type, slave_type, slave_number):
    """ create proper spark driver/executor config based onand instance cpu/memory specs at """
    core_per_executor = 5 # magic number
    core_per_master_ec2 = df_ec2_specs[master_type]["core"]
    mem_per_master_ec2 = df_ec2_specs[master_type]["mem_g"]
    core_per_slave_ec2 = df_ec2_specs[slave_type]["core"]
    mem_per_slave_ec2 = df_ec2_specs[slave_type]["mem_g"]

    print("Total Cores: %d, Master(%d)+(%d)Slave(%d)"
        % (core_per_master_ec2 + slave_number * core_per_slave_ec2, core_per_master_ec2, slave_number, core_per_slave_ec2))
    print("Total Memory in GB:", mem_per_master_ec2  + slave_number * mem_per_slave_ec2)
    # print("core_per_slave_ec2:", core_per_slave_ec2)
    # print("core_per_executor:", core_per_executor)
    executor_num = slave_number * floor((core_per_slave_ec2 - 1) / core_per_executor) - 1 # magic
    mem_per_executor = (mem_per_slave_ec2-1) * 0.9 / floor(core_per_slave_ec2 / core_per_executor) # magic
    return int(floor(executor_num)), int(floor(mem_per_executor))

def make_instance_group(mastertype, masterebssize, coretype, corenum, coreebssize):
    """ Construct list of dicts for the EMR creation"""
    # master node config
    master_config_dict = {}
    master_config_dict["InstanceCount"] = 1
    master_config_dict["EbsConfiguration"] = {
        "EbsBlockDeviceConfigs":[
            {"VolumeSpecification":
                {
                    "SizeInGB":masterebssize,
                    "VolumeType":"gp2"
                },
             "VolumesPerInstance":1
            }
        ]
    }
    master_config_dict["InstanceGroupType"] = "MASTER"
    master_config_dict["InstanceType"] = mastertype
    master_config_dict["Name"] = "Master"

    # core nodes config
    core_config_dict = {}
    core_config_dict["InstanceCount"] = corenum
    core_config_dict["EbsConfiguration"] = {
        "EbsBlockDeviceConfigs":[
            {"VolumeSpecification":
                {
                    "SizeInGB":coreebssize,
                    "VolumeType":"gp2"
                },
             "VolumesPerInstance":1
            }
        ]
    }
    core_config_dict["InstanceGroupType"] = "Core"
    core_config_dict["InstanceType"] = coretype
    core_config_dict["Name"] = "Slave"

    # instance_group_json_pretty = json.dumps(
    #     [master_config_dict, core_config_dict],
    #     sort_keys=True,
    #     indent=4,
    #     separators=(',', ':'))

    return json.dumps([master_config_dict, core_config_dict])

def make_ec2_attributes():
    """ Construct JSON for EC2 attributes"""
    ec2_attr_dict = {
        "KeyName":private_key,
        "AdditionalSlaveSecurityGroups":["sg-a2d2f9df"],
        "InstanceProfile":"EMR_EC2_DefaultRole",
        "ServiceAccessSecurityGroup":"sg-2704565a",
        "SubnetId":"subnet-00e0692d",
        "EmrManagedSlaveSecurityGroup":"sg-25045658",
        "EmrManagedMasterSecurityGroup":"sg-2b045656",
        "AdditionalMasterSecurityGroups":["sg-a2d2f9df"]
    }

    # ec2_attr_json_pretty = json.dumps(
    #     ec2_attr_dict,
    #     sort_keys=True,
    #     separators=(',', ':'),
    #     indent=4)
    return json.dumps(ec2_attr_dict)

def make_emr_config(master_type, slave_type, slave_number):
    """ Construct JSON for EMRFS and Spark Config"""
    # calculate the SPARK configuration
    (executor_num, mem_per_executor) = make_spark_config(master_type, slave_type, slave_number)

    emrfs_config_dict = {
        "Classification":"emrfs-site",
        "Properties":{
            "fs.s3.consistent.retryPeriodSeconds":"10",
            "fs.s3.consistent":"true",
            "fs.s3.consistent.retryCount":"5",
            "fs.s3.consistent.metadata.tableName":"EmrFSMetadata"
        },
        "Configurations":[]
    }
    spark_config_dict = {
        "Classification":"spark-defaults",
        "Properties":{
            "spark.executor.memory":str(mem_per_executor)+"g",
            "spark.executor.memoryOverhead":str(floor(mem_per_executor*0.20))+"g",
            "spark.driver.memory":"15g",
            "spark.driver.memoryOverhead":"2g",
            "spark.driver.maxResultSize":"15g",
            "spark.executor.cores":"5",
            "spark.executor.instances":str(executor_num),
            "spark.serializer":"org.apache.spark.serializer.KryoSerializer",
            # "spark.serializer.objectStreamReset":"-1",
            "spark.kryoserializer.buffer.max":"512",
            "spark.sql.tungsten.enabled":"true",
            "spark.sql.execution.arrow.enabled": "true",
            "spark.rdd.compress":"true",
            # "spark.jars.packages": "com.johnsnowlabs.nlp:spark-nlp_2.11:2.6.3",
            "spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT":"1",
            "spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT":"1",
            "spark.sql.execution.arrow.pyspark.enabled":"true",
            "spark.sql.execution.arrow.pyspark.fallback.enabled":"true",
            "spark.sql.parquet.mergeSchema":"false",
            "spark.hadoop.parquet.enable.summary-metadata":"false",
            "spark.scheduler.listenerbus.eventqueue.capacity":"50000",
        },
        "Configurations":[]
    }

    spark_env_config_dict = {
        "Classification": "spark-env",
        "Properties":{
        	"ARROW_PRE_0_15_IPC_FORMAT": "1"
        },
        "Configurations": [
        {
         "Classification": "export",
         "Properties": {
            "PYSPARK_PYTHON": "/usr/bin/python3"
          }
        }
    ]
    }

    yarn_env_config_dict = {
    	"Classification": "yarn-env",
    	"Properties":{
        	"ARROW_PRE_0_15_IPC_FORMAT": "1"
        },
    	"Configurations":[
    	{
    		"Classification": "export",
    		"Properties":{
    			"PYSPARK_PYTHON":"/usr/bin/python3"
    		}
    	}
    	]	
    }

    yarn_config_dict = {
        "Classification": "yarn-site",
        "Properties": {
         "yarn.nodemanager.vmem-pmem-ratio": "4",
         "yarn.nodemanager.pmem-check-enabled": "false",
         "yarn.nodemanager.vmem-check-enabled": "false"
         }
    }

    # emr_config_json_pretty = json.dumps(
    #     [emrfs_config_dict, spark_config_dict],
    #     sort_keys=True,
    #     indent=4,
    #     separators=(',', ':'))
    return json.dumps([emrfs_config_dict, spark_config_dict, spark_env_config_dict, yarn_env_config_dict, yarn_config_dict])

# def start_emr_via_boto3(emrname, mastertype, masterebssize, coretype, corenum, coreebssize):
#     """create EMR clusters using boto3 emr run_job_flow() API, details see:
#     http://boto3.readthedocs.io/en/latest/reference/services/emr.html#EMR.Client.run_job_flow"""
#     client = boto3.client('emr')
#     resp = client.run_job_flow(
#         Name=emrname,
#         LogUri='s3n://aws-logs-934181291337-us-east-1/elasticmapreduce/',
#         AdditionalInfo='',
#         ReleaseLabel='emr-5.5.0',
#         # TODO: implement Insances argument constructor, very complicated depp json:
#         # see http://boto3.readthedocs.io/en/latest/reference/services/emr.html#EMR.Client.run_job_flow
#         # Instances=make_instance_group_json(mastertype, masterebssize, coretype, corenum, coreebssize),
#         # Configurations=make_emr_config(mastertype, coretype, corenum),
#         ServiceRole='EMR_DefaultRole',
#         SecurityConfiguration='prod-security-SSE-TLS',
#         Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}, {'Name': 'Ganglia'}, {'Name': 'Zeppelin'}],
#         ScaleDownBehavior='TERMINATE_AT_INSTANCE_HOUR'
#     )
#     return resp['JobFlowId']

def start_emr_via_awscli(cluster_name, master_type, master_ebs_size, slave_type, slave_num, slave_ebs_size, bootstrap_action):
    """create EMR cluster using AWSCLI 'emr create-cluster' API, via a subprocess"""
# AWSCLI API (slightly different in argument format)
# aws emr create-cluster \
# --applications Name=Hadoop Name=Spark Name=Zeppelin Name=Ganglia \
# --ec2-attributes '{"KeyName":"ec2_private_02_18","AdditionalSlaveSecurityGroups":["sg-a2d2f9df"],"InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-2704565a","SubnetId":"subnet-00e0692d","EmrManagedSlaveSecurityGroup":"sg-25045658","EmrManagedMasterSecurityGroup":"sg-2b045656","AdditionalMasterSecurityGroups":["sg-a2d2f9df"]}' \
# --service-role EMR_DefaultRole \
# --security-configuration 'prod-security-SSE-TLS' \
# --enable-debugging \
# --release-label emr-5.9.0 \
# --log-uri 's3n://aws-logs-934181291337-us-east-1/elasticmapreduce/' \
# --name 'XX@ETLTest1-15-r3.8xl' \
# --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"r3.8xlarge","Name":"Master - 1"},{"InstanceCount":14,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"r3.8xlarge","Name":"Core - 2"}]' \
# --configurations '[{"Classification":"emrfs-site","Properties":{"fs.s3.consistent.retryPeriodSeconds":"10","fs.s3.consistent":"true","fs.s3.consistent.retryCount":"5","fs.s3.consistent.metadata.tableName":"EmrfsKurtisDynamo"},"Configurations":[]},{"Classification":"spark-defaults","Properties":{"spark.executor.memory":"36G","spark.driver.memory":"15G","spark.driver.maxResultSize":"15G","spark.executor.cores":"5","spark.executor.instances":"83"},"Configurations":[]}]' \
# --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR \
# --region us-east-1

    aws_top_args = ["aws", "emr", "create-cluster"]
    app_list_args = ["--applications"] + ["Name=%s" % app for app in ("Hadoop","Spark","Zeppelin","Ganglia")]#,"Hue","MXNet","Livy")]
    ec2_attr_args = ["--ec2-attributes", str(make_ec2_attributes())]
    service_role_args = ["--service-role", "EMR_DefaultRole"]
    security_config_args = ["--security-configuration", "prod-security-SSE-TLS"]
    debug_args = ["--enable-debugging"]
    emr_ver_args = ["--release-label", "emr-5.30.1"]
    log_uri_args = ["--log-uri", "s3n://aws-logs-934181291337-us-east-1/elasticmapreduce/"]
    name_args = ["--name", cluster_name]
    instance_grp_args = ["--instance-groups", str(make_instance_group(master_type, master_ebs_size, slave_type, slave_num, slave_ebs_size))]
    emr_config_args = ["--configurations", str(make_emr_config(master_type, slave_type, slave_num))]
    scale_down_args = ["--scale-down-behavior", "TERMINATE_AT_TASK_COMPLETION"]
    region_args = ["--region", "us-east-1"]
    if re.search('s3://', bootstrap_action):
        bootstrap_action_args =["--bootstrap-action", "Path=\"%s\"" % bootstrap_action]
    elif bootstrap_action == 'jpl':
        bootstrap_action_args =["--bootstrap-action", "Path=\"%s\"" % 's3://studiosresearch-applications/bootstrap/bootstrap_jpl_via_systemd.sh']
    else:
        bootstrap_action_args = []
    emr_name_tag_args = ["--tags", "emrClusterName=%s" % cluster_name]
    ebs_rootdevsize_args = ["--ebs-root-volume-size", "30"]

    shell_cmd = aws_top_args + app_list_args + ec2_attr_args + service_role_args \
              + security_config_args + debug_args + emr_ver_args + log_uri_args \
              + name_args + instance_grp_args + emr_config_args + bootstrap_action_args \
              + scale_down_args + region_args + emr_name_tag_args + ebs_rootdevsize_args
    return shell_cmd

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Spin up a EMR cluster.")
    parser.add_argument('--cluster_name', '-name',    help='cluster name', type=str, required=True)
    parser.add_argument('--master_type', '-mtype', help='master ec2 type',  type=str, required=True)
    parser.add_argument('--slave_type', '-stype',  help='slave ec2 type',  type=str, required=True)
    parser.add_argument('--slave_num', '-snum',    help='slave ec2 number', type=int, required=True)
    parser.add_argument('--master_ebs_size', '-mebs', help='master ec2 ebs size in GB',  type=int, default=256)
    parser.add_argument('--slave_ebs_size',  '-sebs', help='slave ec2 ebs size in GB',   type=int, default=256)
    parser.add_argument('--bootstrap_action',  '-bs', help='bootstrap scripts location in s3 with s3://path/to/script.sh',   type=str,
                        default='s3://studiosresearch-applications/bootstrap/bootstrap_jpl_via_systemd.sh', required=False)
    # install_scipy_numpy_pandas_mpl
    args = parser.parse_args()
    cluster_name = args.cluster_name
    master_type = args.master_type
    slave_type  = args.slave_type
    slave_num   = args.slave_num
    master_ebs_size = args.master_ebs_size
    slave_ebs_size  = args.slave_ebs_size
    bootstrap_action = args.bootstrap_action

    # instance_type_list = df_ec2_specs.index.tolist()
    instance_type_list = list(df_ec2_specs.keys())

    if master_type not in instance_type_list:
        raise ValueError('master type %s not in predfined instance type list:\n %s' % (master_type, ' / '.join(instance_type_list)))

    if slave_type not in instance_type_list:
        raise ValueError('master type %s not in predfined instance type list:\n %s' % (slave_type, ' / '.join(instance_type_list)))

    mycmd = start_emr_via_awscli(cluster_name=cluster_name,
                                 master_type=master_type,
                                 master_ebs_size=master_ebs_size,
                                 slave_type=slave_type,
                                 slave_ebs_size=slave_ebs_size,
                                 slave_num=slave_num,
                                 bootstrap_action=bootstrap_action
                                 )
    # echo the command to screen
    # print('Launch command:\n\n' + '\n'.join(mycmd))
    
    process = subprocess.Popen(mycmd,
                           stdout=subprocess.PIPE,
                           shell=False,
                           preexec_fn=os.setsid)
    # wait for the process to terminate
    out, err = process.communicate()
    if out:
        print("---------------EMR Launch Success!--------------")
        print(out.decode('ascii'))
    if err:
        print("EMR Launch Failed!")
        print(err)
    # kill the waiting
    # os.killpg(os.getpgid(pro.pid), signal.SIGTERM)
