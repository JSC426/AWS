#!/apollo/bin/env -e SDETools python

import sys
import os
import getpass
import shutil
from pyodinhttp import odin_retrieve_pair

def getUserMaterialName(materialSet=None):
    '''Construct MaterialSetName associated with each IAM user of AWS account
    if empty, will use $USERNAME and 'com.amazon.access.TEAM-USER-1' as a
    guess, otherwise take in the argument as literal'''

    if materialSet is None:
        return 'com.amazon.access.StudiosResearchProd-%s-1' % getpass.getuser()
    else:
        return materialSet

def getCredentialPair(materialSet):
    '''query Local odin deamon to retrieve the user/password for IAM user'''
    principal, credential = odin_retrieve_pair(materialSet)
    return (principal.data, credential.data)

def updateAWSCLIConfig():
    configfile = os.path.join(os.environ['HOME'],'.aws','config')
    # first copy the old file
    if os.path.exists(configfile):
        print("backing up old config file")
        shutil.copyfile(configfile, configfile+'.bk')

    fConfig = open(configfile,'w')
    fConfig.write('[default]\n')
    fConfig.write('region=us-east-1\n')
    fConfig.write('output=json\n')
    fConfig.close()

def updateAWSCLICredentials(key,secret):
    credfile = os.path.join(os.environ['HOME'],'.aws','credentials')
    # first copy the old file
    if os.path.exists(credfile):
        print("backing oup old credentials file")
        shutil.copyfile(credfile, credfile+'.bk')
    # overwrtie old file
    f = open(credfile,'w')
    f.write('[default]\n')
    f.write('aws_access_key_id=%s\n' % key)
    f.write('aws_secret_access_key=%s\n' % secret)
    f.close()

if __name__ == "__main__":
    materialName = getUserMaterialName()
    (key,secret) = getCredentialPair(materialName)
    if not os.path.exists(os.path.join(os.environ['HOME'],'.aws')):
        os.makedir(os.path.join(os.environ['HOME'],'.aws'))
    updateAWSCLIConfig()
    updateAWSCLICredentials(key,secret)
    print("AWSCLI Config update success!")
