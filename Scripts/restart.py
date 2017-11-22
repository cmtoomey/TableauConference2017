from __future__ import print_function

import json
import urllib
import boto3

print('Loading function')

ssm = boto3.client('ssm')


def lambda_handler(event, context):
    print('Running Cleanup')
    response = ssm.send_command(
        InstanceIds=['ID'],
        DocumentName='AWS-RunPowerShellScript',
        Parameters={ "commands":[ "tabadmin cleanup; tabadmin cleanup --restart" ]}
    )
