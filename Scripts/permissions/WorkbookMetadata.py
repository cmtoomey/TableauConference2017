#Import your packages
from slacker import Slacker
import psycopg2
import boto3
import time
import datetime
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import multiprocessing as mp
from datetime import datetime
import yaml

#Set your credentials and other settings
with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

username = settings['username']
password = settings['password']
serverurl = settings['url']
slack_token = settings['token']
current_time = time.time()
slack = Slacker(slack_token)
#This will cause the record to expire in 180 days
#We rewrite every N days, so this will always get reset, unless content is deleted
expiry = current_time + 15552000
cpus=mp.cpu_count()
dynamo = boto3.client('dynamodb')
#Define your functions
def toDynamo(item):
    dynamo.put_item(
        TableName='Tableau_Workbooks',
        Item={
            'Name': {'S':item['name']},
            'ID': {'S': item['ID']},
            'Owner': {'S': item['owner']},
            'Site': {'S':item['site']},
            'Write': {'N': str(current_time)},
            'Expiry': {'N': str(expiry)},            
        }
    )

def updateDynamo(name,update):
    dynamo.update_item(
        TableName='Tableau_Workbooks',
        Key={
            'Name': {'S':name},
        },
        UpdateExpression='SET Perms = :r',
        ExpressionAttributeValues={
            ':r': {'S':update}
        }
    )

def login(site_name):
    url = serverurl+"/api/2.6/auth/signin"
    payload = "{\n  \"credentials\": {\n    \"name\": \""+username+"\",\n    \"password\": \""+password+"\",\n    \"site\": {\n      \"contentUrl\": \""+site_name+"\"\n    }\n  }\n}"
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        }
    response = requests.request("POST", url, data=payload, headers=headers, verify=False)
    token = response.json()
    return token.values()[0]['token']

def get_sites(auth):
    url = serverurl+"/api/2.6/sites"
    headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'X-Tableau-Auth': auth
    }
    response = requests.request('get', url=url, headers=headers)
    json = response.json()
    response_json = json['sites']['site']
    sites = []
    for response in response_json:
        values = [response['contentUrl'], response['id']]
        site = {'site_id':values[1], 'url':values[0]}
        sites.append(site)
    return sites

def get_workbooks(site_id,auth):
    url = serverurl+"/api/2.6/sites/"+site_id+"/workbooks?pageSize=1000"
    headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'X-Tableau-Auth': auth
    }
    response = requests.request('get', url=url, headers=headers)
    json = response.json()
    return json

def perms(data):
    url = serverurl+"/api/2.6/sites/"+data['siteid']+"/workbooks/"+data['workbook']+'/permissions'
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': data['auth']
        }
    response = requests.request("GET", url, headers=headers)
    updateDynamo(data['name'], response.text)
    
pool = mp.Pool(processes=cpus)

#Go
auth = login('')
sites = get_sites(auth)
workbooks_list = []
for site in sites:
    auth = login(site['url'])
    workbooks = get_workbooks(site['site_id'],auth)
    if len(workbooks['workbooks'])==0:
        print('No workbooks to archive')
    else: 
        workbook_filter = workbooks['workbooks']['workbook']        
        for workbook in workbook_filter:
            record = {'name':workbook['name'],'ID':workbook['id'],'owner':workbook['owner']['id'],'site':site['site_id']}
            workbooks_list.append(record)
pool.map(toDynamo,workbooks_list) 
workbook_ids = []
for site in sites:        
    site_id = site['site_id']
    auth = login(site['url'])
    workbooks = get_workbooks(site_id,auth)
    if len(workbooks['workbooks'])==0:
        print('No workbooks to archive')
    else: 
        workbook_filter = workbooks['workbooks']['workbook']  
        for workbook in workbook_filter:
            record = {'siteid':site_id,'workbook':workbook['id'],'auth':auth, 'name':workbook['name']}
            workbook_ids.append(record)
pool.map(perms,workbook_ids)
#Cleanup
pool.close() 
pool.join() 
slack.chat.post_message('@user', 'Workbook details archived')

#Access permissions with the following
# response = dynamo.get_item(
#     TableName =  'Tableau_Workbooks',
#     Key = {
#         'Name': {
#             'S': 'AdhocInboundAgentAnalysis'
#         }
#     }
    
# )