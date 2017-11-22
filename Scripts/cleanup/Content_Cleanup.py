from slacker import Slacker
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import yaml

with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

username = settings['username']
password = settings['password']
serverurl = settings['url']
pg_user = settings['postgres_user']
pg_password = settings['postgres_password']
pg_database = settings['postgres_database']
pg_port = settings['postgres_port']
slack_token = settings['token']
archive = settings['archive']

slack = Slacker(slack_token)

def login(site_name):
    url = serverurl+"/api/2.6/auth/signin"
    payload = "{\n  \"credentials\": {\n    \"name\": \""+username+"\",\n    \"password\": \""+password+"\",\n    \"site\": {\n      \"contentUrl\": \""+site_name+"\"\n    }\n  }\n}"
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        }
    response = requests.request("POST", url, data=payload, headers=headers, verify=False)
    token = response.json()
    return [token.values()[0]['token'],token.values()[0]['user']['id']]

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

def get_users(site_id, auth):
    url = serverurl+"/api/2.6/sites/"+site_id+"/users?filter=siteRole:eq:Unlicensed"
    headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'X-Tableau-Auth': auth
    }
    response = requests.request('get', url=url, headers=headers)
    json = response.json()
    return json['users']

def get_workbooks(site_id,user_id,auth):
    url = serverurl+"/api/2.6/sites/"+site_id+"/users/"+user_id+"/workbooks?ownedBy=true&pageSize=1000"
    headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'X-Tableau-Auth': auth
    }
    response = requests.request('get', url=url, headers=headers)
    json = response.json()
    return json['workbooks']

def reassign(site_id,workbook_id,new_owner,auth):
    url = "https://tableau.server.url/api/2.6/sites/"+site_id+"/workbooks/"+workbook_id
    payload = "{\n    \"workbook\": {\n        \"owner\": {\n            \"id\": \""+new_owner+"\"\n        },\n        \"id\": \"7c0dbe3a-5a1b-4c3f-bd5e-c917d1f4866f\"\n    }\n}"
    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'x-tableau-auth': auth
        }
    requests.request("PUT", url, data=payload, headers=headers)

def tag(site,workbook,tag,auth):
    url = "https://tableau.server.url/api/2.6/sites/"+site+'/workbooks/'+workbook+'/tags'
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': auth
        }
    payload = "{\n\t\"tags\":{\n\t\t\"tag\":[\n\t\t\t\t{\t\"label\":\""+tag+"\"}\n\t\t\t]\n\t}\n}"
    requests.request("PUT", url, data=payload, headers=headers,verify=False)

def projects(site_id,auth):
    url = "https://tableau.server.url/api/2.6/sites/"+site_id+"/projects?pageSize=1000"
    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'x-tableau-auth': auth
        }

    response = requests.request("GET", url, headers=headers)
    return response.json()

def delete(site_id,project_id,auth):
    url = "https://tableau.server.url/api/2.6/sites/"+site_id+"/projects/"+project_id
    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'x-tableau-auth': auth
    }
    
    requests.request("DELETE", url, headers=headers)

def delete_user(site_id,user_id,auth):
    url = "https://tableau.server.url/api/2.6/sites/"+site_id+"/users/"+user_id

    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'x-tableau-auth': auth
        }

    response = requests.request("DELETE", url, headers=headers)
    return response.status_code

auth = login('')
sites = get_sites(auth[0])

#Get Unlicensed Users
reassignment_user = auth[1]

for site in sites:
    if site['url'] == 'UserSandbox':
        auth = login(site['url'])
        token = auth[0]
        reassignment_user = auth[1]
        workbook_to_reassign = []
        users = get_users(site['site_id'],token)
        if len(users)>0:
            user_list = users['user']
            for user in user_list:
                workbooks = get_workbooks(site['site_id'],user['id'],token)
                if len(workbooks)>0:
                    for item in workbooks['workbook']:
                        workbook_to_reassign.append(item['id'])
            slack.chat.post_message('@user', 'You have '+str(len(workbook_to_reassign))+" to reassign in the "+site['url']+" site")
            for workbook in workbook_to_reassign:
                tag(site['site_id'],workbook,'reassign',token)
                reassign(site['site_id'],workbook,reassignment_user,token)
            user_list_name = [user['name'] for user in users['user']]             
            project_list = projects(site['site_id'],token)
            project_list = project_list['projects']['project']
            project_delete = [project['id'] for project in project_list if project['name'] in user_list_name]
            for project in project_delete:
                delete(site['site_id'],project,token)
            for user in user_list:                                
                code = delete_user(site['site_id'],user['id'],token)
                if code == 409:
                    slack.chat.post_message('@user', 'Tableau User '+user['name']+' could not be deleted on Site: '+site['url'])
    else: 
        auth = login(site['url'])
        token = auth[0]
        reassignment_user = auth[1]
        workbook_to_reassign = []
        users = get_users(site['site_id'],token)
        if len(users)>0:
            user_list = users['user']
            for user in user_list:
                workbooks = get_workbooks(site['site_id'],user['id'],token)
                if len(workbooks)>0:
                    for item in workbooks['workbook']:
                        workbook_to_reassign.append(item['id'])
            slack.chat.post_message('@user', 'You have '+str(len(workbook_to_reassign))+" to reassign in the "+site['url']+" site")
            for workbook in workbook_to_reassign:
                tag(site['site_id'],workbook,'reassign',token)
                reassign(site['site_id'],workbook,reassignment_user,token)
            for user in user_list:                                
                code = delete_user(site['site_id'],user['id'],token)
                if code == 409:
                    slack.chat.post_message('@user', 'Tableau User '+user['name']+' could not be deleted on Site: '+site['url'])
slack.chat.post_message('@user','Cleanup completed')