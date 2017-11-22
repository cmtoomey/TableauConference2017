import psycopg2
import pprint
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from slacker import Slacker
import yaml

with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

admin = settings['username']
password = settings['password']
tab_server_api = settings['url']
tab_server_url = settings['shorturl']
postgres_user = settings['postgres_user']
postgres_pwd = settings['postgres_password']
postgres_db = settings['postgres_database']
postgres_port = settings['postgres_port']
slack_token = settings['token']
slack = Slacker(slack_token)
pp = pprint.PrettyPrinter(indent=2)

#Connect to Postgres and get workbooks
def pg_connect():
    global conn_string
    conn_string = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (tab_server_url,postgres_db,postgres_user,postgres_pwd,postgres_port)

    global conn
    try:
        conn = psycopg2.connect(conn_string)
    except Exception as err:
        print "Unexpected error while establishing connection:{}".format(err)
        sys.exit(1)

    global cursor
    cursor = conn.cursor()
    print "Connected!\n"

    return cursor

cursor = pg_connect()

def pg_workbooks():
    cursor.execute("select id, name, workbook_url, username, site_id, site_luid, sitename, luid, case when (date_part('day',now()-last_view_time)>=30 and date_part('day',now()-last_view_time)<60) then 30 when (date_part('day',now()-last_view_time)>=60 and date_part('day',now()-last_view_time)<90) then 60 when (date_part('day',now()-last_view_time)>=90) then 90 else 0 end as days_since_last_view from (select w.id, w.name, w.workbook_url, u.name as username, w.site_id, wkb.luid, sites.luid as site_luid, sites.url_namespace as sitename, max(vs.last_view_time) as last_view_time from _workbooks w left join _users u on w.owner_id = u.id left join _views_stats vs on w.id=vs.views_workbook_id join workbooks wkb on w.id=wkb.id join sites on sites.id=w.site_id where date_part('day',now()-w.created_at)>=180 and sites.url_namespace!='UserSandbox' group by 1,2,3,4,5,6,7,8 order by 5) a")
    workbooks = cursor.fetchall()
    workbook_data = []
    for row in workbooks:
        values = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]]
        record = {'id': values[0], 'workbookname':values[1],'workbookurl':values[2],'username':values[3],'siteid':values[4],'siteluid':values[5],'siteurl':values[6], 'workbookluid':values[7],'days':values[8]}
        workbook_data.append(record)
    return workbook_data

def pg_workbooks_sandbox():
    cursor.execute("select id, name, workbook_url, username, site_id, site_luid, sitename, luid, 90 as days_since_last_view from (select w.id, w.name, w.workbook_url, u.name as username, w.site_id, wkb.luid, sites.luid as site_luid, sites.url_namespace as sitename, max(vs.last_view_time) as last_view_time from _workbooks w left join _users u on w.owner_id = u.id left join _views_stats vs on w.id=vs.views_workbook_id join workbooks wkb on w.id=wkb.id join sites on sites.id=w.site_id where date_part('day',now()-w.created_at)>=90 and sites.url_namespace='UserSandbox' group by 1,2,3,4,5,6,7,8 order by 5) a")
    workbooks = cursor.fetchall()
    workbook_data = []
    for row in workbooks:
        values = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]]
        record = {'id': values[0], 'workbookname':values[1],'workbookurl':values[2],'username':values[3],'siteid':values[4],'siteluid':values[5],'siteurl':values[6], 'workbookluid':values[7],'days':values[8]}
        workbook_data.append(record)
    return workbook_data

def pg_sites():
    cursor.execute("select id, name, url_namespace, luid from sites order by 1")
    sites = cursor.fetchall()
    global sites_data
    sites_data = []

    for row in sites:
		values = [row[0], row[1], row[2], row[3]]
		record = {'site_id':values[0], 'site_name':values[1], 'url_namespace':values[2], 'site_luid':values[3]}
		sites_data.append(record)

    return sites_data


workbooks = pg_workbooks()
sandbox = pg_workbooks_sandbox()
sites = pg_sites()
conn.close()

#Login
def login(site_name):
    url = tab_server_api+"/api/2.6/auth/signin"
    payload = "{\n  \"credentials\": {\n    \"name\": \""+admin+"\",\n    \"password\": \""+password+"\",\n    \"site\": {\n      \"contentUrl\": \""+site_name+"\"\n    }\n  }\n}"
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        }
    response = requests.request("POST", url, data=payload, headers=headers, verify=False)
    token = response.json()
    return token.values()[0]['token']

def tag(site,workbook,tag,auth):
    url = tab_server_api+"/api/2.6/sites/"+site+'/workbooks/'+workbook+'/tags'
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': auth
        }
    payload = "{\n\t\"tags\":{\n\t\t\"tag\":[\n\t\t\t\t{\t\"label\":\""+str(tag)+"\"}\n\t\t\t]\n\t}\n}"
    requests.request("PUT", url, data=payload, headers=headers,verify=False)

def tag_delete(site,workbook,tag,auth):
    url = tab_server_api+"/api/2.6/sites/"+site+'/workbooks/'+workbook+'/tags/'+str(tag)
    headers = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': auth
        }
    response = requests.request("DELETE", url, headers=headers,verify=False)
    print(str(tag)+' removed from workbook: '+workbook)

def query_for_workbooks(site, tag, auth):
    url = tab_server_api+"/api/2.6/sites/"+site+"/workbooks?filter=tags:eq:"+str(tag)
    headers = {
    'accept': "application/json",
    'content-type': "application/json",
    'x-tableau-auth': auth
    }
    response = requests.request("GET", url, headers=headers, verify=False)
    if int(response.json()['pagination']['totalAvailable']) == 0:
        return []
    else:
        return response.json()['workbooks']['workbook']

#Tag Cleanup
exempt_keep = []
for site in sites:
    auth = login(site['url_namespace'])
    print("Signed in to "+site['url_namespace'])
    #Store workbooks here for tag removal
    exempt = query_for_workbooks(site['site_luid'],'EXEMPT',auth)
    workbook30 = query_for_workbooks(site['site_luid'],30,auth)
    workbook60 = query_for_workbooks(site['site_luid'],60,auth)
    workbook90 = query_for_workbooks(site['site_luid'],90,auth)
    #Push EXEMPT IDs
    for wb in exempt:
        exempt_keep.append(wb['id'])
    #Cleanup 30 tags
    if len(workbook30) == 0:
        print('No 30-Workbooks here')
    else:
        for wb in workbook30:
            tag_delete(site['site_luid'],wb['id'],30,auth)
    #Cleanup 60 tags
    if len(workbook60) == 0:
        print('No 60-Workbooks here')
    else:
        for wb in workbook60:
            tag_delete(site['site_luid'],wb['id'],60,auth)
    #Cleanup 90 tags
    if len(workbook90) == 0:
        print('No 90-Workbooks here')
    else:
        for wb in workbook90:
            tag_delete(site['site_luid'],wb['id'],90,auth)

pool = ThreadPoolExecutor(15)
# For Production Workbooks
for site in sites:
    print site['url_namespace']
    auth = login(site['url_namespace'])
    print("Signed in to "+site['url_namespace'])
    screened_workbooks = [v for v in workbooks if v['siteid'] == site['site_id'] and v['days']>0]
    filtered_workbooks = []
    for wb in screened_workbooks:
        if wb['workbookluid'] not in exempt_keep:
            filtered_workbooks.append(wb)    
    url_list = []
    headers = []
    payloads = []
    for wb in filtered_workbooks:
        url = tab_server_api+"/api/2.6/sites/"+site['site_luid']+"/workbooks/"+wb['workbookluid']+"/tags"
        url_list.append(url)
        header = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': auth
        }
        headers.append(header)
        payload = payload = "{\n\t\"tags\":{\n\t\t\"tag\":[\n\t\t\t\t{\t\"label\":\""+str(wb['days'])+"\"}\n\t\t\t]\n\t}\n}"
        payloads.append(payload)
        username = wb['username']        
        if wb['days'] == 30:            
            slack.chat.post_message('@'+username.lower(),'Your Workbook: '+wb['workbookname']+' has not been viewed for 30 days. No action is necessary. Workbooks with no views after 90 days will be deleted.')
        elif wb['days'] == 60:            
            slack.chat.post_message('@'+username.lower(),'Your Workbook: '+wb['workbookname']+' has not been viewed for 60 days. No action is necessary. Workbooks with no views after 90 days will be deleted.')
        else:            
            slack.chat.post_message('@'+username.lower(),'Your Workbook: '+wb['workbookname']+' has not been viewed for 90 days. This workbook is scheduled for deletion in 24 hours. To remove it from the queue, remove the 90 tag or add an EXEMPT tag')
    for index, url in enumerate(url_list):
        pool.submit(requests.request("PUT", url, data=payloads[index], headers=headers[index], verify=False))

#Sandbox Workbooks
auth = login('UserSandbox')
print("Signed in to "+'UserSandbox')
url_list = []
headers = []
payloads = []
filtered_workbooks = []
for wb in sandbox:
    if wb['workbookluid'] not in exempt_keep:
        filtered_workbooks.append(wb)   
for wb in filtered_workbooks:
    url = tab_server_api+"/api/2.6/sites/95b4803e-66ff-42e7-a41a-f86d9fdb5fdd/workbooks/"+wb['workbookluid']+"/tags"
    url_list.append(url)
    header = {
    'content-type': "application/json",
    'accept': "application/json",
    'X-Tableau-Auth': auth
    }
    headers.append(header)
    payload = payload = "{\n\t\"tags\":{\n\t\t\"tag\":[\n\t\t\t\t{\t\"label\":\""+str(90)+"\"}\n\t\t\t]\n\t}\n}"
    payloads.append(payload)
    username = wb['username']
    # slack.chat.post_message('@'+username.lower(),'Your Sandbox Workbook: '+wb['workbookname']+' is older than 90 days. This workbook is scheduled for deletion in 24 hours. To remove it from the queue, add an EXEMPT tag')
for index, url in enumerate(url_list):
    pool.submit(requests.request("PUT", url, data=payloads[index], headers=headers[index], verify=False))

slack.chat.post_message('@user', 'Content Tagging Completed')