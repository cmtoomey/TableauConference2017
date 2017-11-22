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
    cursor.execute("select id, name, workbook_url, username, site_id, site_luid, sitename, luid, case when (date_part('day',now()-last_view_time)>=30 and date_part('day',now()-last_view_time)<60) then 30 when (date_part('day',now()-last_view_time)>=60 and date_part('day',now()-last_view_time)<90) then 60 when (date_part('day',now()-last_view_time)>=90) then 90 else 0 end as days_since_last_view from (select w.id, w.name, w.workbook_url, u.name as username, w.site_id, wkb.luid, sites.luid as site_luid, sites.url_namespace as sitename, max(vs.last_view_time) as last_view_time from _workbooks w left join _users u on w.owner_id = u.id left join _views_stats vs on w.id=vs.views_workbook_id join workbooks wkb on w.id=wkb.id join sites on sites.id=w.site_id where date_part('day',now()-w.created_at)>=180 group by 1,2,3,4,5,6,7,8 order by 5) a")
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


# workbooks = pg_workbooks()
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

#Deleting Workbooks
def find_tagged_wb(site,auth):
    url=tab_server_api+"/api/2.6/sites/"+site+"/workbooks?filter=tags:in:[30,60,90]&pageSize=300"
    header = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': auth
        }
    response = requests.request('get',url, headers=header, verify=False)
    json = response.json()
    return json['workbooks']

def delete_workbook(site,wb,auth):
    url=tab_server_api+'/api/2.6/sites/'+site+'/workbooks/'+wb
    header = {
        'content-type': "application/json",
        'accept': "application/json",
        'X-Tableau-Auth': auth
        }
    response = requests.request('delete', url, headers=header, verify=False)
    return response.text

for site in sites:
    auth = login(site['url_namespace'])
    print(site['url_namespace'])
    workbooks = find_tagged_wb(site['site_luid'],auth)
    workbooks_deleted = []
    if len(workbooks) > 0:
        workbook_tagged = workbooks['workbook']
        exempt_workbooks = []
        for wb in workbook_tagged:
            for tag in wb['tags']['tag']:
                if tag['label'] in ['EXEMPT','30','60']:
                    exempt_workbooks.append(wb['id'])
        [workbooks_deleted.append(wb) for wb in workbook_tagged if wb['id'] not in exempt_workbooks]
    else:
        print('No Workbooks here')
    print(len(workbooks_deleted))
    for wb in workbooks_deleted:
        delete_workbook(site['site_luid'],wb['id'],auth)
    slack.chat.post_message('#channel', 'Workbooks Deleted from '+site['url_namespace'])
    slack.chat.post_message('@user', 'Workbooks Deleted from '+site['url_namespace'])