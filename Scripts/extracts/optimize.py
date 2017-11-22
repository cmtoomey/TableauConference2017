import pandas as pd
import numpy as np
import psycopg2
import pprint
import datetime as dt
import binascii
import json
import Crypto
import yaml
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
from Crypto.Cipher import PKCS1_v1_5
from base64 import b64decode
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# ### Here we set our connection information
# We also set the minimum priority, 5 in this case, so that we can save space for High Priority tasks driven by the business, not performance.
# We then set our maximum priority, 75 in this case, so that there is room for subscriptions.
with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

tableau_username = settings['username']
tableau_password = settings['password']
full_url = settings['url']
tab_server_url = settings['shorturl']
postgres_user = settings['postgres_user']
postgres_pwd = settings['postgres_password']
postgres_db = settings['postgres_database']
postgres_port = settings['postgres_port']
slack_token = settings['token']
slack = Slacker(slack_token)

# We add our priority information (min, max) here
min_priority = 5
max_priority = 75
delta = max_priority - min_priority
# ### Here we connect to our Postgres repository
conn_string = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (tab_server_url,postgres_db,postgres_user,postgres_pwd,postgres_port)
conn = psycopg2.connect(conn_string)
# #### Now that we are connected, let's get our background_tasks into a DataFrame
query = "select a.id, a.job_name, a.started_at, a.completed_at, a.finish_code, a.site_id, c.schedule_id, a.correlation_id, b.name from background_jobs a join sites b on a.site_id = b.id join tasks c on a.correlation_id = c.id where a.job_type like '%extract%'"
df = pd.read_sql(query, con=conn)
# ### Compute completion time
df['diff']=df['completed_at']-df['started_at']
# ### Convert the data to seconds, using numpy
df['diff']=df['diff'] / np.timedelta64(1, 's')
# ### Aggregation is up next. 
# Group by site, schedule and the correlation id.
# *What is correlation id?*
# Correlation ID is the Postgres identifier for the task (in this case the workbook refresh). It can also be a data source refresh
df_agg = df.groupby(['site_id','schedule_id','correlation_id']).agg({'correlation_id':'size','finish_code':sum, 'diff':'mean'})
df_agg.rename(columns={'correlation_id':'count_rows'}, inplace=True)
# This says group by site, schedule and task, and then aggregates each column independently.
# 1. Size: how many records do we have
# 2. Finish_code: 1 = failure, so sum will be how many failures we have had
# 3. Diff: average time to complete
# ### Compute the completion rate for each job
df_agg['completion'] = (df_agg['finish_code']/df_agg['count_rows'])+1
# ### Compute the penalty for failed jobs
df_agg['penalty'] = df_agg['diff']*df_agg['completion']
# ### Rank penalty within each Site and Schedule
df_agg['rank'] = df_agg.groupby(['site_id','schedule_id'])['penalty'].rank()
df_len = df_agg.groupby(['site_id','schedule_id']).size()
df_len = pd.DataFrame(df_len, columns=['group_size'])
df_len.reset_index(inplace=True)
# ### Cleanup functions
df_agg.reset_index(inplace=True)
df_full = df_agg.merge(df_len, on=['site_id','schedule_id'])
# ### Calculate Priority
# *This needs to be an integer for Tableau Server*
df_full['rank_1'] = (df_full['rank']-1)
df_full['steps'] = np.where(df_full['group_size'] == 1, 1, delta/(df_full['group_size']-1))
df_full['min'] = min_priority
df_full['priority'] = (df_full['steps']*df_full['rank_1']+min_priority).astype(int)
# ### Drop off everything but correlation_id and priority
columns = ['correlation_id','priority']
df_full = df_full[columns]
df_full.set_index('correlation_id', inplace=True)
# Now create a dictionary to iterate over for calling VizPortalAPI
priority = []
for index, row in df_full.iterrows():
    priority.append({'id':index, 'priority':row['priority']})
# ### Tableau Setup Functions
def _encode_for_display(text):
    """
    Encodes strings so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.
    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')
# Establish a session so we can retain the cookies
session = requests.Session()
def generatePublicKey():
      payload = "{\"method\":\"generatePublicKey\",\"params\":{}}"
      endpoint = "generatePublicKey"
      url = full_url + "/vizportal/api/web/v1/"+endpoint
      headers = {
      'content-type': "application/json;charset=UTF-8",
      'accept': "application/json, text/plain, */*",
      'cache-control': "no-cache"
      }
      response = session.post(url, data=payload, headers=headers, verify=False)
      response_text = json.loads(_encode_for_display(response.text))
      response_values = {"keyId":response_text["result"]["keyId"], "n":response_text["result"]["key"]["n"],"e":response_text["result"]["key"]["e"]}
      return response_values
# Generate a public key that will be used to encrypt the user's password
public_key = generatePublicKey()
pk = public_key["keyId"]
# Encrypt with RSA public key (it's important to use PKCS11)
def assymmetric_encrypt(val, public_key):
     modulusDecoded = long(public_key["n"], 16)
     exponentDecoded = long(public_key["e"], 16)
     keyPub = RSA.construct((modulusDecoded, exponentDecoded))
     # Generate a cypher using the PKCS1.5 standard
     cipher = PKCS1_v1_5.new(keyPub)
     return cipher.encrypt(val)
# Encrypt the password used to login
encryptedPassword = assymmetric_encrypt(tableau_password,public_key)
def vizportalLogin(encryptedPassword, keyId):
     encodedPassword = binascii.b2a_hex(encryptedPassword)
     payload = "{\"method\":\"login\",\"params\":{\""+username+"\":\"%s\", \"encryptedPassword\":\"%s\", \"keyId\":\"%s\"}}" % (tableau_username, encodedPassword,keyId)
     endpoint = "login"
     url = full_url + "/vizportal/api/web/v1/"+endpoint
     headers = {
     'content-type': "application/json;charset=UTF-8",
     'accept': "application/json, text/plain, */*",
     'cache-control': "no-cache"
     }
     response = session.post(url, data=payload, headers=headers,verify=False)
     return response
login_response = vizportalLogin(encryptedPassword, pk)
if login_response.status_code == 200:
    print "Login to Vizportal Successful!"
sc = login_response.headers["Set-Cookie"]
headers = []
for item in sc.split(";"):
    if "workgroup" in item:
        headers.append(item.split("=")[1])
    elif "XSRF" in item:
        headers.append(item.split("=")[1])
workgroup_session_id, xsrf_token = headers[0], headers[1]
def setPriority(task,priority):
    payload = "{  \"method\": \"setExtractTasksPriority\",\"params\": {\"ids\": [\"%s\"],\"priority\": \"%s\"}}" % (task,priority)
    endpoint = "setExtractTasksPriority"
    url = full_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False) 
    print response

#This is just for generating a list of task IDs and what their calculated priority should be
for i in range(len(priority)):
    print str(priority[i]['id'])+' '+str(priority[i]['priority'])

#Example use case of the priority function
setPriority('6464',100)