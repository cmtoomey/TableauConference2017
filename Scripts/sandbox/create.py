#pip install requests
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
# You'll need to install the following modules
# I used PyCrypto which can be installed manually or using "pip install pycrypto"
import binascii
import Crypto
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
from Crypto.Cipher import PKCS1_v1_5
from base64 import b64decode
#don't forget to pip install boto3 for dev purposes. You won't need it on Lambda
import boto3
import yaml
from slacker import Slacker

#No Warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

tableau_username = settings['username']
tableau_password = settings['password']
tab_server_url = settings['url']
slack_token = settings['token']

#Step 1 - Login
#encrypt your username and password
# kms = boto3.client('kms')
# keyid = "arn:aws:kms:yourkmskey"
# username_encrypt = kms.encrypt(KeyId=keyid,Plaintext=tableau_username)
# cipher = username_encrypt.get('CiphertextBlob')
# username_decrypt = kms.decrypt(CiphertextBlob=cipher)
slack = Slacker(slack_token)

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
      url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
      headers = {
      'content-type': "application/json;charset=UTF-8",
      'accept': "application/json, text/plain, */*",
      'cache-control': "no-cache"
      }
      response = session.post(url, data=payload, headers=headers, verify=False)
      response_text = json.loads(_encode_for_display(response.text))
      response_values = {"keyId":response_text["result"]["keyId"], "n":response_text["result"]["key"]["n"],"e":response_text["result"]["key"]["e"]}
      return response_values

# Generate a pubilc key that will be used to encrypt the user's password
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
     payload = "{\"method\":\"login\",\"params\":{\"username\":\"%s\", \"encryptedPassword\":\"%s\", \"keyId\":\"%s\"}}" % (tableau_username, encodedPassword,keyId)
     endpoint = "login"
     url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
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

#Step 2 Switch to site you want - in this case it's UserSandbox
def userSandbox():
    payload = "{\"method\":\"switchSite\",\"params\":{\"urlName\":\"UserSandbox\"}}"
    endpoint = "switchSites"
    url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False)
    new_headers = response.headers['Set-Cookie']
    headers = []
    for item in new_headers.split(";"):
        if "workgroup" in item:
            headers.append(item.split("=")[1])
        elif "XSRF" in item:
            headers.append(item.split("=")[1])
    return headers
    
new_headers = userSandbox()
workgroup_session_id, xsrf_token = new_headers[0], new_headers[1]

#Step 3 - Find the Tableau_Publisher Group
def getGroups():
     payload = "{\"method\":\"getGroups\",\"params\":{\"order\":[{\"field\":\"name\",\"ascending\":true}],\"page\":{\"startIndex\":0,\"maxItems\":1000}}}"
     endpoint = "getGroups"
     url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
     headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
     response = session.post(url, data=payload, headers=headers,verify=False)     
     groups = response.json()
     name_array = groups['result']['groups']    
     for i in range(len(name_array)):
         #Change Tableau Publishers to the group name you want
         if name_array[i]['name']=='Tableau_Publishers':
             return name_array[i]['id']
             
publisher_group_id = getGroups()

#Step 4 - Get the publisher group usernames
def getPublisherGroupNames():
    payload = "{\"method\":\"getSiteUsers\",\"params\":{\"filter\":{\"operator\":\"and\",\"clauses\":[{\"operator\":\"has\",\"field\":\"groupIds\",\"value\":\"%s\"}]},\"order\":[{\"field\":\"displayName\",\"ascending\":true}],\"page\":{\"startIndex\":0,\"maxItems\":1000}}}" % (publisher_group_id)
    endpoint = "getSiteUsers"
    url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False)
    all_publisher_names = response.json()['result']['users']
    publisher_range = range(len(all_publisher_names))
    publisher_names = []
    for i in publisher_range:
        publisher_names.append(all_publisher_names[i]['username'].lower())
    return publisher_names

publisher_array = getPublisherGroupNames()

#Step 5 - Find all the projects
def getProjects():
    payload = "{\"method\":\"getProjects\",\"params\":{\"order\":[{\"field\":\"name\",\"ascending\":true}],\"page\":{\"startIndex\":0,\"maxItems\":1000}}}"
    endpoint = "getProjects"
    url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False)
    all_projects = response.json()['result']['projects']
    project_length = range(len(all_projects))
    project_names = []
    for i in project_length:
        project_names.append(all_projects[i]['name'])
    return project_names

#Step 6 - Compare the list of usernames to the list of projects
project_array = getProjects()
publisher_length  = range(len(publisher_array))
list_of_new_projects = []

for i in range(len(publisher_array)):
    if publisher_array[i].lower() not in project_array:
        list_of_new_projects.append(publisher_array[i].lower())

#Step 7 - Create Project with Project name as username, output ProjectID
def createProject(username):
    payload = "{\"method\": \"createProject\",\"params\": {\"name\": \"%s\"}}" % (username)
    endpoint = "createProject"
    url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False)
    return response.json()['result']['id']

#Step 8 - get UserID for current userSandbox
def getUserId(username):
    payload = "{\"method\":\"getSiteUsers\",\"params\":{\"filter\":{\"operator\":\"and\",\"clauses\":[{\"operator\":\"eq\",\"field\":\"username\",\"value\":\"%s\"},{\"operator\":\"eq\",\"field\":\"domainName\",\"value\":\"ZILLOW.LOCAL\"}]},\"page\":{\"startIndex\":0,\"maxItems\":1}}}" % (username)
    endpoint = "getSiteUsers"
    url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False)
    return response.json()['result']['users'][0]['id']

#Step 9 - Assign Ownership to user
def setProjectOwner(projectid,userid):
    payload = "{  \"method\": \"setProjectsOwner\",\"params\": {\"ids\": [\"%s\"],\"ownerId\": \"%s\"}}" % (projectid,userid)
    endpoint = "setProjectsOwner"
    url = tab_server_url + "/vizportal/api/web/v1/"+endpoint
    headers = {
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    'cache-control': "no-cache",
    'x-xsrf-token': xsrf_token,
    'cookie': "workgroup_session_id="+workgroup_session_id+"; XSRF-TOKEN="+xsrf_token
    }
    response = session.post(url, data=payload, headers=headers,verify=False) 

for i in range(len(list_of_new_projects)):
    slackname = "@"+list_of_new_projects[i]    
    slack.chat.post_message(slackname,'Welcome to $$$. You have been successfully added to the Tableau Publishers group and your Sandbox is ready! Please join us on #tableau if you have any questions. :partyparrot:',username='Tableaubot')
    slack.chat.post_message('@user','Tableau Sandbox created for '+list_of_new_projects[i])
    project_id = createProject(list_of_new_projects[i])
    user_id = getUserId(list_of_new_projects[i])    
    setProjectOwner(project_id,user_id)