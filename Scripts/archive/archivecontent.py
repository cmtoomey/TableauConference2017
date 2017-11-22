import tableauserverclient as TSC
import requests
import yaml
import os
import errno
from slacker import Slacker
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

username = settings['username']
password = settings['password']
url = settings['url']
slack_token = settings['token']
archive = settings['archive']

# This is for Slack Integration and creating our multithreaded pool
slack = Slacker(slack_token)
request_options = TSC.RequestOptions(pagesize=1000)
# # Next up, configure your login
tableau_auth = TSC.TableauAuth(username,password)
server = TSC.Server(url)
server.add_http_options({'verify':False})
# Now login, and gather all the information you need about sites.
# *Remember, Tableau will log you into the default site, and the server client doesn't have the **Site Switch** method yet*
server.auth.sign_in(tableau_auth)
# Create an array for Sites, populate it with Content URLs
sites = []
for site in TSC.Pager(server.sites, request_options):
    sites.append({'url':site.content_url,'name':site.name})
#This is how you access the individual attributes in the dictionary
print sites[0]['name']
# Helper function for creating folders
def mkdir_p(path):
    if not os.path.exists(path):
        os.mkdir(path)
# Now we are going to create a function that will get all the workbooks for the site
def getWorkbooks():
    workbooks = []
    for wb in TSC.Pager(server.workbooks, request_options):
        workbooks.append({'id':wb.id, 'name':wb.name,'project':wb.project_name})
    return workbooks
# Let's get all the workbooks and then take a look at the first one
serverWorkbooks = getWorkbooks()
#Get the first one to make sure things look ok
print len(serverWorkbooks)
print serverWorkbooks[0]
# Change our Directory to the root of where we want to write out the workbooks
os.chdir(archive)
# Now we get that, and will use that as the base to change into the correct folder for archival
root = os.getcwd()
# Now we change the directory for our current Site
os.chdir(root+'/'+sites[0]['name'])
# Next we map the working directory, and map workbooks to the Workbooks class
output = os.getcwd()
print output
workbooks = server.workbooks
# We are going to check if the project folder exists, if not, we will create it
numWorkbooks = range(len(serverWorkbooks))
for i in numWorkbooks:
    path = output+'/'+serverWorkbooks[i]['project']
    mkdir_p(path)
# Now let's write out all the workbooks for the default Site
for i in numWorkbooks:
    path = output + '/'+serverWorkbooks[i]['project']
    print 'Archiving '+str([i])+' of '+str(len(serverWorkbooks))+' '+serverWorkbooks[i]['name']
    workbooks.download(serverWorkbooks[i]['id'], filepath = path)
# Then we sign out and we'll repeat for the rest of the sites
slack.chat.post_message('@user', 'Workbooks archived')
server.auth.sign_out()
# This time, we'll use our site list to help us iterate
# We don't need the first one anymore, so let's get rid of that one
sites.pop(0)
print sites
numSites = range(len(sites))
# Login, do it again, log out
for i in numSites: 
    tableau_auth = TSC.TableauAuth(username,password,site_id=sites[i]['url'])
    server.auth.sign_in(tableau_auth)
    serverWorkbooks = getWorkbooks()
    print (sites[i]['url']+': '+str(len(serverWorkbooks)))
    workbooks = server.workbooks
    os.chdir(root+'/'+sites[i]['url'])
    #Now that you have them, count them and create your folders
    numWorkbooks = range(len(serverWorkbooks))
    output = os.getcwd()
    for j in numWorkbooks:
        path = output+'/'+serverWorkbooks[j]['project']
        mkdir_p(path)
    for k in numWorkbooks:
        path = output + '/'+serverWorkbooks[k]['project']
        print 'Archiving '+str([k])+' of '+str(len(serverWorkbooks))+' '+serverWorkbooks[k]['name']
        workbooks.download(serverWorkbooks[j]['id'], filepath = path)
    server.auth.sign_out()
    slack.chat.post_message('@user', 'Workbooks in '+sites[i]['url']+' archived')
slack.chat.post_message('@user', 'Archiving Completed')