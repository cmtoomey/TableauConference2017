#import tableau server python library
import os
import tableauserverclient as TSC
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import yaml

with open("../config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    settings = cfg['settings']

username = settings['username']
password = settings['password']
url = settings['url']

#login
sites = ['SITE']
num_sites = range(len(sites))

#setup search_string
front = 'SQL-SER-LOC-'
end = ['003','004','005','007']
end_mac = '.dmz.url.url.com'

short_string_OG = [front+end[i] for i in range(4)]
long_string_OG = [short_string_OG[i]+end_mac for i in range(4)]

new_front = 'SQL-SER-NEW-'
new_end = ['003','004','002','002']
new_end_mac = '.dmz.url.url.com'
analytics = os.path.normpath("SQL-SER-NEW--002/Analytics")
econ = os.path.normpath("SQL-SER-NEW--002/Econ")

short_string = [new_front+new_end[i] for i in range(4)]
long_string = [short_string[i]+new_end_mac for i in range(4)]

short_string_new = []
long_string_new = []

for i in range(4):
    if i<2:
        short_string_new.append(short_string[i])
    elif i == 2:
        short_string_new.append(analytics)
    else:
        short_string_new.append(econ)

for i in range(4):
    if i<2:
        long_string_new.append(long_string[i])
    elif i == 2:
        long_string_new.append(long_string[i]+"\\Analytics")
    else:
        long_string_new.append(long_string[i]+"\\Econ")

#Workbook per Site
tableau_auth = TSC.TableauAuth(username, password)
# tableau_auth.site_id = 'Trulia'
server = TSC.Server(url)
server.add_http_options({'verify': False})
server.auth.sign_in(tableau_auth)

book_id = [wb.id for wb in TSC.Pager(server.workbooks)]
num_workbooks = range(len(book_id))

for a in num_workbooks:
    item = server.workbooks.get_by_id(book_id[a])
    server.workbooks.populate_connections(item)
    num_con = range(len(item.connections))
    for i in num_con:
        for j in range(4):
            if long_string_OG[j] == item.connections[i].server_address.lower():
                item.connections[i].server_address = long_string_new[j]
                server.workbooks.update_conn(item, item.connections[i])
            elif short_string_OG[j] == item.connections[i].server_address.lower():
                item.connections[i].server_address = short_string_new[j]
                server.workbooks.update_conn(item, item.connections[i])

server.auth.sign_out()