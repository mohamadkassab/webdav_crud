from webdav3.client import Client
import pyodbc
import asyncio
import traceback
from static import Credentials
from logger import *
from functions import *
import sys
import requests

# session parameters 
session_options = {
 'webdav_hostname': Credentials.webdav_hostname,
 'webdav_login': Credentials.webdav_login,
 'webdav_password': Credentials.webdav_password,
 'webdav_protocol': Credentials.webdav_protocol,
 'webdav_verify_ssl': Credentials.webdav_verify_ssl,
 'webdav_max_version': Credentials.webdav_max_version,
 'chunk_transmission': 'ON',
 'connection_timeout': 10
}
# Specify the SQL Server connection details
# server   = Credentials.server
# database = Credentials.database
# username = Credentials.username
# password = Credentials.password
# in, out, remote , log files 
local_path_import_folder  = Credentials. local_path_import_folder 
local_path_exported_picture = Credentials.local_path_exported_picture
local_path_exported_xml = Credentials.local_path_exported_xml
local_path_logfolder = Credentials.local_path_logfolder
local_path_downloaded_xml = Credentials.local_path_downloaded_xml
webdav_path_upload_picture= Credentials. webdav_path_upload_picture
webdav_path_upload_xml = Credentials.webdav_path_upload_xml
webdav_path_download_xml  = Credentials.webdav_path_download_xml
local_webdav_upload_pictures = Credentials. local_webdav_upload_pictures
local_webdav_upload_xml = Credentials.local_webdav_upload_xml
xml_download_folder_name = Credentials. xml_download_folder_name 
webdav_https = Credentials.webdav_https
username_http = Credentials.webdav_login
password_http = Credentials.webdav_password
xml_upload_folder_name = Credentials.  xml_upload_folder_name
webdav_all_path = [webdav_path_upload_picture, webdav_path_upload_xml, webdav_path_download_xml]
local_all_path = [local_webdav_upload_pictures, local_webdav_upload_xml, local_path_import_folder , local_path_exported_picture, local_path_exported_xml, local_path_logfolder, local_path_downloaded_xml]
# Create the connection string
conn_str = Credentials.conn_str
# excution options:
use_remote_webdav_picture = False
use_remote_webdav_xml = True
download_files = True
upload_files= True
check_remote_folder_if_exist = False
check_remote_file_if_exist = False
check_remote_file_size = False
  

# entry point
try:   
   
   # Connect to the database
   login_timeout = 5
   cnxn = pyodbc.connect(conn_str, timeout=login_timeout)
   cursor = cnxn.cursor()
   
   # https session
   session = requests.Session()
   session.auth = (username_http, password_http)
   client = Client(session_options)
   
   # logger 
   logger = Logger.myLogger("log")
   logger.info("Execution Started !")
   logic_functions = Functions
   
   # check path availibility of folders
   # if logic_functions.CheckWebdavPath(webdav_all_path, client, logger):
   #    sys.exit()
   # if logic_functions.ChecklocalPath(local_all_path, logger):
   #    sys.exit()  
               
   # step 1: download from webdav xml orders & customers
   if download_files:
      asyncio.run(logic_functions.Download_files(webdav_path_download_xml, xml_download_folder_name, local_path_downloaded_xml, client, logger, webdav_https, session))

   # step 2 : iterate over pictures in the input folder & step 4: updaload to webdav xml files
   if upload_files:
      options = [use_remote_webdav_picture, use_remote_webdav_xml, check_remote_folder_if_exist, check_remote_file_if_exist, check_remote_file_size]
      asyncio.run(logic_functions.Upload_files(local_path_import_folder , local_path_exported_picture, local_path_exported_xml, webdav_path_upload_xml, webdav_path_upload_picture, logger, cursor, client, xml_upload_folder_name, session, webdav_https, options, local_webdav_upload_pictures, local_webdav_upload_xml))    

   # close connections
   logic_functions.CloseConnections(cursor, cnxn, session, logger)
   logger.info("Execution Ended !")
   
except Exception as e :
       logic_functions.CloseConnections(cursor, cnxn, session, logger)
       logger.error("file:main.py, function:main, error: " + str(e) + str(traceback))

