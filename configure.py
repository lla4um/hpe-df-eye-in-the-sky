import fileinput
import os
import settings
import time
import subprocess
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

print("Starting pre-flight checks ... ")

# Create folders
if not os.path.exists(settings.ROOT_PATH):
    os.makedirs(settings.ROOT_PATH)
print("Root Project directory created " + settings.ROOT_PATH)

if not os.path.exists(settings.DATA_FOLDER):
    os.makedirs(settings.DATA_FOLDER)
print("Data directory created " + settings.DATA_FOLDER)

if not os.path.exists(settings.RECORDING_FOLDER):
    os.makedirs(settings.RECORDING_FOLDER)
print("Recording directory created " + settings.RECORDING_FOLDER)

if not os.path.exists(settings.LOG_FOLDER):
    os.makedirs(settings.LOG_FOLDER)
print("Log directory created " + settings.LOG_FOLDER)

# Create streams
print("Creating Ezmeral Data streams ...")

def create_stream(stream_path):
  if not os.path.islink(stream_path):
    print("creating stream: "+'maprcli stream create -path ' + stream_path + ' -produceperm p -consumeperm p -topicperm p -copyperm p -adminperm p')
    os.system('maprcli stream create -path ' + stream_path + ' -produceperm p -consumeperm p -topicperm p -copyperm p -adminperm p')

def create_table(table_path):
  if not os.path.islink(table_path):
  #maprcli table create -path <path> -tabletype json
    print("creating table: "+'maprcli table create -path ' + table_path + ' -tabletype json')
    os.system('maprcli table create -path ' + table_path + ' -tabletype json')


os.system("rm -rf " + settings.BASE_POSITIONS_STREAM)
print("removing any existing data at stream location " + settings.BASE_POSITIONS_STREAM)
create_stream(settings.BASE_POSITIONS_STREAM)
print("Positions stream created " + settings.BASE_POSITIONS_STREAM )
os.system("rm -rf " + settings.BASE_PROCESSORS_STREAM)
create_stream(settings.BASE_PROCESSORS_STREAM)
print("Processors stream created" + settings.BASE_PROCESSORS_STREAM)
os.system("rm -rf " + settings.BASE_VIDEO_STREAM)
create_stream(settings.BASE_VIDEO_STREAM)
print("Video stream created " + settings.BASE_VIDEO_STREAM)
os.system("rm -rf " + settings.BASE_RECORDING_STREAM)
create_stream(settings.BASE_RECORDING_STREAM)
print("Recording stream created " + settings.BASE_RECORDING_STREAM)


print("initializing Ezmeral Data Fabric Tables for drones")
os.system("rm -rf " + settings.DRONEDATA_TABLE)
DRONEDATA_TABLE = settings.DRONEDATA_TABLE
ZONES_TABLE = settings.ZONES_TABLE
CLUSTER_IP = settings.CLUSTER_IP
CLUSTER_NAME = settings.CLUSTER_NAME
SECURE_MODE = settings.SECURE_MODE
SSL_ENABLED = settings.SSL_ENABLED
username = settings.USERNAME
password = settings.PASSWORD
PEM_FILE = settings.PEM_FILE



# Initialize databases
#create tables with data fabric cli
create_table(settings.DRONEDATA_TABLE)
print("DRONEDATA_TABLE table created " + settings.DRONEDATA_TABLE )
create_table(settings.ZONES_TABLE)
print("ZONES_TABLE table created " + settings.ZONES_TABLE )

if SSL_ENABLED:
  print("using ssl connection")
  connection_str = "{}:5678?auth=basic;" \
                           "ssl=true;" \
                           "sslCA={};" \
                           "sslTargetNameOverride={};" \
                           "user={};" \
                           "password={}".format(CLUSTER_IP,PEM_FILE,CLUSTER_IP,username,password)
else:
  connection_str = "{}:5678?auth=basic;user={};password={};ssl=false".format(CLUSTER_IP,username,password)
print("connection_str: " + connection_str)

connection = ConnectionFactory.get_connection(connection_str=connection_str)
#connection = ConnectionFactory().get_connection(connection_str=connection_str)
dronedata_table = connection.get_or_create_store(DRONEDATA_TABLE)
print("creating ezmeral table for drone data at " + DRONEDATA_TABLE)
print("creating data entries for DRONE_ID's drone_1, drone_2, and drone_3")
for DRONE_ID in ["drone_1","drone_2","drone_3"]:
    dronedata_table.insert_or_replace({"_id":DRONE_ID,
                                       "flight_data":{"battery":50,"fly_speed":5.0},
                                       "log_data":"unset",
                                       "count":0,
                                       "connection_status":"disconnected",
                                       "position": {"zone":"home_base", "status":"landed","offset":0.0}})


zones_table = connection.get_or_create_store(ZONES_TABLE)
print("creating ezmeral table for drone zone assignmnets at " + ZONES_TABLE)
try:
  # Create home_base if doesn't exist
  zones_table.insert({"_id":"home_base","height":"10","left":"45","top":"45","width":"10","x":"0","y":"0"})
except:
  pass

print("updating init file")
os.system("sed -i 's/demo\.mapr\.com/{}/g' init.sh".format(CLUSTER_NAME))
os.system("sed -i 's/demo\.mapr\.com/{}/g' clean.sh".format(CLUSTER_NAME))

print("Configuration complete, initialize environment variables with source init.sh then run the aplication using start.py")
