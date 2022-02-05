#! /usr/bin/python


"""

The Eye In The Sky

Real time people detection from remote controlled drones video streams
TODO: convert to run as user and not root
to run the project (as root): 
- customize settings.py as needed
- python configure.py
- source init.sh
- python start.py

"""

import os
import settings
import time
import subprocess
import sys


#### Kill previous instances
current_pid = os.getpid()
all_pids = os.popen("ps aux | grep 'start.py' | awk '{print $2}'").read().split('\n')[:-1]
for pid in all_pids:
    if int(pid) != current_pid:
        print("killing {}".format(pid))
        os.system("kill -9 {}".format(pid))




def launch_script(script_name,arg=None):
	print("launching script .py " + script_name + " with arg list = " + arg )
    if arg:
        return subprocess.Popen(["python", settings.ROOT_PATH + script_name,arg,"remote"])
    else:
        return subprocess.Popen(["python", settings.ROOT_PATH + script_name])

def terminate_process(process):
    process.terminate()


processes = []


processes.append(launch_script("teits_ui.py"))
print("User interface started ... ")

# Receivers: launches receiver.py with a drone id  name/tag
if settings.REMOTE_MODE:
    for i in range(settings.ACTIVE_DRONES):
        processes.append(launch_script("receiver.py",arg="drone_"+str(i+1)))
        print("Receiver for drone_{} started ... ".format(i+1))
        time.sleep(1)


# Pilots: launches pilot.py with a drone id  name/tag
if settings.DRONE_MODE == "video":
    for i in range(settings.ACTIVE_DRONES):
        processes.append(launch_script("pilot.py",arg="drone_"+str(i+1)))
        print("Drone {} simulator started ... ".format(i+1))
        time.sleep(1)
        
if settings.DRONE_MODE == "live":
    for i in range(settings.ACTIVE_DRONES):
        processes.append(launch_script("pilot.py",arg="drone_"+str(i+1)))
        print("Drone {} simulator started ... ".format(i+1))
        time.sleep(1)

# Dispatcher: launches dispatcher.py
processes.append(launch_script("dispatcher.py"))
print("Dispatcher started ... ")

# Processor: for the number of processors in settings.NUMBER_OF_PROCESSORS, launch processor.py
for i in range(settings.NUMBER_OF_PROCESSORS):
    processes.append(launch_script("processor.py"))
    print("Processor {} started ... ".format(i+1))


while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        break

print('Terminating processes...')
for process in processes:
    terminate_process(process)
    print(".")
    time.sleep(1)


print("\n Terminated")


