    The Eye In The Sky

    Real time image processing from remote controlled drones

    <hr>
    <h3>1/ Overview</h3>

    This demo shows how MapR simplifies processing of real time events at scale.
    It displays analytics about the number of people counted in real time from video streams.

    Drones are controlled via WiFi.


    <hr>
    <h3>2/ Architecture</h3>

    The architecture can be split in 3 main functions :
    - ingestion of the video streams
    - processing of the video frames
    - user interface

    Each part uses differents python scripts than can be run on multiple nodes.

    2-1/ Ingestion : pilot.py
    The pilot script has two roles :
    - receiving the video stream from the drone and push the frames into the processing pipeline ("source" stream)
    - reading the flight instructions from a stream ("positions" stream) and send them to the drones

    It can be run in two modes :
    - "live" mode : video streams are coming from drones
    - "video" mode : video streams are played from recorded files

    The pilot script is usually run on the cluster in "video" mode, and on one (or multiple) edge node in "live" mode.


    2-2/ Processing : dispatcher.py and processor.py
    The dispatcher takes each frame from the "source" stream and send them to the first available processor.
    Once the processor has finished, it pushes the processed images in a "processed" stream.


    2-3/ User interface : teits_ui.py
    This script manages the web interface:
    - sends the images to the UI
    - get instructions to move the drones and pushes them to the "positions" stream


    All the application settings are defined in the "settingss.py" file.

    <hr>
    <h3>3/ Recommended infrastructure</h3>

    In "live" mode, this demo is typically run on a laptop using VMs
    In "video" mode, it can be run anywhere.

    This application has been developped on CentOS 7

    Drones used are the Ryze Tello.

    3-1/ Central cluster
    Single VM is usually enough.
    10GB RAM recommended.
    50GB storage.
    One NIC connected to the host network

    3-2/ Edge nodes
    1GB RAM
    10GB disk
    Two NICs : one connected to the cluster, one bridged to a WiFi interface to connect the drone.

    <hr>
    <h3>4/ Installation Ubuntu</h3>
    Run as Emeral user
    	The install has been worked to not run as root. It is assumed you installed Ubuntu and set up a main user. In this case the main user was Ezmeral
    	
    4-1/ Central cluster

    Prerequisites:
    - Ezmeral Data Fabric 6.2 or later
    - Ezmeral Data Fabric cluster has DB and streams


    Installation steps:
    4-1-1/ Set up user and get github project
    - go to your Data Fabric as MapR Admin user and add your Ezmeral user with full credentials. Ensure you can log into Data Fabric as Ezmreral user
    	TODO: use DF CLI to establish user on cluster
    - on Ubunto Ezmeral user home, git clone https://github.com/lla4um/hpe-df-eye-in-the-sky.git
    - cd hpe-df-eye-in-the-sky git repo
    
    4-1-2/ Configure settings : This is in the settings.py file
    - DRONE_MODE :
        - live : use with real drones. Run the pilot.py script on the laptop connected to the drone
        - video : use without drones. Import the videos you want to stream into the data/recording folder as {{zone_name}}.mp4
        DRONE_MODE = "video"    # "video" : plays video files, "live": send data from drones.
        
    - User Credentials
    	USERNAME = "ezmeral" # best to NOT to use MapR user
		PASSWORD = "your-pwd" # TODO: move to user input
        NO_FLIGHT = True  # when True, the flight commands aren't sent to the drones but live video will still stream in and be processed
    
    4-1-3/ install packages and python app to project folder listed in settings
    - run Setup 
    	sudo ./setup.sh
    		This installs all packages needed and all python3 code needed.
    - python3 configure.py
    	This will configure all Data Fabric Streams, Tables, and copy the code to the project location in Data Fabrci.
    	It will also clean out any existing images, recordings, streams, and tables.
    	Run every time you need to clean up space
    	Project folder is set in settings to /mapr/your-cluster/projects/teits
    		# TODO: create volumes for project folder, and this project, 
    - source init.sh
    - You are ready to run if no errors occured


    4-2/ Edge node: TODO: Not tested yet with ubuntu
    Prerequisistes:
    - MapR client configured to access the main cluster

    Installation steps:
    - go to the project folder using global namespace
    - source init.sh
    	
    <h3>5/ Installation CentOS</h3>
    This section is outdated. Latest install works on Ubuntu 20.04
    Run as root !

    5-1/ Central cluster

    Prerequisites:
    - MapR 6.1 cluster with DB and streams

    Installation steps:
    - go to your mapr FS root
    - git clone https://github.com/xkr269/e984339581c76ec6117de4ee341ae6f4.git
    - mv e984339581c76ec6117de4ee341ae6f4 teits
    - cd teits
    - ./setup.sh
    - python configure.py
    - source init.sh

    Configure settings :
    - DRONE_MODE :
        - live : use with real drones. Run the pilot.py script on the laptop connected to the drone
        - video : use without drones. Import the videos you want to stream into the data/recording folder as {{zone_name}}.mp4


    5-2/ Edge node
    Prerequisistes:
    - MapR client configured to access the main cluster

    Installation steps:
    - go to the project folder using global namespace
    - source init.sh

    <hr>
    
    <h3>6/ Run the application on Ubuntu</h3>

    6-1/ Launch the main application on the cluster
    using ezmeral user
    	from the git project repo
    	source imnit.sh
    	python3 configure.py # to push andy changes in settings to project folder and to clean stale Data
    	python3 /mapr/my.cluster.com/projects/teits/start.py # start the demo from the project folder, not the repo but both locations will work.


    6-2/ Initial configuration of the drone environment
    your drones will be able to move around based on your instructions.
    Their movements are restricted to pre-defined zones.
    These zones have to be created before running the demo :
    - Access the zone editor : {{cluster_ip}}/edit
    - Create and position the zones for the drones

    Zones

    6-3/ Connect each edge VM to a drone and launch the pilot
    python pilot.py drone_N (ie. python pilot.py drone_1)

    
    <h3>7/ Run the application on CentOS</h3>

    7-1/ Launch the main application on the cluster
    python start.py


    7-2/ Initial configuration of the drone environment
    your drones will be able to move around based on your instructions.
    Their movements are restricted to pre-defined zones.
    These zones have to be created before running the demo :
    - Access the zone editor : {{cluster_ip}}/edit
    - Create and position the zones for the drones

    Zones

    7-3/ Connect each edge VM to a drone and launch the pilot
    python pilot.py drone_N (ie. python pilot.py drone_1)


    <hr>
    <h3> Interactive piloting </h3>
    You can remote control the drone using the keyboard.
    Default control keys are the following (using azerty keyboard):
    - Access the zone editor : {{cluster_ip}}/edit
    - Create and position the zones for the drones
    - "forward" : 'z'
    - "backward" : 's'
    - "left" : 'q'
    - "right" : 'd'
    - "up" : 'ArrowUp'
    - "down" : 'ArrowDown'
    - "clockwise" : 'ArrowRight'
    - "counter_clockwise" : 'ArrowLeft'
    - "flip" : 'f'
    - "takeoff" : 'Tab'
    - "land" : 'Backspace'



    You can change the control keys in the settings file.

    <hr>
    the clean.sh script is recommended to run if you have to relaunch the script.
    it kills all previous instances. 