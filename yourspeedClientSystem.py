############################################
#
#   yourspeed client-site system v.1.0
#   by Tushamanking
#
############################################
import paho.mqtt.client as mqtt
import time
import struct
import socket
import _thread
import math
import xml.etree.ElementTree as ET
import subprocess
import serial

print("yourspeed client-site system v.1.0 Starting")


############################################
#
#   system config read from XML file
#
############################################

tree = ET.parse('yourspeedClientSiteConfig.xml')
configXML = tree.getroot()

def wireXMLSystemEnable(enable):
    tree = ET.parse('yourspeedClientSiteConfig.xml')
    configXML = tree.getroot()

    for xmlChange in configXML.iter('enable'):
        xmlChange.text = str(enable)
        tree.write('yourspeedClientSiteConfig.xml')

def wireXMLSystemEnable(enable, batOn, batOff): #wireXMLSystemEnable with battery using mode
    tree = ET.parse('yourspeedClientSiteConfig.xml')
    configXML = tree.getroot()

    for xmlChange in configXML.iter('enable'):
        xmlChange.text = str(enable)
    
    for xmlChange in configXML.iter('batteryOff'):
        xmlChange.text = str(batOff)
    
    for xmlChange in configXML.iter('batteryOn'):
        xmlChange.text = str(batOn)

    tree.write('yourspeedClientSiteConfig.xml')

def wireXMLSystemRadarServiceEnable(set):
    tree = ET.parse('yourspeedClientSiteConfig.xml')
    configXML = tree.getroot()

    for xmlChange in configXML.iter('radarService'):
        xmlChange.text = str(set)
        tree.write('yourspeedClientSiteConfig.xml')

def wireXMLSystemRadarTimeWork(set): # set input list = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    tree = ET.parse('yourspeedClientSiteConfig.xml')
    configXML = tree.getroot()

    c = ""
    for x in set:
        c += str(x)

    for xmlChange in configXML.iter('timeWork'):
        xmlChange.text = c
        tree.write('yourspeedClientSiteConfig.xml')

def wireXMLRadar(id, enable, distance): # use when update configuration of radar
    tree = ET.parse('yourspeedClientSiteConfig.xml')
    configXML = tree.getroot()

    for allTag in configXML:
        if allTag.tag == "radar" :
            for radar in allTag:
                if int(radar.get('id')) == id:
                    radar.find('enable').text = str(enable)
                    radar.find('distance').text = str(distance)

    tree.write('yourspeedClientSiteConfig.xml')

def wireXMLLED(id, enable, warningSpeed, overSpeed, showTime, brink): # use when update configuration of led
    tree = ET.parse('yourspeedClientSiteConfig.xml')
    configXML = tree.getroot()

    for allTag in configXML:
        if allTag.tag == "led" :
            for led in allTag:
                if int(led.get('id')) == id:
                    led.find('enable').text = str(enable)
                    led.find('warningSpeed').text = str(warningSpeed)
                    led.find('overSpeed').text = str(overSpeed)
                    led.find('brink').text = str(brink)
                    led.find('showTime').text = str(showTime)

    tree.write('yourspeedClientSiteConfig.xml')

class systemOb():
    id = 0
    enable = 0
    radarService = 0
    localIP = "no-ip"
    relayUSBEnable = 0
    relayUSBSerialNumber = "no-serial"
    comPortEnable = 0
    comPort = "COM1"
    timeWorkEnable = 0
    timeWork = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ledEnable = 0
    batteryMode = 0
    batteryOff = 0
    batteryOn = 0
    batteryNow = 100
    
    def info(self):
        print("system ID : ", self.id)  
        print("system Enable : " , self.enable)  
        print("system Radar Service : " ,self.radarService)  
        print("system localIP : " + self.localIP)
        print("system Relay USB enable : " , str(self.relayUSBEnable))
        print("system Relay USB serial number : " + self.relayUSBSerialNumber)
        print("system Relay comport enable : " , self.comPortEnable)
        print("system Relay comport : " , self.comPort)
        print("system timeWorkEnable : " , self.timeWorkEnable)
        print("system timeWork : " , self.timeWork)
        print("system led enable", self.ledEnable)
        print("system battery mode :", self.batteryMode)
        print("system battery Off :", self.batteryOff)
        print("system battery On :", self.batteryOn)
        print(" ")    

class radarDeviceObect():
    radarID = 0
    ip = "no-ip"
    tcpPort = 0
    udpPort = 0
    enable = 0
    distance = 0
    speedOffset = 0

    def info(self) :
        print("radarID : " + str(self.radarID))
        print("ip address : " + str(self.ip))
        print("tcpPort : " + str(self.tcpPort))
        print("udpPort : " + str(self.udpPort))
        print("enable : " + str(self.enable))
        print("distance : " + str(self.distance))
        print("speedOffset : " + str(self.speedOffset))
        print(" ")

class ledDeviceObect():
    ledID = 0
    ip = "no-ip"
    port = 0
    enable = 0
    warningSpeed = 50
    overSpeed = 90
    brink = 0
    showTime = 0

    def info(self):
        print("led id : ", self.ledID, " ip :", self.ip, "port", self.port, "enable", self.enable, "warningSpeed",self.warningSpeed, "overSpeed",self.overSpeed, "brink",self.brink,"showTime",self.showTime)

radarDeviceList = [0] * 10
numberOfRadar = 0

ledDeviceList = [0] * 5
numberOfLED = 0

systemOb = systemOb()

# find all radar id config
for allTag in configXML:
    #print(deviceTag.tag)
    
    if allTag.tag == "radar" :
        radarIndex = 0
        for radar in allTag :
            #print("radar id ", radar.get('id'))
            #print(radar.find('ip').text)
            #print(radar.find('port').text)
            #print()
            #print("radarIndex" ,radarIndex)
            radarDeviceList[radarIndex] =  radarDeviceObect()
            radarDeviceList[radarIndex].radarID = int(radar.get('id'))
            radarDeviceList[radarIndex].ip = radar.find('ip').text
            radarDeviceList[radarIndex].tcpPort = int(radar.find('tcpPort').text)
            radarDeviceList[radarIndex].udpPort = int(radar.find('udpPort').text)
            radarDeviceList[radarIndex].enable = int(radar.find('enable').text)
            radarDeviceList[radarIndex].distance = int(radar.find('distance').text)
            radarDeviceList[radarIndex].speedOffset = int(radar.find('speedOffset').text)
            radarIndex = radarIndex + 1
            numberOfRadar = radarIndex

    if allTag.tag == "led" :
        ledIndex = 0
        for led in allTag:
            ledDeviceList[ledIndex] = ledDeviceObect()
            ledDeviceList[ledIndex].ledID = int(led.get('id'))
            ledDeviceList[ledIndex].ip = led.find('ip').text
            ledDeviceList[ledIndex].port = int(led.find('port').text)
            ledDeviceList[ledIndex].enable = int(led.find('enable').text)
            ledDeviceList[ledIndex].warningSpeed = int(led.find('warningSpeed').text)
            ledDeviceList[ledIndex].overSpeed = int(led.find('overSpeed').text)
            ledDeviceList[ledIndex].brink = int(led.find('brink').text)
            ledDeviceList[ledIndex].showTime = int(led.find('showTime').text)
            ledIndex += 1
            numberOfLED = ledIndex

    if allTag.tag == "system" :
        systemOb.id = int(allTag.find('id').text)
        systemOb.enable = int(allTag.find('enable').text)
        systemOb.radarService = int(allTag.find('radarService').text)
        systemOb.localIP = allTag.find('localIP').text
        systemOb.relayUSBEnable = int(allTag.find('relayUSBEnable').text)
        systemOb.relayUSBSerialNumber = allTag.find('relayUSBSerialNumber').text
        systemOb.comPortEnable = int(allTag.find('comPortEnable').text)
        systemOb.comPort = allTag.find('comPort').text
        systemOb.timeWorkEnable = int(allTag.find('timeWorkEnable').text)
        timeWork_t = allTag.find('timeWork').text
        
        x = 0
        for c in timeWork_t :
            systemOb.timeWork[x] = int(c)
            x += 1
        
        systemOb.ledEnable = int(allTag.find('ledEnable').text)
        systemOb.batteryMode = int(allTag.find('batteryMode').text)
        systemOb.batteryOff = int(allTag.find('batteryOff').text)
        systemOb.batteryOn = int(allTag.find('batteryOn').text)

#test zone
#time_work = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
#wireXMLSystemEnable(0)
#wireXMLSystemEnable(0, 90, 30)
#wireXMLSystemRadarServiceEnable(0)
#wireXMLSystemRadarTimeWork(time_work)
#wireXMLRadar(1, 1, 200) # set radar id 1
#wireXMLLED(1, 1, 100, 100, 100, 100)


print("------ radar information ------")
for x in range(numberOfRadar):
    radarDeviceList[x].info()

print("------ led information ------")
for x in range(numberOfLED):
    ledDeviceList[x].info() 

print ("------ system information ------")
systemOb.info()

print("------ system initial finished ------")
time.sleep(3)


############################################
#
#   MQTT Function
#
############################################

#mqttBrokerIp = "192.168.2.100" # pi eth0 ip
mqttBrokerIp = "localhost" # pi for test
mqttBrokerPort = 5000
mqttClient = mqtt.Client()
mqttClientPublishTopic = "radarTrafficData/" + str(systemOb.id) 
mqttConnectFlag = 0

def mqttConnect():
    global mqttBrokerIp, mqttBrokerPort, mqttClient
    try:
        mqttClient.on_connect = on_connect
        mqttClient.on_message = on_message
        mqttClient.on_diconnect = on_disconnect
        mqttClient.connect(host=mqttBrokerIp, port=mqttBrokerPort, keepalive=60)
        mqttClient.loop_start()  # start mqttClie loop

    except:
        print("can't connect to mqtt broker")

def mqttDisconnect():
    global mqttClient
    try :
        mqttClient.loop_stop(force=True)
    except:
        print("can't stop loop mqtt ")

    try :
        mqttClient.disconnect()
    except:
        print("can't disconnect mqtt ")

def getMqttConnectFlag():
    global mqttConnectFlag
    return mqttConnectFlag

def on_disconnect(client, userdata, rc):
    global mqttConnectFlag
    print("mqtt disconnected !! ")
    mqttConnectFlag = 0

def on_connect(client, userdata, flags, rc):
    global mqttConnectFlag, mySystemID
    print("Connected mqtt client to mqtt broker "+ str(rc))
    if rc == 0 :
        client.subscribe("speedTest", qos=0)
        client.subscribe('updateDisplay/' + str(mySystemID), qos=0)
        client.subscribe('updateRadar/' + str(mySystemID), qos=0)
        client.subscribe('updateSystem/' + str(mySystemID), qos=0)
        client.subscribe('updateTime/' + str(mySystemID), qos=0)
        client.subscribe("requestUpdateDisplayLocal", qos=0) #for update display local service
        mqttConnectFlag = 1
        print("connected to mqtt brocker")
    else :
        mqttConnectFlag = 0

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global mqttClient, radarServiceON_OFF, mySystemID
    #print(msg.topic + " " + str(msg.payload))

    if msg.topic == "speedTest":
        
        print("HI HI HI")
        #print(type(msg.payload.decode("utf-8")))
        messageRaw = msg.payload.decode("utf-8")
        message = messageRaw.split(",")
        #speed = int(message[0])
        #print(len(message))
        speed = int(message[0]) & 0xFF
        len = int(message[1]) & 0xFF
        '''
        if len(message) == 1 :
            speed = int(message[0]) & 0xFF
            len = 0x01
        else :
            speed = int(message[0]) & 0xFF
            len = int(message[1]) & 0xFF
        print("speed ",speed, "len ", len)
        '''
        mqttPublishTrafficData(mqttClient, 0x01, speed, len, 0x01)

    elif msg.topic == "updateSystem/" + str(mySystemID) :
        print("updateSystem/" + str(mySystemID))
        updateSystemConfig(msg.payload)
        
    elif msg.topic == "updateTime/" + str(mySystemID) :
        print("updateTime/" + str(mySystemID))
        udpateTimeWorkConfig(msg.payload)   

    elif msg.topic == "updateRadar/" + str(mySystemID):
        print("updateRadar/" + str(mySystemID))
        updateRadarConfig(msg.payload)
    
    elif msg.topic == "updateDisplay/" + str(mySystemID):
        print("updateDisplay/" + str(mySystemID))
        updateDisplayConfig(msg.payload)

    elif msg.topic == "requestUpdateDisplayLocal":
        print("-> request Update from Display Local")
        updateDisplayConfigLocal()

def mqttPublishTrafficData(mqttClient, carType, speed, lane, direction):
    global mqttClientPublishTopic, mySystemID

    toSentBuffer = bytearray(10)
    toSentBuffer[0] = 0xc7
    toSentBuffer[1] = 0x08
    toSentBuffer[2] = 0x01
    toSentBuffer[3] = (mySystemID & 0xFF)
    toSentBuffer[4] = carType
    toSentBuffer[5] = lane
    toSentBuffer[6] = (speed >> 8) & 0xFF
    toSentBuffer[7] = speed & 0xFF
    toSentBuffer[8] = direction
    toSentBuffer[9] = 0xc8
    try:
        mqttClient.publish(topic = mqttClientPublishTopic, payload =  toSentBuffer, qos=0, retain=False)
    except:
        print("can't publish to mqtt broker")

def mqttPubAggregate(mqttClient, data):
    global mySystemID
    _topic = "trafficAggregate/" + str(mySystemID)

    try:
        mqttClient.publish(topic = _topic, payload =  data, qos=0, retain=False)
    except:
        print("can't publish to mqtt broker")

############################################
#
#  update config function zone
#
############################################

def updateDisplayConfig(payload):
    global mySystemID
    if payload[0] == 0xc7 and payload[3] == mySystemID and payload[10] == 0xc8:
        print("-> display config " + str(mySystemID))
        displayTime, = struct.unpack('!H', payload[6:8])

        if  payload[5] != 0 :
            bk = 1
        else :
            bk = 0

        print("--display config update------")
        print("System id " + str(mySystemID))
        print("Display on/off id " + str(payload[4]))
        print("Display bink  " + str(bk))
        print("Display wraning speed " + str(payload[8]))
        print("Display over speed " + str(payload[9]))
        print("Display show time " + str(displayTime))
        print("-----------------------------")
        
        wireXMLLED(1, payload[4], payload[8], payload[9], displayTime, bk)

        ledDeviceList[0].enable = payload[4]
        ledDeviceList[0].warningSpeed = payload[8]
        ledDeviceList[0].overSpeed = payload[9]
        ledDeviceList[0].brink = bk
        ledDeviceList[0].showTime = displayTime

        ledDeviceList[0].info()
        updateDisplayConfigLocal()
        #setDisplayConfig(mySystemID, payload[4], bk, payload[8], payload[9], displayTime)
        #sendConfigDisplayToDisplaySerive()

def updateDisplayConfigLocal():
    global mySystemID

    buf = bytearray()
    buf.append(0xc7) #header
    buf.append(0x09)
    buf.append(0x11)
    buf.append(mySystemID & 0xFF) # displayID
    buf.append(ledDeviceList[0].enable & 0xFF) # on/off
    buf.append(ledDeviceList[0].brink & 0xFF)  # blink
    buf.append((ledDeviceList[0].showTime >> 8) & 0xFF)  # display_timeA
    buf.append(ledDeviceList[0].showTime & 0xFF)  # display_timeB
    buf.append(ledDeviceList[0].warningSpeed & 0xFF)  # worning_speed
    buf.append(ledDeviceList[0].overSpeed & 0xFF)  # over_speed
    buf.append(0xc8) # end

    mqttClient.publish(topic='updateDisplayLocal', payload=buf, qos=0, retain=False)
    print("<- send current display config to display service")

def sendDisplayStatusToServer():
    global mySystemID

    buf = bytearray()
    buf.append(0xc7) #header
    buf.append(0x04)
    buf.append(0x22)
    buf.append(mySystemID & 0xFF) # displayID
    buf.append(ledDeviceList[0].enable % 0xFF)  # display status on/off
    buf.append(0xc8) # end

    mqttClient.publish(topic='updateStatusDisplay', payload=buf, qos=0, retain=False)

def updateRadarConfig(payload):
    global mySystemID

    if payload[0] == 0xc7 and payload[6] == 0xc8 and payload[3] == mySystemID:

        radarDeviceList[0].enable = payload[4]
        radarDeviceList[0].distance = payload[5]

        wireXMLRadar(1, payload[4], payload[5]) # use when update configuration of radar

        print("--radar config update------")
        radarDeviceList[0].info()
        
def updateSystemConfig(payload):
    global mySystemID

    if payload[0] == 0xc7 and payload[7] == 0xc8 and payload[3] == mySystemID :

        radarDeviceList[0].enable = payload[6] # if system config is on/off radar is on/off
        wireXMLRadar(1, payload[6], radarDeviceList[0].distance) # use when update configuration of radar

        systemOb.batteryOff = payload[4]
        systemOb.batteryOn = payload[5]
        systemOb.radarService = payload[6]
        wireXMLSystemEnable(1, payload[5], payload[4]) #wireXMLSystemEnable with battery using mode
        wireXMLSystemRadarServiceEnable(payload[6])

        systemOb.info()
        radarDeviceList[0].inof()

def udpateTimeWorkConfig(payload):
    if payload[0] == 0xc7 and payload[7] == 0xc8 and payload[3] == mySystemID :
        setConfigTimeWork(mySystemID, payload[4], payload[5], payload[6])


def setConfigTimeWork(systemID, timeA, timeB, timeC):

    timeTep = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

    for x in range(0, 8):
        timeTep[x] = (timeA & (0x80 >> x) ) >> ( 7 - x )

    for x in range(0, 8):
        timeTep[x + 8] = (timeB & (0x80 >> x) ) >> ( 7 - x )

    for x in range(0, 8):
        timeTep[x + 16] = (timeC & (0x80 >> x) ) >> ( 7 - x )

    #print(timeTep)
    systemOb.timeWork = timeTep
    wireXMLSystemRadarTimeWork(timeTep)
    systemOb.info()

def requestUpdateConfig():
    global mqttClient

    print("<- send reauest update from server")

    try:
        mqttClient.publish(topic = "requestUpdate", payload =  "Hi", qos=0, retain=False)
    except:
        print("can't publish requestUpdate")
    
############################################
#
#  battery function zone
#
############################################




def readComport():
    sePort = serial.Serial(systemOb.comPort)
    sePort.baudrate = 9600
    time.sleep(2)
    sePort.write(b'B')
    buf = sePort.readline()
    voltLevel =  int(buf.decode("utf-8").replace('\r\n', ''))
    sePort.close()
    voltLevel = round((voltLevel/1023)*100)
    
    if voltLevel > 100 :
        voltLevel = 100

    return voltLevel





############################################
#
#   TCP Command for Control radar sensor
#
############################################

#TCP_IP = '192.168.43.116'  # tcpsocket # test
#TCP_IP = '192.168.2.2'  # tcpsocket
#TCP_PORT = 51716  # tcpsocket
sockTCP = None
tcpConnectFlag = 0

def getTcpConnectFlag():
    global tcpConnectFlag
    return tcpConnectFlag

def setTcpConnectFlag(set):
    global tcpConnectFlag
    tcpConnectFlag = set

def tcpConnect(radarObject):
    global sockTCP, tcpConnectFlag, socket

    sockTCP = None

    try:
        sockTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sockTCP.connect((TCP_IP, TCP_PORT))
        print("connect to ",radarObject.ip, " port ",radarObject.tcpPort)
        sockTCP.connect((radarObject.ip, radarObject.tcpPort))
        print("connected to radar tcp socket client")
        tcpConnectFlag = 1
    except:
        print("OSError : can't connect to tcp socket check your tcp radar ip")
        tcpConnectFlag = 0


def tcpRadarStartCommand():
    global sockTCP, tcpConnectFlag
    radarStartCommand = bytearray([0x01, 0x61, 0x62, 0xff, 0x9e])  # radar start command
    try:
        sockTCP.send(radarStartCommand)
        print("send radar start command")
        #setRadarStatus(1)

    except:
        print("can't send tcp start command to radar")
        setRadarStatus(0)
        tcpConnectFlag = 0

def tcpRadarStopCommand():
    global sockTCP, tcpConnectFlag
    radarStopCommand = bytearray([0x01, 0x62, 0x63, 0xff, 0x9d])    # radar stop command
    try:
        sockTCP.send(radarStopCommand)
        print("send radar stop command")
        setRadarStatus(0)
    except :
        print("can't send radar stop command")
        tcpConnectFlag = 0

def tcpRadarTCPDisconnect():
    global sockTCP, tcpConnectFlag, socket
    try:
        #sockTCP.shutdown(socket.SHUT_RDWR)
        sockTCP.close()
        sockTCP = None
        print("disconnect from radar tcp socket")
        setRadarStatus(0)
    except:
        print("error sockTCP.close()")
        tcpConnectFlag = 0

############################################
#
#   Timer Loop
#   use for sending update system and radar status to main server, and clear traffic object data array buffer
############################################

def time1minLoop(args):
    print("time event task start")
    timeToClearTrafficObectList = 0
    timeToCheckStatusToSendUpdate = 0
    timeToRequestConfig = 0
    while True :
        #do something

        time.sleep(1)
        timeToClearTrafficObectList = timeToClearTrafficObectList + 1
        timeToCheckStatusToSendUpdate = timeToCheckStatusToSendUpdate + 1
        timeToRequestConfig = timeToRequestConfig + 1

        if timeToClearTrafficObectList == 60 :
            timeToClearTrafficObectList = 0
            clearTrafficObectList()
            print("clearTrafficObectList")

        if timeToCheckStatusToSendUpdate == 10 :
            timeToCheckStatusToSendUpdate = 0


            systemOb.batteryNow = readComport()


            """
            if getSystemStatus() == 1:
                print("system on")
            else :
                print("system off")
            """

            

            if systemOb.batteryMode == 1:
                sendSystemStatusAndBatteryStatus() # if system used battery
                print("<- send system and battery" + str(systemOb.batteryNow) + " status to server")
            else:
                sendSystemStatus()
                print("<- send system status to server")

            sendRadarStatus()
            print("<- send radar status to server")

            sendDisplayStatusToServer()
            print("<- send display status to server")

        
        if timeToRequestConfig == 300 :
            #print("request to server")
            timeToRequestConfig = 0
            requestUpdateConfig()  # send request config update from server
        


############################################
#
#   Status function 
#
############################################

def getSystemStatus():
    global systemStatus
    return systemStatus

def setSystemStatus(set):
    global systemStatus
    systemStatus = set

def getRadarStatus():
    global radarStatus
    return  radarStatus

def setRadarStatus(set):
    global radarStatus
    radarStatus = set


############################################
#
#   trafic detection function 
#
############################################

def getMySystemID():
    global mySystemID
    return mySystemID

def newObjectFilter(object):
    global trafficObectList

    if object != 0 and object != None :
        if (object.trackID in trafficObectList) == False :
            print("New object detecting id " + str(object.trackID))
            trafficObectList.append(object.trackID)
            return object

    return 0

def rangOfDetection(object):
    global distanceDetection

    objectDistance = math.sqrt( math.pow(object.xPosition, 2) + math.pow(object.yPosition, 2) + math.pow(object.zPosition, 2))
    if objectDistance <= distanceDetection :
        #print("objectDistance : " + str(objectDistance) + " speed " , object.carSpeed)
        return object

    return 0

def yourSpeedProcess(object): # main process

    global mqttClient
    
    nowtime = time.localtime(time.time())


    if getSystemStatus() != 0 : # check radar service on/off config
        if systemOb.timeWork[nowtime[3] - 1] != 0 and systemOb.timeWorkEnable == 1: #check system worktime enable mode and worktime 
            if rangOfDetection(object) != 0 :
                carObject = newObjectFilter(object)

                if carObject != 0 :
                    #send mqtt publish
                    print("object comming : lane : " + str(carObject.lane) + " speed " , str(carObject.carSpeed))
                    mqttPublishTrafficData(mqttClient, carObject.carClass, carObject.carSpeed, carObject.lane, carObject.direction)
        else:
            print("non-work time")
    else:
        print("radar service status is off")

def clearTrafficObectList():
    global trafficObectList
    trafficObectList = [None]

def sendRadarStatus():

    toSentBuffer = bytearray(6)
    toSentBuffer[0] = 0xc7
    toSentBuffer[1] = 0x04
    toSentBuffer[2] = 0x21
    toSentBuffer[3] = getMySystemID() & 0xff
    toSentBuffer[4] = getRadarStatus() & 0xff
    toSentBuffer[5] = 0xc8

    print("radarStatus " + str(getRadarStatus()))

    try:
        mqttClient.publish(topic="updateStatusRadar", payload=toSentBuffer, qos=0, retain=False)
    except:
        print("mqtt : can't publish to mqtt broker")

def sendSystemStatus():

    toSentBuffer = bytearray(7)
    toSentBuffer[0] = 0xc7
    toSentBuffer[1] = 0x05
    toSentBuffer[2] = 0x20
    toSentBuffer[3] = getMySystemID() & 0xff
    toSentBuffer[4] = getSystemStatus() & 0xff
    toSentBuffer[5] = 0x00
    toSentBuffer[6] = 0xc8

    print("systemStatus " + str(getSystemStatus()))

    try:
        mqttClient.publish(topic="updateStatusSystem", payload=toSentBuffer, qos=0, retain=False)
    except:
        print("mqtt : can't publish to mqtt broker")

def sendSystemStatusAndBatteryStatus():

    toSentBuffer = bytearray(7)
    toSentBuffer[0] = 0xc7
    toSentBuffer[1] = 0x05
    toSentBuffer[2] = 0x20
    toSentBuffer[3] = getMySystemID() & 0xff
    toSentBuffer[4] = getSystemStatus() & 0xff
    toSentBuffer[5] = systemOb.batteryNow & 0xff  # battery status
    toSentBuffer[6] = 0xc8

    print("systemStatus and batteryStatus" + str(getSystemStatus()))

    try:
        mqttClient.publish(topic="updateStatusSystem", payload=toSentBuffer, qos=0, retain=False)
    except:
        print("mqtt : can't publish to mqtt broker")


############################################
#
#   udp radar loop function 
#   interface ip on this function use real hardware ip of your computer
############################################

#interface_ip = "192.168.2.100"  # ip eth0 ip
interface_ip = systemOb.localIP
#interface_ip = "localhost"  # ip for test
#interface_port = 9931   #udp radar paket port sending

sockUDP = None
updConnectRadarFlag = 0

def udpRadarConnect():
    global sockUDP, interface_ip, interface_port

    print("udp : connect to listening radar packet ...")

    try:
        sockUDP = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)  # UDP
        sockUDP.bind((interface_ip, interface_port))

        print("udp : connected to radar udp client")
        setUpdConnectRadarFlag(1)

    except OSError:
        print("udp : OSError connected to radar udp client Check your ip address")
        setUpdConnectRadarFlag(0)

def getUpdConnectRadarFlag():
    global updConnectRadarFlag
    return updConnectRadarFlag

def setUpdConnectRadarFlag(set):
    global updConnectRadarFlag
    updConnectRadarFlag = set

def udpLoop(flag):

    global sockUDP

    print("udp : radar interface Loop start")

    udpReConnectCounter = 0
    udpRadarConnect()

    while True :

        try:

            if getUpdConnectRadarFlag() :
                udpReConnectCounter = 0
                if systemOb.radarService : # if radar service on
                    data, addr = sockUDP.recvfrom(1024)  # buffer size is 1024 bytes
                    #deStatusPacket(data)
                    deStatisticPacket(data)
                    hasObject = deTrackPacket(data)
                    if hasObject != 0 and hasObject != None :
                        #ob = trafficObect()
                        #return 1, sensorID, trackID, xPosition, yPosition, zPosition, carSpeed, lane, carClass, direction
                        #ob = hasObject
                        setRadarStatus(1)
                        yourSpeedProcess(hasObject)
                        #ob.info()
            else :
                time.sleep(1)
                udpReConnectCounter += 1
                if udpReConnectCounter > 100 :
                    print("udp : re connect to listening radar packet")
                    sockUDP = None
                    udpRadarConnect()

        except OSError:
            print("udp : OSError : check your ip address or network interface")
            #print("Program Closed")
            print("udp : program wait 5 s to return to listening radar ip")
            time.sleep(5000)


############################################
#
#   de-Protocol Packet
#
############################################

class trafficObect():

    radarID = 0
    trackID = 0
    xPosition = 0.0
    yPosition = 0.0
    zPosition = 0.0
    carSpeed = 0
    lane = 0
    carClass = 0
    direction = 0

    def info(self) :
        print("radarID : " + str(self.radarID))
        print("trackID : " + str(self.trackID))
        print("xPo : " + str(self.xPosition))
        print("yPo : " + str(self.yPosition))
        print("zPo : " + str(self.zPosition))
        print("car speed : " + str(self.carSpeed))
        print("lane : " + str(self.lane))
        print("class : " + str(self.carClass))
        print("diretion : " + str(self.direction))
        
def deTrackPacket(payload):
    global speedOfset

    packetTrackTypeHeader = [0x01, 0x21]  # header 0
    packetTrackTypeEnd = [0xFF, 0xDE]  # end 0
    crcSum = 0
    lastByte = 0
    packetHeaderFlag = False

    if len(payload) < 34 : # track packet size is 34 bytes
        return 0

    for index in range(0, len(payload), 1):
        if index == 0:
            packetHeaderFlag = False

        if payload[index] == packetTrackTypeHeader[0] and payload[index + 1] == packetTrackTypeHeader[1]:
            # if payload[index] == packetTrackTypeHeader[0] :
            # print ("this is track packet type header")
            if packetHeaderFlag == False:
                packetHeaderFlag = True

        elif payload[index] == packetTrackTypeEnd[0] and payload[index + 1] == packetTrackTypeEnd[1]:
            # print ("this is track packet type end")
            # print ("lastByte :" + str(lastByte))
            # print ("check sum : " + str(crcSum & 0xFF))
            if lastByte == (crcSum & 0xFF) and packetHeaderFlag == True:
                # print "packet corrected checksum cal is : " + hex(crcSum & ord(b'\xFF')) + " checksum rev : " + hex(ord(lastByte))
                #print ("Track packet")

                '''
                sensorID, = struct.unpack('H', payload[2:4])
                trackID, = struct.unpack('I', payload[4:8])
                xPosition, = struct.unpack('f', payload[8:12])
                yPosition, = struct.unpack('f', payload[12:16])
                zPosition, = struct.unpack('f', payload[16:20])
                carSpeed, = struct.unpack('f', payload[20:24])
                lane = payload[29]
                carClass = payload[30]
                '''

                #print("sensorID : " + str(sensorID))
                #print("trackID : " + str(trackID))
                #print("xPo : " + str(xPosition))
                #print("yPo : " + str(yPosition))
                #print("zPo : " + str(zPosition))
                #print("car speed : " + str(carSpeed))
                #print("lane : " + str(lane))
                #print("class : " + str(carClass))
                #print()

                ob = trafficObect()

                carSpeed, = struct.unpack('f', payload[20:24])

                if carSpeed < 0:
                    direction = 1  # leaving
                else:
                    direction = 0  # approaching

                carSpeed = abs(carSpeed)

                ob.radarID, = struct.unpack('H', payload[2:4])
                ob.trackID, = struct.unpack('I', payload[4:8])
                ob.xPosition, = struct.unpack('f', payload[8:12])
                ob.yPosition, = struct.unpack('f', payload[12:16])
                ob.zPosition, = struct.unpack('f', payload[16:20])
                ob.carSpeed = int(round(carSpeed)) + speedOfset
                ob.lane = payload[29]
                ob.carClass = payload[30]
                ob.direction = direction

                return ob
                break

        if index != 31:
            crcSum += payload[index]

        lastByte = payload[index]

    return 0

def deStatusPacket(payload):
    packetTrackTypeHeader = [0x01, 0x11]  # header 0
    packetTrackTypeEnd = [0xFF, 0xEE]  # end 0
    crcSum = 0
    lastByte = 0
    packetHeaderFlag = False

    #print(type(payload))

    if len(payload) < 16 : # track packet size is 16 bytes
        return 0

    # print(payload[0] == ord(packetTrackTypeHeader[0]))
    for index in range(0, len(payload), 1):
        if index == 0:
            packetHeaderFlag = False

        if payload[index] == packetTrackTypeHeader[0] and payload[index + 1] == packetTrackTypeHeader[1]:
            #if payload[index] == packetTrackTypeHeader[0] :
            #print ("this is status packet type header")
            if packetHeaderFlag == False:
                packetHeaderFlag = True

        elif payload[index] == packetTrackTypeEnd[0] and payload[index + 1] == packetTrackTypeEnd[1]:
            # print ("this is status packet type end")
            # print ("lastByte :" + str(lastByte))
            # print ("check sum : " + str(crcSum & 0xFF))
            if lastByte == (crcSum & 0xFF) and packetHeaderFlag == True:
                #print "packet corrected checksum cal is : " + hex(crcSum & ord(b'\xFF')) + " checksum rev : " + hex(ord(lastByte))
                #print ("Hello Status packet")

                break

        if index != 13:
            crcSum += payload[index]

        lastByte = payload[index]

def deStatisticPacket(payload):

    global mqttClient, myRadarID, aggregateLossCount, mySystemID

    #StatisticApphoaching
    packetStatisticApphoachingHeader = [0x01, 0x51]  # header 0
    packetStatisticApphoachingEnd = [0xFF, 0xAE]  # end 0

    #StatisticLeaving
    packetStatisticLeavingHeader = [0x01, 0x52]  # header 0
    packetStatisticLeavingEnd = [0xFF, 0xAD]  # end 0

    crcSum = 0
    lastByte = 0
    packetHeaderFlag = False

    #print(type(payload))

    if len(payload) < 48 : # track packet size is 16 bytes
        return 0

    # print(payload[0] == ord(packetTrackTypeHeader[0]))
    for index in range(0, len(payload), 1):
        if index == 0:
            packetHeaderFlag = False

        if payload[index] == packetStatisticApphoachingHeader[0] and (payload[index + 1] == packetStatisticApphoachingHeader[1] or payload[index + 1] == packetStatisticLeavingHeader[1]) :
            #print ("this is statistic packet type header")
            if packetHeaderFlag == False:
                packetHeaderFlag = True

        elif payload[index] == packetStatisticApphoachingEnd[0] and (payload[index + 1] == packetStatisticApphoachingEnd[1] or payload[index + 1] == packetStatisticLeavingEnd[1]):
            #print ("this is statistic packet type end")
            # print ("lastByte :" + str(lastByte))
            # print ("check sum : " + str(crcSum & 0xFF))
            if lastByte == (crcSum & 0xFF) and packetHeaderFlag == True:
                #print "packet corrected checksum cal is : " + hex(crcSum & ord(b'\xFF')) + " checksum rev : " + hex(ord(lastByte))
                print ("Hello Statistic packet")

                radarID, = struct.unpack('H', payload[2:4])
                laneID = payload[4]
                laneOCC, = struct.unpack('f', payload[5:9])
                avgSpeed, = struct.unpack('f', payload[9:13])
                headWay, = struct.unpack('I', payload[13:17])
                vehicleCount, = struct.unpack('I', payload[17:21])
                class1Count, = struct.unpack('I', payload[21:25])
                class2Count, = struct.unpack('I', payload[25:29])
                class3Count, = struct.unpack('I', payload[29:33])
                class4Count, = struct.unpack('I', payload[33:37])

                '''
                print("radarID " + str(radarID))
                print("laneID " + str(laneID))
                print("laneOCC " + str(laneOCC))
                print("avgSpeed " + str(avgSpeed))
                print("headWay " + str(headWay))
                print("vehicelCount " + str(vehicleCount))
                print("class 1 " + str(class1Count))
                print("class 2 " + str(class2Count))
                print("class 3 " + str(class3Count))
                print("class 4 " + str(class4Count))
                '''
                toSentBuffer = bytearray()
                toSentBuffer.append(0xc7) # header
                toSentBuffer.append(37) # size 37
                toSentBuffer.append(0x02)  # protocol client type
                toSentBuffer.append(mySystemID & 0xff)  # myradar id
                toSentBuffer.append(0x00)  # myradar id
                for x in payload[4:37]:
                    toSentBuffer.append(x)
                toSentBuffer.append(0xc8)
                #print(toSentBuffer)

                aggregateLossCount = 0 # if the radar dont sent aggregate we will count it
                setRadarStatus(1) # set radar status
                mqttPubAggregate(mqttClient, toSentBuffer)
                print("sent aggregate data")

                break

        if index != 45:
            crcSum += payload[index]

        lastByte = payload[index]

def findNewTrafficObject(tfobject):
    global trafficObectList

    if (tfobject.radarID in trafficObectList) == False :
        trafficObectList.append(tfobject.radarID)
        tfobject.inof()


############################################
#
#   USB Relay Control
#
############################################

counterForRelayRadarReset = 0

def relayRadarOpen():
    global systemOb
    usb_serial_number = systemOb.relayUSBSerialNumber
    try:
        print("Relay : opening the relay radar")
        subprocess.call(["CommandApp_USBRelay.exe", usb_serial_number, "open", "01"])
    except :
        print("Relay : error : closing the radar USB Realay dont online")

def relayRadarClose():
    global systemOb
    usb_serial_number = systemOb.relayUSBSerialNumber
    try:
        print("Relay : closing the ralay radar")
        subprocess.call(["CommandApp_USBRelay.exe", usb_serial_number, "close", "01"])
    except :
        print("Relay : error : closing the radar USB Realay dont online")

def relayRadarReset():
    relayRadarOpen()
    print("Relay : Relay reset : wait 10 s")
    time.sleep(10) #
    relayRadarClose()
    print("Relay : wait 30 s for radar boosting")
    time.sleep(30) #

def countForRelayRadarReset():
    global counterForRelayRadarReset
    if counterForRelayRadarReset < 2 :
        print("Relay : counter For Relay Radar Reset %s/2" % (counterForRelayRadarReset))
        counterForRelayRadarReset += 1

    else:
        counterForRelayRadarReset = 0
        relayRadarReset()


############################################
#
#   global zone
#
############################################

trafficObectList = [None]

radarServiceON_OFF = 1
radarStatus = 0 #  radar online / offline
systemStatus = 1 # system online / offline
mySystemID = systemOb.id

distanceDetection = radarDeviceList[0].distance
myRadarID = radarDeviceList[0].radarID
speedOfset = radarDeviceList[0].speedOffset

interface_port = radarDeviceList[0].udpPort

aggregateLossCount = 0

############################################
#
#   Main Function
#
############################################

def main():
    global radarDeviceList, aggregateLossCount

    relayRadarClose() #

    mqttReConnectDelay = 20 # 20s
    mqttCount = 0

    tcpReConnectCount = 0
    tcpReConnectDealy = 30 # 30s
    tcpReConnectRound = 0

    mqttConnect()
    print("mqtt : wait 5s to connect to mqtt brocker")
    time.sleep(5)

    requestUpdateConfig()
   
    time.sleep(5)
    updateDisplayConfigLocal() # send display config to display service

    # Create UDP threads as follows
    try:
        _thread.start_new_thread(udpLoop, (1,)) # udp loop 
        time.sleep(5)
    except:
        print("udp : Error : unable to start udpLoop")
    

    # Create Timer threads as follows
    try:
        _thread.start_new_thread(time1minLoop, (1,)) # time 1 min loop
        time.sleep(5)
    except:
        print("time 1 m : Error: unable to start time1minLoop")

    while True:

        if getMqttConnectFlag() != 1:
            if mqttCount < mqttReConnectDelay :
                mqttCount += 1
            else:
                mqttCount = 0
                print("mqtt : try to reconnect to mqtt borker")
                mqttConnect()

        # Create tcpConnect to radar
        if  getTcpConnectFlag() != 1 : 
            if getMqttConnectFlag() == 1 :   
                if tcpReConnectCount == 0:
                    print("tcp : init tcp connection")
                    tcpRadarStopCommand()
                    time.sleep(5)
                    tcpRadarTCPDisconnect()
                    time.sleep(5)
                    tcpConnect(radarDeviceList[0])
                    time.sleep(5)
                    tcpRadarStartCommand()
                    tcpReConnectCount = tcpReConnectDealy
                    #countForRelayRadarReset() # reset relay of radar
                else:
                    tcpReConnectCount -= 1
                    print("tcp : tcpReConnectCount : " + str(tcpReConnectCount)) 
            else:
                print("tcp : Do not start tcp radar connection , wait mqtt reconnect to brocker")
        else:

            if aggregateLossCount > 300: #when the radar loss long time (300s)
                    setTcpConnectFlag(0)
                    tcpReConnectCount = tcpReConnectDealy
            else:
                aggregateLossCount += 1
                print("radar : aggregateLossCount ", aggregateLossCount, " /300")

        time.sleep(1)
        
main() # program start here
print("end of program")