import datetime
import time
import mysql.connector
import paho.mqtt.client as mqtt


############################################
###        global zone                  ###
###########################################
mydb = None
mycursor = None
mysqlConnectFlag = 0
mysqlReconntDelay = 10 # 10s


mqttClient = None
mqttBroker = "localhost"
mqttBrokerPort = 5000
mqttReConnectDelay = 10 # 10s

#for pubilsher
mqttTopicUpdateDisplay = 'updateDisplay/'
mqttTopicUpdateRadar = 'updateRadar/'
mqttTopicUpdateSystem = 'updateSystem/'
mqttTopicUpdateTime = 'updateTime/'

#for subcriber
mqttTopicRequestUpdate = "requestUpdate"

mqttConnectFlag = 0

previousUpdateDisplay = None
previousUpdateRadar = None
previousUpdateSystem = None
previousUpdateTime = None


'''
select update_time from information_schema.tables
where table_schema = 'solution_y' and table_name = 'view_aggregate'
'''

############################################
###        body zone                    ###
###########################################

############################################
###        db function  zone            ###
###########################################
def mySqlConnect():
    global mysql, mydb, mycursor, mysqlConnectFlag

    try:
        # for test
        '''
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="admin",
            database="solution_y"
        )
        '''
        # for Real
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="ry<okFxicdi,",
            database="solution_y"
        )


        mycursor = mydb.cursor()
        mysqlConnectFlag = 1
        print("mysql DB connected ")
    except :
        print("can't connect ot mysql DB")
        mysqlConnectFlag = 0

def getMySqlConnectFlag():
    global mysqlConnectFlag
    return mysqlConnectFlag

def setMySqlConnectFlag(set):
    global mysqlConnectFlag
    mysqlConnectFlag = set

def checkDBconfigUpdate(tableName):
    global mycursor, mydb

    sql = "select update_time from information_schema.tables \
            where table_name = '" + tableName + "'"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    mydb.commit()
    if myresult != None:
        #print( tableName + " check update")
        return  myresult[0][0]
    else:
        #print("there isn't " + tableName)
        return 0

def getConfigDisplay(displayID): #

    sql = "select * from config_display where display_id =" + str(displayID)
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    if len(myresult) != 0 :
        #print(myresult[0])
        return myresult[0] # displayID, displayTime, brink, warning, over, on/off
    else:
        return 0

def getConfigRadar(radarID):

    sql = "select * from config_radar where radar_id =" + str(radarID)
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    if len(myresult) != 0 :
        #print(myresult[0])
        return myresult[0] # radarID, on/off, rangDetection
    else:
        return 0

def getConfigSystem(systemID):

    sql = "select * from config_system where system_id =" + str(systemID)
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    if len(myresult) != 0:
        #print(myresult[0])
        return myresult[0]  # systemID, batteryON, batteryOFF, systemON/OFF
    else:
        return 0

def getConfigTime(systemID):

    sql = "select * from config_time where system_id =" + str(systemID)
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    if len(myresult) != 0:
        #print(myresult[0][15])

        timeA = 0x00
        timeB = 0x00
        timeC = 0x00

        for x in range(1,9) :
            #print(myresult[0][x])
            if myresult[0][x] == 1:
                timeA = timeA  | (0x80 >> (x - 1))

        for x in range(9,17) :
            #print(myresult[0][x])
            if myresult[0][x] == 1:
                timeB = timeB  | (0x80 >> (x - 9))

        for x in range(17,25) :
            #print(myresult[0][x])
            if myresult[0][x] == 1:
                timeC = timeC  | (0x80 >> (x - 17))


        return myresult[0][0], timeA, timeB, timeC  # systemID, timeA, timeB, timeC
    else:
        return 0

###########################################
###        mqtt function  zone          ###
###########################################

def mqttConnect():
    global mqttClient, mqttBroker, mqttBrokerPort, mqttConnectFlag

    #mqttClient = None
    mqttClient = mqtt.Client()
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.on_disconnect = on_disconnect
    mqttClient.on_socket_close = on_socket_close

    try:
        mqttClient.connect(host = mqttBroker, port = mqttBrokerPort, keepalive = 60)
        mqttStartLoop()
    except:
        print("can't connect to mqtt broker")
        mqttConnectFlag = 0

def mqttDisconnect():
    global mqttClient
    try :
        mqttClient.loop_stop(force=True)
    except:
        print("mqtt can't stop loop")

    try :
        mqttClient.disconnect()
    except:
        print("mqtt can't disconnect")

def on_connect(client, userdata, flags, rc):
    global mqttConnectFlag, mqttTopicRequestUpdate
    print("Connected with result code " + str(rc))
    if rc == 0:
        client.subscribe(mqttTopicRequestUpdate, qos=0)
        client.subscribe("test", qos=0)
        mqttConnectFlag = 1
        requestUpdate()
    else:
        mqttConnectFlag = 0

def on_message(client, userdata, msg):
    global mqttTopicRequestUpdate, checkAliveChouter
    print(msg.topic + " : " + str(msg.payload))
    if msg.topic == mqttTopicRequestUpdate:
        checkAliveChouter = 0
        requestUpdate()

def on_disconnect(client, userdata, rc):
    global mqttConnectFlag
    print("mqtt client disconnect")
    mqttConnectFlag = 0

def on_socket_close(client, rc):
    global mqttConnectFlag
    print("mqtt client socket close")
    mqttConnectFlag = 0

def mqttStartLoop():
    global mqttClient
    mqttClient.loop_start()

def getMqttConnectionFlag():
    global mqttConnectFlag
    return mqttConnectFlag

def setMqttConnectionFlag(set):
    global mqttConnectFlag
    mqttConnectFlag = set

def mqttSendPublichDisplayConfig(msg):
    global mqttClient, mqttTopicUpdateDisplay, mqttConnectFlag

    buf = bytearray()
    buf.append(0xc7) #header
    buf.append(0x09)
    buf.append(0x11)
    buf.append(msg[0] & 0xFF) # displayID
    buf.append(msg[5] & 0xFF) # on/off
    buf.append(msg[2] & 0xFF)  # blink
    buf.append((msg[1] >> 8) & 0xFF)  # display_timeA
    buf.append(msg[1] & 0xFF)  # display_timeB
    buf.append(msg[3] & 0xFF)  # worning_speed
    buf.append(msg[4] & 0xFF)  # over_speed
    buf.append(0xc8) # end
    try:
        mqttClient.publish(topic=mqttTopicUpdateDisplay + str(msg[0]), payload=buf, qos=0, retain=False)
        print("send mqttSendPublichDisplayConfig " + str(msg[0]))
    except:
        print("can't send mqttSendPublichDisplayConfig")
        mqttConnectFlag = 0

def mqttSendPublichRadarConfig(msg):
    global mqttClient, mqttTopicUpdateRadar, mqttConnectFlag

    buf = bytearray()
    buf.append(0xc7) #header
    buf.append(0x05)
    buf.append(0x10)
    buf.append(msg[0] & 0xFF) # radarID
    buf.append(msg[1] & 0xFF) # on/off
    buf.append(msg[2] & 0xFF)  # distance
    buf.append(0xc8) # end
    try:
        mqttClient.publish(topic=mqttTopicUpdateRadar + str(msg[0]), payload=buf, qos=0, retain=False)
        print("send mqttSendPublichRadarConfig " + str(msg[0]))
    except:
        print("can't send mqttSendPublichRadarConfig")
        mqttConnectFlag = 0

def mqttSendPublichSystemConfig(msg):
    global mqttClient, mqttTopicUpdateSystem, mqttConnectFlag

    buf = bytearray()
    buf.append(0xc7) #header
    buf.append(0x06)
    buf.append(0x12)
    buf.append(msg[0] & 0xFF) # systemID
    buf.append(msg[1] & 0xFF) # bat_on
    buf.append(msg[2] & 0xFF)  # bat_off
    buf.append(msg[3] & 0xFF)  # on/off
    buf.append(0xc8) # end
    try:
        mqttClient.publish(topic = mqttTopicUpdateSystem + str(msg[0]), payload=buf, qos=0, retain=False)
        print("send mqttSendPublichSystemConfig " + str(msg[0]))
    except:
        print("can't send mqttSendPublichSystemConfig")
        mqttConnectFlag = 0

def mqttSendPublichTimeConfig(systemID, timeA, timeB, timeC):
    global mqttClient, mqttTopicUpdateTime, mqttConnectFlag

    buf = bytearray()
    buf.append(0xc7) #header
    buf.append(0x06)
    buf.append(0x13)
    buf.append(systemID & 0xFF) # systemID
    buf.append(timeA & 0xFF) # timeA
    buf.append(timeB & 0xFF)  # timeB
    buf.append(timeC & 0xFF)  # timeC
    buf.append(0xc8) # end
    try:
        mqttClient.publish(topic = mqttTopicUpdateTime + str(systemID), payload=buf, qos=0, retain=False)
        print("send mqttSendPublichTimeConfig " + str(systemID))
    except:
        print("can't send mqttSendPublichTimeConfig")
        mqttConnectFlag = 0

def requestUpdate():
    global previousUpdateDisplay,previousUpdateRadar, previousUpdateTime, previousUpdateSystem
    previousUpdateDisplay = None
    previousUpdateRadar = None
    previousUpdateSystem = None
    previousUpdateTime = None

###########################################
###        main function  zone          ###
###########################################
checkAliveChouter = 0

def main():

    global previousUpdateSystem, previousUpdateTime, previousUpdateDisplay, previousUpdateRadar, checkAliveChouter


    print("UpdateService Start")

    mySqlCounter = 0
    mqttCounter = 0
    timeToSend = 60*5 # 5 min
    timeCounter = 0

    while True :

        if getMySqlConnectFlag() != 1:

            if mySqlCounter < mysqlReconntDelay :
                mySqlCounter = mySqlCounter + 1
            else :
                mySqlCounter = 0
                print("try to connect to mysql DB")
                mySqlConnect()
                requestUpdate()

        if getMySqlConnectFlag() == 1 and getMqttConnectionFlag() != 1 :
            if mqttCounter  < mqttReConnectDelay :
                mqttCounter = mqttCounter + 1
            else  :
                mqttCounter = 0
                print("try to connect mqtt broker")
                mqttDisconnect()
                mqttConnect()
                requestUpdate() # force to send update all config when mqtt is reconnect

        timeCounter = timeCounter + 1
        if timeCounter > timeToSend :
            timeCounter = 0
            requestUpdate()
            print("send update by 5 min")



        if getMySqlConnectFlag() == 1 and getMqttConnectionFlag() == 1:

            try :
                re = checkDBconfigUpdate("config_system")
                if re != previousUpdateSystem :
                    print(re)
                    previousUpdateSystem = re
                    msg = getConfigSystem(1)
                    print(msg)
                    mqttSendPublichSystemConfig(msg)
                    msg = getConfigSystem(2)
                    print(msg)
                    mqttSendPublichSystemConfig(msg)
                    msg = getConfigSystem(3)
                    print(msg)
                    mqttSendPublichSystemConfig(msg)
                    #print("send config system")

            except :
                print("something error to get config_system")

            try:

                re = checkDBconfigUpdate("config_radar")
                if re != previousUpdateRadar:
                    print(re)
                    previousUpdateRadar = re
                    msg = getConfigRadar(1)
                    print(msg)
                    mqttSendPublichRadarConfig(msg)
                    msg = getConfigRadar(2)
                    print(msg)
                    mqttSendPublichRadarConfig(msg)
                    msg = getConfigRadar(3)
                    print(msg)
                    mqttSendPublichRadarConfig(msg)
                    #print("send config radar")
            except :
                print("something error to get config_radar")

            try:
                re = 0
                re = checkDBconfigUpdate("config_display")
                if re != previousUpdateDisplay :
                    print(re)
                    previousUpdateDisplay = re
                    msg = getConfigDisplay(1)
                    print(msg)
                    mqttSendPublichDisplayConfig(msg)
                    msg = getConfigDisplay(2)
                    print(msg)
                    mqttSendPublichDisplayConfig(msg)
                    msg = getConfigDisplay(3)
                    print(msg)
                    mqttSendPublichDisplayConfig(msg)
                    #print("send config display")
            except :
                print("something error get config_display")

            try:
                re = 0
                re = checkDBconfigUpdate("config_time")
                if re != previousUpdateTime :
                    print(re)
                    previousUpdateTime = re
                    msg = getConfigTime(1)
                    print(msg)
                    mqttSendPublichTimeConfig(msg[0], msg[1], msg[2], msg[3])
                    msg = getConfigTime(2)
                    print(msg)
                    mqttSendPublichTimeConfig(msg[0], msg[1], msg[2], msg[3])
                    msg = getConfigTime(3)
                    print(msg)
                    mqttSendPublichTimeConfig(msg[0], msg[1], msg[2], msg[3])
                    #print("send config time")
            except :
                print("something error to get config_time")

        checkAliveChouter += 1
        if checkAliveChouter > 300 :
            print("checkAliveCounter reset")
            checkAliveChouter = 0
            setMySqlConnectFlag(0)
            setMqttConnectionFlag(0)
        else :
            print("checkAliveCouter : " + str(checkAliveChouter))

        time.sleep(1)

main()
