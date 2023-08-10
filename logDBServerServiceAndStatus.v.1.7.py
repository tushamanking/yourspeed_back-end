import mysql.connector
import paho.mqtt.client as mqtt
import struct
import time


################################
#   global zone                #
################################

# for mysql
mycursor = None
mydb = None
mysqlReconnectDelay = 10
mysqlConnectFlag = 0


#for mqtt subcriber
client = None
mqttTopicUpdateDisplaySub = 'updateStatusDisplay'
mqttTopicUpdateRadarSub = 'updateStatusRadar'
mqttTopicUpdateSystemSub = 'updateStatusSystem'
mqttIP = "localhost"
mqttPort = 5000
mqttConnectFlag = 0
mqttReconnectDelay = 10


def mySqlConnect():
    global mydb, mycursor, mysqlConnectFlag
    try:

        #for real
        mydb = None
        mycursor = None
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="ry<okFxicdi,",
            database="solution_y"
        )

    
        # for test
        '''
        
            mydb = mysql.connector.connect(
                host = "localhost",
                user = "root",
                passwd = "admin",
                database = "solution_y"
            )
        '''
        mycursor = mydb.cursor()
    except :
        print("------------------------------- error on connect db")

def mySqlClose():
    global mydb
    try:
        mydb.close()
    except:
        print("------------------------------- error mydb.close()")

'''
sql = "INSERT INTO test (id, name) VALUES (%s, %s)"
val = (2, "radar2")
mycursor.execute(sql, val)
mydb.commit()
'''

'''
INSERT INTO `view_report` (`ai_num`, `radar_id`, `direction`, `date`, `time`, `speed`, `lane`)
  VALUES (NULL, '1', '0', '2019-06-10', '14:14:14', '180', '4');
'''

'''
INSERT INTO `solution_y`.`view_aggregate` (`ai_num`, `radar_id`, `lane_id`, `lane_occ`, `avg_speed`,
`direction`, `v_count`, `c1_count`, `c2_count`, `c3_count`, `c4_count`)
VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''

################################
#   Data base function zone    #
################################
def insertTrafficDataToDB(radarID, speed, lane, direction):
    global mydb, mycursor

    sql = "INSERT INTO view_report (ai_num, radar_id, direction, date, time, speed, lane) \
           VALUES (NULL, %s, %s, %s, %s, %s, %s)"

    nowtime = time.localtime(time.time())
    date_s = str(nowtime[0]) + "-" + str(nowtime[1]) + "-" + str(nowtime[2])
    time_s = str(nowtime[3]) + ":" + str(nowtime[4]) + ":" + str(nowtime[5])

    val = (radarID, direction, date_s, time_s, speed, lane)
    try:
        mySqlConnect()
        mycursor.execute(sql, val)
        mydb.commit()
        mySqlClose()
    except:
        print("------------------------------- error insertTrafficDataToDB()")

def insertAggregateDataToDB(radarID, laneID, laneOCC, avgSpeed, headWay, vehicleCount, class1Count, class2Count, class3Count, class4Count):
    global mydb, mycursor

    sql = "INSERT INTO `solution_y`.`view_aggregate` (`ai_num`, `radar_id`, `lane_id`, `lane_occ`, `avg_speed`, \
            `direction`, `v_count`, `c1_count`, `c2_count`, `c3_count`, `c4_count`) \
            VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    val = (radarID, laneID, laneOCC, avgSpeed, headWay, vehicleCount, class1Count, class2Count, class3Count, class4Count)
    try:
        mySqlConnect()
        mycursor.execute(sql, val)
        mydb.commit()
        mySqlClose()
    except:
        print("------------------------------- can't insert to aggregate data")

def showDataInViewReport():
    mySqlConnect()
    mycursor.execute("SELECT * FROM view_report")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

    mySqlClose()

def showDataInAggregateReport():
    mySqlConnect()
    mycursor.execute("SELECT * FROM view_aggregate")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

    mySqlClose()

def dbUpdateStatusDisplay(displayID, status):
    global mydb, mycursor
    sql = "UPDATE system_status SET display_status = %s WHERE system_id = %s"
    val = (status, displayID)# use displayID refer to systemID
    try :
        mySqlConnect()
        mycursor.execute(sql, val)
        mydb.commit()
        mySqlClose()
    except:
        print("------------------------------- error dbUpdateStatusDisplay ()")

def dbUpdateStatusRadar(radarID, status):
    global mydb, mycursor
    sql = "UPDATE system_status SET radar_status = %s WHERE system_id = %s"
    val = (status, radarID)# use radarID refer to systemID
    try:
        mySqlConnect()
        mycursor.execute(sql, val)
        mydb.commit()
        mySqlClose()
    except:
        print("------------------------------- error dbUpdateStatusRadar()")

def dbUpdateStatusSystem(systemID, status, batLevel):
    global mydb, mycursor
    sql = "UPDATE system_status SET system_status = %s, battery_level = %s WHERE system_id = %s;"
    val = (status, batLevel, systemID) # use radarID refer to systemID
    try:
        mySqlConnect()
        mycursor.execute(sql, val)
        mydb.commit()
        mySqlClose()
    except:
        print("------------------------------- error dbUpdateStatusSystem")


################################
#  check status function zone #
################################
radarCountFlag = bytearray(3)
displayCountFlag = bytearray(3)
systemCountFlag = bytearray(3)
radarCount = bytearray(3)
displayCount = bytearray(3)
systemCount = bytearray(3)
statusTimeOut = 300


def getStatusRadarCount(id):
    global radarCount
    return  radarCount[id]

def getStatusDisplayCount(id):
    global displayCount
    return displayCount[id]

def getStatusSystemCount(id):
    global systemCount
    return systemCount[id]

def increteRadarCount(id):
    global radarCount
    radarCount[id] += 1

def increteDisplayCount(id):
    global displayCount
    displayCount[id] += 1

def increteSystemCount(id):
    global systemCount
    systemCount[id] += 1

def resetRadarCount(id):
    global radarCount, radarCountFlag
    radarCount[id] = 0
    radarCountFlag[id] = 0

def resetDisplayCount(id):
    global displayCount,  displayCountFlag
    displayCount[id] = 0
    displayCountFlag[id] = 0

def resetStatusSystemCount(id):
    global systemCount, systemCountFlag
    systemCount[id] = 0
    systemCountFlag[id] = 0

def checkCountStatus():
    global radarCount, displayCount, systemCount, radarCountFlag, displayCountFlag, systemCountFlag, statusTimeOut

    for index in range(0,3):
        if getStatusSystemCount(index) < statusTimeOut :
            increteSystemCount(index)
            print("StatusSystemCount %s:%s" % (index + 1, getStatusSystemCount(index)))
        else :
            if systemCountFlag[index] != 1 :
                systemCountFlag[index] = 1
                dbUpdateStatusSystem(int(index + 1), 0, 0) #
                print("StatusSystem %s offline" % (index + 1))

        if getStatusRadarCount(index) < statusTimeOut :
            increteRadarCount(index)
            print("StatusRadarCount %s:%s" % (index + 1, getStatusRadarCount(index)))
        else :
            if radarCountFlag[index] != 1 :
                radarCountFlag[index] = 1
                dbUpdateStatusRadar(int(index + 1), 0)
                print("StatusRadar % offline"% (index + 1))

        if getStatusDisplayCount(index) < statusTimeOut :
            increteDisplayCount(index)
            print("StatusDisplayCount %s:%s" % (index + 1, getStatusDisplayCount(index)))
        else :
            if displayCountFlag[index] != 1 :
                displayCountFlag[index] = 1
                dbUpdateStatusDisplay(int(index + 1), 0)
                print("StatusDisplay %s offline" % (index + 1))

################################
#   de-packet function zone    #
################################

def deProtocol(payload):
  packetHeader = False

  if payload[0] == 0xc7 :
    packetHeader = True

  if packetHeader :
      packetSize = payload[1]
      #print(str(len(payload)))
      if packetSize == (len(payload) - 2):
          clientProtocolType = payload[2]
          if clientProtocolType == 1 : # Traffic data
              radarID = payload[3]
              carType = payload[4]
              lane = payload[5]

              speed =  payload[6] % 0xFFFF
              speed = (speed << 8) + (payload[7] & 0x00FF)
              direction = payload[8]

              #print("tyep " + str(carType))
              #print("lane " + str(lane))
              #print("speed " + str(speed))
              #print("ditection " + str(direction))

              #return packetHeader, type, lane, speed, direction
              print("traffic data comming")
              insertTrafficDataToDB(radarID, speed, lane, direction)

          elif clientProtocolType == 2 : # aggregate date
              print("aggregate data comming")

              radarID, = struct.unpack('H', payload[3:5])
              laneID = payload[5]
              laneOCC, = struct.unpack('f', payload[6:10])
              avgSpeed, = struct.unpack('f', payload[10:14])
              headWay, = struct.unpack('I', payload[14:18])
              vehicleCount, = struct.unpack('I', payload[18:22])
              class1Count, = struct.unpack('I', payload[22:26])
              class2Count, = struct.unpack('I', payload[26:30])
              class3Count, = struct.unpack('I', payload[30:34])
              class4Count, = struct.unpack('I', payload[34:38])

              #print("radarID " + str(radarID))
              #print("laneID " + str(laneID))
              #print("laneOCC " + str(laneOCC))
              #print("avgSpeed " + str(avgSpeed))
              #print("headWay " + str(headWay))
              #print("vehicelCount " + str(vehicleCount))
              #print("class 1 " + str(class1Count))
              #print("class 2 " + str(class2Count))
              #print("class 3 " + str(class3Count))
              #print("class 4 " + str(class4Count))

              insertAggregateDataToDB(radarID, laneID, laneOCC, avgSpeed, headWay, vehicleCount, class1Count,
                                      class2Count, class3Count, class4Count)

              #showDataInAggregateReport()
              return 0

          elif clientProtocolType == 0x20 : # sendSystemStatus protocol
              if payload[len(payload) - 1] == 0xc8:
                dbUpdateStatusSystem(payload[3], payload[4], payload[5])
                print("sendSystemStatus protocol")
                resetStatusSystemCount(payload[3] - 1)
              return 0

          elif clientProtocolType == 0x21 : # sendRadarStatus protocol
              if payload[len(payload) - 1] == 0xc8:
                dbUpdateStatusRadar(payload[3], payload[4])
                print("sendRadarStatus protocol")
                resetRadarCount(payload[3] - 1 )
              return 0

          elif clientProtocolType == 0x22 : # sendDisplayStatus protocol
              if payload[len(payload) - 1] == 0xc8:
                dbUpdateStatusDisplay(payload[3], payload[4])
                print("sendDisplayStatus protocol")
                resetDisplayCount(payload[3] - 1)
              return 0

      return 0
  return 0


################################
#   mqtt function zone    #
################################

def on_connect(client, userdata, flags, rc):
    global mqttConnectFlag

    print("Connected with result code "+str(rc))
    if rc == 0 :
        client.subscribe("radarTrafficData/#", qos=0)
        client.subscribe("trafficAggregate/#", qos=0) # subscribe all sub-topic trafficAggregate/
        client.subscribe(mqttTopicUpdateDisplaySub, qos = 0)
        client.subscribe(mqttTopicUpdateRadarSub, qos=0)
        client.subscribe(mqttTopicUpdateSystemSub, qos = 0)
        mqttConnectFlag = 1

    else:
        mqttConnectFlag = 0

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global checkAliveCounter

    '''
    if msg.topic == "radarTrafficData/1" :
        print("radarTrafficData/1")
        #print(msg.topic + " " + str(msg.payload))
        result, type, lane, speed, direction = deProtocol(msg.payload)
        if result :
          insertTrafficDataToDB(1, speed, lane, direction)
          #showDataInViewReport()
          checkAliveCounter = 0

    elif msg.topic == "trafficAggregate/1":
        print("trafficAggregate/1 is comming")
        deProtocol(msg.payload)
        checkAliveCounter = 0

    else :
        deProtocol(msg.payload)
        checkAliveCounter = 0
        
    '''

    deProtocol(msg.payload)
    checkAliveCounter = 0

def on_disconnect(client, userdata, rc):
    global mqttConnectFlag
    print("mqtt client disconnect")
    mqttConnectFlag = 0

def on_socket_close(client, rc):
    global mqttConnectFlag
    print("mqtt client socket close")
    mqttConnectFlag = 0

def mqttConnect():
    global client, mqttIP, mqttPort, mqttConnectFlag

    try :
        #client = None
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        client.on_socket_close = on_socket_close

        client.connect(mqttIP, mqttPort, 60)
        client.loop_start()
    except:
        print("------------------------------- can't connect to mqtt broker")
        mqttConnectFlag = 0

def mqttDisconnect():
    global client
    try :
        client.loop_stop(force=True)
    except:
        print("mqtt can't stop loop")

    try :
        client.disconnect()
    except:
        print("mqtt can't disconnect")
################################
#   main function zone    #
################################
checkAliveCounter = 0

def main():

    global mysqlReconnectDelay, mysqlConnectFlag, mqttReconnectDelay, mqttConnectFlag, checkAliveCounter

    mqttCount = 0

    print("iogDBServerServiceAndStatus Start")

    while True :

        try:

            if mqttConnectFlag != 1:
                if mqttCount < mqttReconnectDelay :
                    mqttCount = mqttCount + 1
                else:
                    mqttCount = 0
                    print("re connect to mqtt broker")
                    mqttDisconnect()
                    mqttConnect()

            if mqttConnectFlag == 1:
                checkCountStatus()

            checkAliveCounter += 1
            if checkAliveCounter > 200 :
                print("checkAliveCounter reset")
                checkAliveCounter = 0
                #mysqlConnectFlag = 0
                mqttConnectFlag = 0
            else :
                print("checkAliveCounter : " + str(checkAliveCounter))

        except:
            print("something error in loop !!!")

        time.sleep(1)

main() # program start here

