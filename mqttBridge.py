import paho.mqtt.client as mqtt
import time
import os

mySystemID = 3

client1 = None
client2 = None

broker1IP = "localhost"
broker1Port = 5000

broker2IP = "119.59.115.206"
broker2Port = 5000

client1_connection = 0
client2_connection = 0

reConnectDelay = 10 # 10s

loopStatus = True

def on_connect1(client, userdata, flags, rc): # these message will be sent to server
    global client1_connection
    print("Connected to broker 1 result code "+ str(rc))
    if rc == 0:
        client.subscribe("updateStatusSystem", qos = 0)
        client.subscribe("updateStatusRadar", qos=0)
        client.subscribe("updateStatusDisplay", qos=0)
        client.subscribe("radarTrafficData/#", qos=0)
        client.subscribe("trafficAggregate/#", qos=0)
        client.subscribe("requestUpdate", qos=0)
        client1_connection = 1
    else:
        client1_connection = 0

# The callback for when a PUBLISH message is received from the server.
def on_message1(client, userdata, msg):
    global brokerOneEventCheckCount, client2
    nowtime = time.localtime(time.time())
    date_s = str(nowtime[0]) + "-" + str(nowtime[1]) + "-" + str(nowtime[2])
    time_s = str(nowtime[3]) + ":" + str(nowtime[4]) + ":" + str(nowtime[5])

    print("broker 1 send" + msg.topic + " forward " + date_s + " " + time_s)
    client2.publish(topic = msg.topic, payload = msg.payload, qos=0, retain=False)
    brokerOneEventCheckCount = 0

def on_connect2(client, userdata, flags, rc): # message come from server
    global client2_connection, mySystemID
    print("Connected to broker 2 result code "+ str(rc))
    if rc == 0:
        client.subscribe("updateDisplay/" + str(mySystemID), qos = 0)
        client.subscribe("updateRadar/" + str(mySystemID), qos=0)
        client.subscribe("updateTime/" + str(mySystemID), qos=0)
        client.subscribe("updateSystem/" + str(mySystemID), qos=0)
        client2_connection = 1

    else :
        client2_connection = 0

# The callback for when a PUBLISH message is received from the server.
def on_message2(client, userdata, msg):
    global brokerTowEventCheckCount, client1
    nowtime = time.localtime(time.time())
    date_s = str(nowtime[0]) + "-" + str(nowtime[1]) + "-" + str(nowtime[2])
    time_s = str(nowtime[3]) + ":" + str(nowtime[4]) + ":" + str(nowtime[5])

    print("broker 2 send" + msg.topic + " forward " + date_s + " " + time_s)
    client1.publish(topic = msg.topic, payload = msg.payload, qos=0, retain=False)

    brokerTowEventCheckCount = 0

def on_socket_close1(client, rc):
    global client1_connection
    print("client1 socket close")
    client1_connection = 0

def on_socket_close2(client, rc):
    global client2_connection
    print("client2 socket close")
    client2_connection = 0

def mqttClient1_Connect():
    global client1, client1_connection

    client1 = mqtt.Client()
    client1.on_connect = on_connect1
    client1.on_message = on_message1
    client1.on_disconnect = on_disconnect1
    client1.on_socket_close = on_socket_close1

    try:
        client1.connect(broker1IP, broker1Port, 60)
        client1.loop_start()
    except:
        print("there is't broker 1")
        client1_connection = 0


def mqttClient1_Disconnect():
    global client1
    try:
        client1.loop_stop(force=True)
    except:
        print("can't stop loop broker 1")

    try:
        client1.disconnect()
    except:
        print("can't disconnect broker 1")
    finally:
        print("client1 = none")
        client1 = None


def on_disconnect1(client, userdata, rc):
    global client1_connection
    print("broker 1 mqtt disconnected !! ")
    client1_connection = 0

def mqttClient2_Connect():
    global client2, client2_connection

    client2 = mqtt.Client()
    client2.on_connect = on_connect2
    client2.on_message = on_message2
    client2.on_disconnect = on_disconnect2
    client2.on_socket_close = on_socket_close2

    try:
        client2.connect(broker2IP, broker2Port, 60)
        client2.loop_start()
    except:
        print("there is't broker 2")
        client2_connection = 0

def mqttClient2_Disconnect():
    global client2
    try :
        client2.loop_stop(force=True)
    except:
        print("can't stop loop broker 2")

    try :
        client2.disconnect()
    except:
        print("can't disconnect broker 2")
    finally:
        print("client2 = none")
        client2 = None

def on_disconnect2(client, userdata, rc):
    global client2_connection
    print("broker 2 mqtt disconnected !! ")
    client2_connection = 0


############################################
###             system function         ###
###########################################

def systemRestart():
    global os, client1_connection, client2_connection
    print("reboot mqtt brigde")
    client1_connection = 0
    client2_connection = 0
    #restartCommand = "reboot"
    #re = os.popen(restartCommand)

def serviceRestart():
    global os,loopStatus
    loopStatus = False
    print("reboot mqtt brigde")
    os.system("python mqttBridge.py")

brokerOneEventCheckCount = 0
brokerTowEventCheckCount = 0

def main():
    global client1_connection, client2_connection, brokerOneEventCheckCount, brokerTowEventCheckCount, os, loopStatus, client1, client2

    print("mqtt Bride start")
    print("try do connect to broker")

    mqttClient1_Connect()
    mqttClient2_Connect()

    reCon1 = 0
    reCon2 = 0

    

    #re = os.popen("python mqttBridge.py")
    

    while loopStatus:

        
        if client1_connection != 1 :
            #print("client 1 restart connect ")
            mqttClient1_Disconnect()
            mqttClient1_Connect()

        if client2_connection != 1 :
            #print("client 2 restart connect ")
            mqttClient2_Disconnect()
            mqttClient2_Connect()
       
        brokerOneEventCheckCount += 1
        brokerTowEventCheckCount += 1


        if brokerOneEventCheckCount > 500 or brokerTowEventCheckCount > 500 :
            brokerOneEventCheckCount = 0
            brokerTowEventCheckCount = 0
            #loopStatus = False 
            print("reConnect")
            mqttClient1_Disconnect()
            mqttClient2_Disconnect()
            client1_connection = 0
            client2_connection = 0

            

        print("brokerOneEventCheckCount %s" % (brokerOneEventCheckCount))
        print("brokerTowEventCheckCount %s" % (brokerTowEventCheckCount))
        
        
        #pass
        time.sleep(1)

main()
print("program end")
