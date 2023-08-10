import serial
import time
import struct



def readComport():
    sePort = serial.Serial('COM3')
    sePort.baudrate = 9600
    time.sleep(2)
    sePort.write(b'B')
    buf = sePort.readline()
    voltLevel =  int(buf.decode("utf-8").replace('\r\n', ''))
    sePort.close()
    voltLevel = round(voltLevel/5)
    if voltLevel > 100:
        voltLevel = 100

    return voltLevel
    
print(readComport())