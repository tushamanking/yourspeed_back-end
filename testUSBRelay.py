import subprocess

usb_serial_number = "959BI"


def sendGmail():

    subprocess.call(["python", "../GmailAPI/sending.py"])

    try:
        print("sendGamil : sending")
        #subprocess.call(["sendingGmail"])
    except :
        print("sendGamil : error : sending")

def relayTest(): 
    try:
        print("closing the radar")
        subprocess.call(["CommandApp_USBRelay.exe", usb_serial_number, "open", "01"])
    except :
        print("error : closing the radar USB Realay dont online")


sendGmail()