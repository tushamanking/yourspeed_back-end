import cv2
import time
from ftplib import FTP

#cap = cv2.VideoCapture(0)
#cap = cv2.VideoCapture('http://192.168.2.2:8080/?action=stream')

ftpUser = "yourspeedId3"
ftpPass = "yourspeed"

def ftpTest():
    global ftpUser, ftpPass
    try:
        print("upload picture ...")
        ftps = FTP('119.59.115.206')
        ftps.login(user=ftpUser, passwd=ftpPass)
        file = open('Test.jpg', 'rb')
        ftps.storbinary('STOR Test.jpg', file)  # send the file
        file.close()
        ftps.quit()
    except:
        print("can't ftp upload pic to server")

timer = 0

while(True):
    # Capture frame-by-frame

    try:
        print("read image streaming")
        cap = cv2.VideoCapture(0)
        #cap = cv2.VideoCapture('http://192.168.2.2:8080/?action=stream')
        ret, frame = cap.read()

        # Our operations on the frame come here
        #gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        #gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        height, width, layer = frame.shape
        new_h = int(height / 4)
        new_w = int(width / 4)
        resize_im = cv2.resize(frame, (new_w, new_h))

        # Display the resulting frame
        #cv2.imshow('frame',resize_im)
        cv2.imwrite('Test.jpg', resize_im)

        ftpTest()

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    except:
        print("error on read() to cap cv2")

    cap.release()
    print("time sleep 30")
    time.sleep(30)

# When everything done, release the capture
#cap.release()
#cv2.destroyAllWindows()