
import sys
import pyodbc
import win32com.client
import time


Barcode = ""
Barcode_StartIndex = 0
Barcode_EndIndex = 0

Destination = ""
Destination_StartIndex = 0
Destination_EndIndex = 0

def getconn():
    DRIVER = 'SQL Server'
    SERVER_NAME = 'SA1040\SQLEXPRESS'
    DATABASE_NAME = 'SystemsAutomated'

    conn_string = f"""
        Driver={{{DRIVER}}};
        Server={SERVER_NAME};
        Database={DATABASE_NAME};
        Trust_Connection=yes;
     """
    return conn_string

try: 
    while Destination != "0":
        #get next item from Queue
        qinfo=win32com.client.Dispatch("MSMQ.MSMQQueueInfo")
        #computer_name = os.getenv('COMPUTERNAME')
        computer_name = "SA1040"
        qinfo.FormatName="direct=os:"+computer_name+"\\PRIVATE$\\TestQueue"
        queue=qinfo.Open(1,0)   # Open a ref to queue to read(1)
        msg=queue.Receive()
        print ("Label:",msg.Label)
        print ("Body :",msg.Body)
        queue.Close()

        #Find Barcode
        Barcode_StartIndex = msg.Body.find("CaseId=") 
        if Barcode_StartIndex > 0:
            Barcode_EndIndex = msg.Body[Barcode_StartIndex + 8::].find('"') + Barcode_StartIndex + 8
            Barcode = msg.Body[Barcode_StartIndex+8:Barcode_EndIndex]
            #print(Barcode_StartIndex)
            #print(Barcode_EndIndex)
            print ("Barcode: " + Barcode)
        else:
            print("Barcode Not Found")


        #Find Destination
        Destination_StartIndex = msg.Body.find("AreaId=")
        if Destination_StartIndex > 0:
            Destination_EndIndex = msg.Body[Destination_StartIndex + 8::].find('"') + Destination_StartIndex + 8
            Destination = msg.Body[Destination_StartIndex+8:Destination_EndIndex]
            #print(Destination_StartIndex)
            #print(Destination_EndIndex)
            print ("Destination: " + Destination)
        else:
            print("Destination Not Found")

        #print(type(Destination))
        if Destination != "0":

            #Function to get the conn_string
            conn_string = getconn()

            #Connect to SQL Server
            conn = pyodbc.connect(conn_string)

            cursor = conn.cursor()

            SQLCheck = """
                BEGIN
                INSERT INTO MSMQ
                VALUES (GETDATE(), ?, ?, ?)
                END
            """

            records = [
                [Barcode, Destination, msg.Body]
            ]

            for record in records:
                cursor.execute(SQLCheck, record)

            conn.commit()

except:
    #error_string = str(datetime.datetime.now()) + " " + str(traceback.format_exc()) #add timestamp to traceback
    #MSG = "Error Inserting/Updating Process Log"
    #print EntryType + " " + MSG  + " " + error_string
    pass


###############################################################################
#Heartbeat Query after While Loop is broken

#Setting Variables to communicate "Heartbeat" Message
Barcode = "Heartbeat"
msg.Body = "Heartbeat"

#Function to get the conn_string
conn_string = getconn()

#Connect to SQL Server
conn = pyodbc.connect(conn_string)

cursor = conn.cursor()

SQLCheck = """
    IF NOT EXISTS (SELECT * FROM MSMQ WHERE Barcode = ?)
    BEGIN
    INSERT INTO MSMQ
    VALUES (GETDATE(), ?, ?, ?)
    END
    ELSE
    BEGIN
    UPDATE MSMQ
    SET DateTime = GETDATE(), Barcode = ?, Target_Location = ?, Message =?
    WHERE Barcode = ?
    END
"""

records = [
    [Barcode, Barcode, Destination, msg.Body, Barcode, Destination, msg.Body, Barcode]
        ]

for record in records:
    cursor.execute(SQLCheck, record)

conn.commit()