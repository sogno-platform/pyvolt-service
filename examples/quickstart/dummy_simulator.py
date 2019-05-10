import json
import paho.mqtt.client as mqtt
import time
import numpy as np

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True      #set flag
        print("connected OK with returned code=",rc)
    else:
        print("Bad connection with returned code=",rc)

# parameters
sequence = 1
len_data = 300
publishing_topic = "dpsim-powerflow"

'''
# Public Message Broker
broker_adress = "m16.cloudmqtt.com"
mqttc = mqtt.Client("SognoDemo_Client", True)		   	#create new instance
mqttc.username_pw_set("ilgtdaqk", "UbNQQjmcUdqq")
mqttc.on_connect = on_connect                   		#attach function to callback
mqttc.connect(broker_adress, 14543)					 	#connect to broker
'''

# ACS Message Broker
broker_adress = "137.226.248.91"
mqttc = mqtt.Client("SognoDemo", True)          #create new instance
mqttc.username_pw_set("villas", "s3c0sim4!")
mqttc.on_connect = on_connect                   #attach function to callback
mqttc.connect(broker_adress)                    #connect to broker

mqttc.loop_start()                              #start loop to process callback
time.sleep(1)                                   #wait for connection setup to complete

data_file = r"D:\git\code\state-estimation-client\sogno_demo\dpsim_powerflow_record_cigre.txt"
data = []
with open(data_file) as json_file:
    for line in json_file:
        data.append(line)

while sequence < len_data + 1:
    mqttc.publish(publishing_topic, data[sequence])
    print("Sent data for sequence " + str(sequence) + ": " + data[sequence])
    sequence += 1
    time.sleep(1)

mqttc.loop_stop()       #Stop loop
mqttc.disconnect()      #disconnect