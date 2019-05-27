import paho.mqtt.client as mqtt
import time

def connect(client_name, username, password, broker_adress, port=1883):
	mqttc = mqtt.Client(client_name, True)		   	
	mqttc.username_pw_set(username, password)
	mqttc.on_connect = on_connect					#attach function to callback
	#mqttc.on_message = on_message					#attach function to callback
	mqttc.connect(broker_adress, port)				#connect to broker
	mqttc.loop_start()								#start loop to process callback
	time.sleep(4)									#wait for connection setup to complete
	
	return mqttc
	
def on_connect(client, userdata, flags, rc):
	"""
	The callback for when the client receives a CONNACK response from the server.
	"""
	if rc == 0:
		print("connected OK with returned code=", rc)
	else:
		print("Bad connection with returned code=", rc)

# parameters
sequence = 1
len_data = 300
client_name = "DpsimDummy"
topic_publish = "dpsim-powerflow"

# Public Message Broker
broker_address = "m16.cloudmqtt.com"
mqtt_username = "ilgtdaqk"
mqtt_password = "UbNQQjmcUdqq"
port = 14543

"""
# ACS Message Broker
broker_address = "137.226.248.91"
mqtt_username = "villas"
mqtt_password = "s3c0sim4!"
"""

mqttc = connect(client_name, mqtt_username, mqtt_password, broker_address, port)

data_file = r".\sample_data\dpsim_powerflow_record_cigre.txt"
data = []
with open(data_file) as json_file:
	for line in json_file:
		data.append(line)

while sequence < len_data + 1:
	mqttc.publish(topic_publish, data[sequence])
	print("Sent data for sequence " + str(sequence) + ": " + data[sequence])
	sequence += 1
	time.sleep(1)

mqttc.loop_stop()   # Stop loop
mqttc.disconnect()  # disconnect
