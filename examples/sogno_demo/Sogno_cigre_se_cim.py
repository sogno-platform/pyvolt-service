import time
import logging
import json
import paho.mqtt.client as mqtt
import numpy as np

import cimpy
from  acs.state_estimation.network import System
from  acs.state_estimation.nv_state_estimator_cim import DsseCall
from  acs.state_estimation.measurement import Measurents_set
from  acs.state_estimation.results import Results

logging.basicConfig(filename='recv_client.log', level=logging.INFO, filemode='w')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	if rc==0:
		client.connected_flag=True	  #set flag
		print("connected OK with returned code=", rc)
	else:
		print("Bad connection with returned code=", rc)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	global end
	#stop simulation when message "end" is received
	if json.loads(msg.payload) == "end":
		end = True
	else:
		message = json.loads(msg.payload)[0]
		sequence = message['sequence']
		data = message['data']

		if sequence>0:
			#store the recived data in powerflow_results
			for node in powerflow_results.nodes:
				magnitude = 0.0
				phase = 0.0
				uuid = node.topology_node.uuid
				for elem_idx, elem_data in enumerate(mapping):
					if elem_data[0] == uuid:				 #elem_data[0] == uuid
						if elem_data[1] == "mag":			#elem_data[1] = "mag" or "phase"
							magnitude = data[elem_idx]
						elif elem_data[1] == "phase":
							phase = data[elem_idx]
				node.voltage = magnitude*(np.cos(phase) + 1j*np.sin(phase))/1000
				node.voltage_pu = node.voltage/node.topology_node.baseVoltage
			
			#calculate quantities I, Iinj, S and Sinj
			powerflow_results.calculate_all()

			#read measurements from file
			measurements_set = Measurents_set()
			try:
				if sequence<90:
					measurements_set.read_measurements_from_file(powerflow_results, meas_configfile1)
				else:
					measurements_set.read_measurements_from_file(powerflow_results, meas_configfile2)
			except Exception as e:
				print(e)
			#print(measurements_set.getMeasValues())
			
			try:
				#calculate the measured values (affected by uncertainty)
				measurements_set.meas_creation(dist="uniform", seed=sequence)
				#Performs state estimation
				state_estimation_res = DsseCall(system, measurements_set)
				print("state_estimation_res finish!")
			except Exception as e:
				print(e)
			
			#send results to dummy_simulator
			payload = {}
			payload["client"] = "Sogno_cigre_se_cim"
			payload["sequence"] = sequence
			payload["V_est_mag"] = np.absolute(state_estimation_res.get_voltages()).tolist()
			payload["V_est_phase"] = np.angle(state_estimation_res.get_voltages()).tolist()

			#print(payload)
			mqttc.publish(topic_dummy_simulator, "[" + json.dumps(payload) + "]", 0)

#grid files
xml_files = [
	r"..\..\..\cim-grid-data\CIGRE_MV\CIGRE_MV_no_tapchanger_With_LoadFlow_Results\Rootnet_FULL_NE_06J16h_EQ.xml",
	r"..\..\..\cim-grid-data\CIGRE_MV\CIGRE_MV_no_tapchanger_With_LoadFlow_Results\Rootnet_FULL_NE_06J16h_SV.xml",
	r"..\..\..\cim-grid-data\CIGRE_MV\CIGRE_MV_no_tapchanger_With_LoadFlow_Results\Rootnet_FULL_NE_06J16h_TP.xml"]

#measurements files
meas_configfile1 = r".\Measurement_config2.json"
meas_configfile2 = r".\Measurement_config3.json"  

#load grid
Sb = 25
res = cimpy.cimread(xml_files)
system = System()
system.load_cim_data(res, Sb)

#create a results object to store the received data
powerflow_results = Results(system)

#read mapping file and split each line of mapping_file by point e.g.: N0.V.mag -> ["V0", "mag"]
mapping_file = r"..\..\..\state-estimation-client\sogno_demo\villas_sent_data.conf"
mapping = []
with open(mapping_file) as mfile:
	for line in mfile:
		mapping.append(line.strip('\n'))
for num, elem in enumerate(mapping):
	uuid, V, type = elem.split(".")
	mapping[num] = [None]*2
	mapping[num][0] = uuid
	mapping[num][1] = type

#global variable used in callback on_message
end = False

##topic names
topic = "dpsim-powerflow"							   
topic_dummy_simulator = "dpsim-powerflow_results"		  

broker_adress = "m16.cloudmqtt.com"
mqtt.Client.connected_flag=False						#create flag in class
mqttc = mqtt.Client("SognoDemo_Client", True)		   #create new instance
mqttc.username_pw_set("ilgtdaqk", "UbNQQjmcUdqq")
mqttc.on_connect = on_connect						   #attach function to callback
mqttc.on_message = on_message						   #attach function to callback
mqttc.connect(broker_adress, 14543)					 #connect to broker
mqttc.loop_start()									  #start loop to process callback
time.sleep(4)										   #wait for connection setup to complete
mqttc.subscribe(topic)

while not mqttc.connected_flag:	 #wait in loop
	print("In wait loop")
	time.sleep(1)

#send notification message notifying that the connection was successful
mqttc.publish(topic_dummy_simulator, json.dumps("OK"))

while not end:
	time.sleep(0.01)

mqttc.loop_stop()	   #Stop loop 
mqttc.disconnect()	  #disconnect