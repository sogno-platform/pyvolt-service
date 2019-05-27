import time
import logging
import json
import paho.mqtt.client as mqtt
import numpy as np

import cimpy
from acs.state_estimation.network import System
from acs.state_estimation.nv_state_estimator import DsseCall
from acs.state_estimation.measurement import Measurents_set
from acs.state_estimation.results import Results

logging.basicConfig(filename='recv_client.log', level=logging.INFO, filemode='w')

def connect(client_name, username, password, broker_adress, port=1883):
	mqttc = mqtt.Client(client_name, True)		   	
	mqttc.username_pw_set(username, password)
	mqttc.on_connect = on_connect					#attach function to callback
	mqttc.on_message = on_message					#attach function to callback
	mqttc.connect(broker_adress, port)				#connect to broker
	mqttc.loop_start()								#start loop to process callback
	time.sleep(4)									#wait for connection setup to complete
	
	return mqttc
	
def on_connect(client, userdata, flags, rc):
	"""
	The callback for when the client receives a CONNACK response from the server.
	"""
	if rc == 0:
		client.subscribe(topic_subscribe)
		print("connected OK with returned code=", rc)
	else:
		print("Bad connection with returned code=", rc)


def on_message(client, userdata, msg):
	"""
	The callback for when a PUBLISH message is received from the server
	"""

	message = json.loads(msg.payload)[0]
	sequence = message['sequence']
	
	if sequence > 0:
		#store the received data in powerflow_results
		powerflow_results = receiveVillasNodeInput(system, message, input_mapping_vector)
		
		#read measurements from file
		measurements_set = Measurents_set()
		try:
			if sequence < 90:
				measurements_set.read_measurements_from_file(powerflow_results, meas_configfile1)
				scenario_flag=1
			else:
				measurements_set.read_measurements_from_file(powerflow_results, meas_configfile2)
				scenario_flag=2

			#calculate the measured values (affected by uncertainty)
			measurements_set.meas_creation(dist="uniform", seed=sequence)
			#Performs state estimation
			state_estimation_results = DsseCall(system, measurements_set)


			#send results to message broker
			payload = sendVillasNodeOutput(message, output_mapping_vector, powerflow_results, state_estimation_results, scenario_flag)
			mqttc.publish(topic_publish, "[" + json.dumps(payload) + "]", 0)
			#print(payload)
			
		except Exception as e:
			print(e)
		
		# Finished message
		print("Finished state estimation for sequence " + str(sequence))
		
def receiveVillasNodeInput(system, message, input_mapping_vector):
	"""
	function to store the received data in powerflow_results
	
	@system: model of the system (type acs.state_estimation.network.System)
	@param message: received message  from the server (json.loads(msg.payload)[0])
	@param input_mapping_vector: vector that mapping the result of read_mapping_file
	@return powerflow_results: object (type acs.state_estimation.results.Results) to store the received data
	"""
	data = message['data']

	#create a results object to store the received data
	powerflow_results = Results(system)

	#store the received data in powerflow_results
	for node in powerflow_results.nodes:
		magnitude = 0.0
		phase = 0.0
		uuid = node.topology_node.uuid
		for idx, elem in enumerate(input_mapping_vector):
			#print("elem[0]: {}, uuid: {}".format(elem[0], uuid))
			if elem[0] == uuid:
				if elem[2] == "mag":  # elem[1] = "mag" or "phase"
					magnitude = data[idx]
				elif elem[2] == "phase":
					phase = data[idx]
		node.voltage = magnitude * (np.cos(phase) + 1j * np.sin(phase)) / 1000
		node.voltage_pu = node.voltage / node.topology_node.baseVoltage
	
	#calculate quantities I, Iinj, S and Sinj
	powerflow_results.calculate_all()
	
	return powerflow_results
	
def sendVillasNodeOutput(message, output_mapping_vector, powerflow_results, state_estimation_results, scenario_flag):
	"""
	function to create the payload to send 
	
	@param message: received message  from the server (json.loads(msg.payload)[0])
	@param output_mapping_vector: 
	@param powerflow_results:
	@param state_estimation_results:
	@param scenario_flag:
	@return payload:
	"""
	
	payload = {}
	payload["ts"] = {}
	payload["ts"]["origin"] = message["ts"]["origin"]
	payload["sequence"] = message["sequence"]
	
	#calculate Vmag_err
	Vmag_err = np.zeros(len(powerflow_results.nodes))
	for idx, elem in enumerate(powerflow_results.nodes):
		uuid_pf = elem.topology_node.uuid
		Vmag_true = np.absolute(elem.voltage)
		Vmag_est = np.absolute(state_estimation_results.get_node(uuid=uuid_pf).voltage)
		Vmag_err[idx] = 100*np.abs(((Vmag_est - Vmag_true)/Vmag_true))
		Vmag_err[idx] = 100*((Vmag_est - Vmag_true)/Vmag_true)
	
	max_err = np.amax(Vmag_err)
	mean_err = np.mean(Vmag_err)
	
	data = [None]*len(output_mapping_vector)
	for idx, elem in enumerate(output_mapping_vector):
		if elem[0] == "max_err":
			data[idx] = max_err
			continue
		elif elem[0] == "mean_err":
			data[idx] = mean_err
			continue
		elif elem[0] == "scenario_flag":
			data[idx] = scenario_flag
			continue
		else:	#elem = ["N4", "V", "phase", "est"] or elem = ["N4", "V", "phase", "pf"]
			node = None
			if elem[3] == "est":
				node = state_estimation_results.get_node(uuid=elem[0])
			elif elem[3] == "pf":
				node = powerflow_results.get_node(uuid=elem[0])
			
			value = None
			if elem[2] == "mag":  # elem_data[2] = "mag" or "phase"
				value = np.absolute(node.voltage)
			elif elem[2] == "phase":
				value = np.angle(node.voltage)
			data[idx] = value
	
	payload["data"] = data
	return payload
	
def read_mapping_file(mapping_file):
	"""
	To read villas_node_input_data.config or villas_node_output_data.config
	"""
	lines = []
	with open(mapping_file) as mfile:
		for line in mfile:
			lines.append(line.strip('\n'))
	mapping = [None] * len(lines)
	for pos, elem in enumerate(lines):	
		mapping[pos] = elem.split(".")
		
	return mapping
	
#grid files
xml_files = [
	r"..\..\state-estimation\examples\quickstart\sample_data\Rootnet_FULL_NE_06J16h_EQ.xml",
	r"..\..\state-estimation\examples\quickstart\sample_data\Rootnet_FULL_NE_06J16h_SV.xml",
	r"..\..\state-estimation\examples\quickstart\sample_data\Rootnet_FULL_NE_06J16h_TP.xml"]

#measurements files
meas_configfile1 = r"..\configs\Measurement_config2.json"
meas_configfile2 = r"..\configs\Measurement_config3.json"

#read mapping file and create mapping vectors
input_mapping_file = r"..\configs\villas_node_input_data.conf"
output_mapping_file = r"..\configs\villas_node_output_data.conf"
input_mapping_vector = read_mapping_file(input_mapping_file)
output_mapping_vector = read_mapping_file(output_mapping_file)

#load grid
Sb = 25
res = cimpy.cimread(xml_files)
system = System()
system.load_cim_data(res, Sb)

client_name = "SognoDemo_Client"
topic_subscribe = "dpsim-powerflow"
topic_publish = "sogno-estimator"

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

input("Press enter to stop client...\n")
mqttc.loop_stop()  								# Stop loop
mqttc.disconnect()  							# disconnect
