from json import dumps, loads
from datetime import datetime, timedelta
from acs.state_estimation.measurement import MeasType

import numpy as np
from acs.state_estimation.results import Results

def read_mapping_file(mapping_file):
	"""
	Create a list which contains the order in which the data must be received/sent from/to VillasNode.
	This mapping is readed from the files villas_node_input_data/villas_node_output_data
	
	@param mapping_file: received message  from the server (json.loads(msg.payload)[0])
	@return mapping: 
		- if input is villas_node_input_data  --> each element of mapping is a list of length 3: [id, "V", "mag" or "phase"]
		- if input is villas_node_output_data --> each element of mapping is a list of length 4: [id, "V", "mag" or "phase", "pf" or "est"]
		* if id = "max_err" or "mean_err" or "scenario_flag" --> this element has the length 1: ["max_err" or "mean_err" or "scenario_flag"]
	"""
	lines = []
	with open(mapping_file) as mfile:
		for line in mfile:
			lines.append(line.strip('\n'))
	mapping = [None] * len(lines)
	for pos, elem in enumerate(lines):	
		mapping[pos] = elem.split(".")
		
	return mapping

def receiveSognoInput(message, pmu_measurements_set):
	"""
	to store the received data in an object of type acs.state_estimation.measurement
	
	"""

	msg_dict=loads(message.payload)
	received_meas_type = msg_dict["type"].split("_")
	if received_meas_type[0]=="volt" and received_meas_type[1]=="phsa": # TODO - support further types		
		pmu_measurements_set.update_measurement(msg_dict["meas_id"], MeasType.Vpmu_mag, msg_dict["data"], False)

def sendSognoOutput(client, topic_publish, state_estimation_results):
	"""
	to create the payload according to "sogno_output.json"
	@param client: MQTT client instance to be used for publishing
	@param topic_publish: topic used for publishing
	@param state_estimation_results: results of state_estimation (type acs.state_estimation.results.Results)
	"""
	
	# use timestamp from dpsim
	timestamp_sogno = datetime.now()	
	timestamp_sogno = timestamp_sogno.strftime("%Y-%m-%dT%H:%M:%S")

	# publish node results
	for node in state_estimation_results.nodes:
		node_id = node.topology_node.uuid

		SognoOutput = {}
		SognoOutput["comp_id"] = node_id
		SognoOutput["timestamp"] = timestamp_sogno

		# publish node voltage magnitude
		SognoOutput["data"] = np.absolute(node.voltage)
		SognoOutput["type"] = "volt_phsa_abs"
		client.publish(topic_publish, dumps(SognoOutput), 0)

		# publish node voltage angle
		SognoOutput["data"] = np.angle(node.voltage)
		SognoOutput["type"] = "volt_phsa_angle"
		client.publish(topic_publish, dumps(SognoOutput), 0)

	
def serviceCalculations():
	pass

	
