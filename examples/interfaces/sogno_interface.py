from json import dumps, loads
from datetime import datetime, timedelta

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

def receiveSognoInput(system, message, input_mapping_vector):
	"""
	to store the received data in an object of type acs.state_estimation.results.Results
	
	@system: model of the system (type acs.state_estimation.network.System)
	@param message: received message from the server (json.loads(msg.payload)[0])
	@param input_mapping_vector: according to villas_node_input.json ((result of read_mapping_file))
	@return powerflow_results: object type acs.state_estimation.results.Results
	"""
	data = message['readings']

	#create a results object to store the received data
	powerflow_results = Results(system)

	#store the received data in powerflow_results
	for node in powerflow_results.nodes:
		magnitude = 0.0
		phase = 0.0
		uuid = node.topology_node.uuid
		for elem in data:
			if elem["id"] == uuid:
				if elem["measurand"] == "voltage_magnitude":
					magnitude = elem["data"]
				elif elem["measurand"] == "voltage_angle":
					phase = elem["data"]
			else:
				continue
		node.voltage = magnitude * (np.cos(phase) + 1j * np.sin(phase)) / 1000
		node.voltage_pu = node.voltage / node.topology_node.baseVoltage
	
	#calculate quantities I, Iinj, S and Sinj
	powerflow_results.calculate_all()
	
	return powerflow_results

def sendSognoOutput(message, output_mapping_vector, 
		state_estimation_results, scenario_flag,
		version="1.0", type = "se_result"):
	"""
	to create the payload according to "sogno_output.json"
	
	@param message: received message from the server (json.loads(msg.payload)[0])
	@param output_mapping_vector: according to villas_node_output.json (result of read_mapping_file)
	@param state_estimation_results: results of state_estimation (type acs.state_estimation.results.Results)
	@param scenario_flag:
	@param version:
	@param type:
	@return payload: a string with the data to send to the server according to the output mapping file 
	"""
	
	SognoOutput = {}
	SognoOutput["version"] = version
	SognoOutput["type"] = type
	SognoOutput["nodes"] = []
	
	timestamp_villas = message["ts"]["origin"][0]
	timestamp_sogno = datetime(1970,1,1,0,0,0) + timedelta(seconds=timestamp_villas)
	#timestamp_sogno = datetime.now()
	timestamp_sogno = timestamp_sogno.strftime("%Y-%m-%dT%H:%M:%S")

	for node in state_estimation_results.nodes:
		node_id = node.topology_node.uuid
		values = []
		
		values.append({
			#"timestamp": timestamp,
			"timestamp": timestamp_sogno,
			"phase": "a",
			"measurand": "voltage_magnitude",
			"data": np.absolute(node.voltage)
		})
		values.append({
			#"timestamp": timestamp,
			"timestamp": timestamp_sogno,
			"phase": "a",
			"measurand": "voltage_angle",
			"data": np.angle(node.voltage)
		})
		vnode = {"node_id": node_id,
				 "values": values}
		SognoOutput["nodes"].append(vnode)
		
	return dumps(SognoOutput)

	
def serviceCalculations():
	pass

	
