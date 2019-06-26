from json import dumps, loads
import numpy as np
from acs.state_estimation.results import Results

def receiveSognoInput(system, message, input_mapping_vector):
	"""
	to store the received data in an object of type acs.state_estimation.results.Results
	
	@system: model of the system (type acs.state_estimation.network.System)
	@param message: received message from the server (json.loads(msg.payload)[0])
	@param input_mapping_vector: according to villas_node_input.json ((result of read_mapping_file))
	@return powerflow_results: object type acs.state_estimation.results.Results
	"""
	message = loads(message)
	data = message['readings']
	#print(data)

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

# receiveVillasNodeInput()
# - according to villas_node_input.json
# - for this, read villas_node_input_data.conf

def sendSognoOutput(message, output_mapping_vector, 
		powerflow_results, state_estimation_results, scenario_flag,
		version="1.0", type = "se_result"):
	"""
	to create the payload
	
	@param message: received message from the server (json.loads(msg.payload)[0])
	@param output_mapping_vector: according to villas_node_output.json (result of read_mapping_file)
	@param powerflow_results: results of powerflow (type acs.state_estimation.results.Results)
	@param state_estimation_results: results of state_estimation (type acs.state_estimation.results.Results)
	@param scenario_flag:
	@return payload: a string with the data to send to the server according to the output mapping file 
	"""
	
	payload = {}
	payload["version"] = version
	payload["type"] = type
	payload["nodes"] = []
	
	for node in state_estimation_results.results:
		node_id = node.topology_node.uuid
		#timestamp = message["ts"]["origin"]
		values = []
		
		values.append({
			#"timestamp": timestamp,
			"timestamp": "2019-04-08T08:44:01",
			"phase": "a",
			"measurand": "voltage_magnitude",
			"data": np.absolute(node.voltage)
		})
		values.append({
			#"timestamp": timestamp,
			"timestamp": "2019-04-08T08:44:01",
			"phase": "a",
			"measurand": "voltage_angle",
			"data": np.angle(node.voltage)
		})
		vnode = {"node_id": node_id,
				 "values": values}
		payload["nodes"].append(vnode)
		
	return dumps(payload)

	
def serviceCalculations():
	pass

	
