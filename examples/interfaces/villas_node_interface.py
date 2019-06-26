from json import dumps, loads
import numpy as np
from acs.state_estimation.results import Results

def read_mapping_file(mapping_file):
	"""
	To read villas_node_input_data.config or villas_node_output_data.config
	
	@param mapping_file: received message  from the server (json.loads(msg.payload)[0])
	@return payload:
	"""
	lines = []
	with open(mapping_file) as mfile:
		for line in mfile:
			lines.append(line.strip('\n'))
	mapping = [None] * len(lines)
	for pos, elem in enumerate(lines):	
		mapping[pos] = elem.split(".")
		
	return mapping

def receiveVillasNodeInput(system, message, input_mapping_vector):
	"""
	to store the received data in an object of type acs.state_estimation.results.Results
	
	@system: model of the system (type acs.state_estimation.network.System)
	@param message: received message from the server (json.loads(msg.payload)[0])
	@param input_mapping_vector: according to villas_node_input.json ((result of read_mapping_file))
	@return powerflow_results: object type acs.state_estimation.results.Results
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
	to create the payload
	
	@param message: received message from the server (json.loads(msg.payload)[0])
	@param output_mapping_vector: according to villas_node_output.json (result of read_mapping_file)
	@param powerflow_results: results of powerflow (type acs.state_estimation.results.Results)
	@param state_estimation_results: results of state_estimation (type acs.state_estimation.results.Results)
	@param scenario_flag:
	@return payload: a string with the data to send to the server according to the output mapping file 
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
	return "[" + dumps(payload) + "]"
	

def serviceCalculations():
	pass

def convertVillasNodeInputToSognoInput(VillasNodeInput, input_mapping_vector, version="1.0", type = "se_result"):
	"""
	@param VillasNode: received message from the server (json.loads(msg.payload)[0])
	@param input_mapping_vector: according to villas_node_input.json ((result of read_mapping_file))
	@return
	"""
	timestamp = VillasNodeInput["ts"]["origin"]
	sequence = VillasNodeInput["sequence"]
	data = VillasNodeInput["data"]
	
	SongoInput = {}
	SongoInput["version"] = version
	SongoInput["identifier"] = "123456"
	SongoInput["type"] = type
	SongoInput["readings"] = []
	
	for idx, elem in enumerate(input_mapping_vector):
		uuid = elem[0]
		type = elem[2]	#mag or phase
		
		value = {}
		value["id"] = uuid
		value["timestamp"] = timestamp
		value["phase"] = "a"
		value["measurand"] = ""
		if type == "mag":
			value["measurand"] = "voltage_magnitude"
		elif type == "phase":
			value["measurand"] = "voltage_angle"
		value["data"] = data[idx]
		
		SongoInput["readings"].append(value)
			
	return dumps(SongoInput)
	
def convertVillasNodeOutputToSognoOutput(VillasNodeOutput, output_mapping_vector, version="1.0", type = "se_result"):
	"""
	@param VillasNodeOutput: 
	@param output_mapping_vector: according to villas_node_input.json ((result of read_mapping_file))
	@return
	"""
	VillasNodeOutput = loads(VillasNodeOutput)[0]
	
	timestamp = VillasNodeOutput["ts"]["origin"]
	sequence = VillasNodeOutput["sequence"]
	data = VillasNodeOutput["data"]
	SognoOutput = {}
	SognoOutput["version"] = version
	SognoOutput["type"] = type
	SognoOutput["nodes"] = []
	
	for idx, elem in enumerate(output_mapping_vector):
		uuid = elem[0]
		if uuid=="max_err" or uuid=="mean_err" or uuid=="scenario_flag":
			continue
		meas_type = elem[2]	#mag or phase
		if elem[3]=="pf":
			continue
		
		value = {}
		value["timestamp"] = timestamp
		value["phase"] = "a"
		value["measurand"] = ""
		if meas_type == "mag":
			value["measurand"] = "voltage_magnitude"
		elif meas_type == "phase":
			value["measurand"] = "voltage_angle"
		value["data"] = data[idx]
		index = search_dict_in_list(SognoOutput["nodes"], uuid)
		if index == None:
			values = [value]
			SognoOutput["nodes"].append({"node_id": uuid,
										 "values": values})
		else:
			SognoOutput["nodes"][index]["values"].append(value)
		
	return dumps(SognoOutput)
	
def search_dict_in_list(dict_list, node_id):
	"""
	search in the list of dicts dict_list if they have one dict with the key node_id==node_id
	@param dict_list: list of dictionaries
	@param node_id: 
	@return: index of the dict in the list
			 None if the dict was not found
	"""
	for idx, dict in enumerate(dict_list):
		if dict["node_id"] == node_id:
			return idx
	return None