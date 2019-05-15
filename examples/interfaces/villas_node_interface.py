# Class which implements an interface layer between villas_node and the state-estimator
# Later some functions to implement a conversion layer from the villas_node to the sogno_format

# At first basic functions to establish the connection

# receiveVillasNodeInput()
# - according to villas_node_input.json
# - for this, read villas_node_input_data.conf

# sendVillasNodeOutput()
# - according to villas_node_output.json
# - write villas_node_output_data.conf (useful later to configure VILLASweb acoordingly)
# - refer to comment in run_state_estimation_service.py to see current configuration

# convertVillasNodeInputToSognoInput
# convertVillasNodeOutputToSognoOutput

