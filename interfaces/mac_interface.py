# Mapping rules:
#
# MAC -> SOGNOv2
#
# identifier -> device
# identifier_to_cim_comp.conf -> meas_id
# timestamp + 000 as msmsms -> timestamp
# data as string -> data as float
# type + source + -> type


def convertMacMessageToSognoMessage(mac_message, version="1.0"):
    """
    @param VillasNode: received message formatted according to "villas_node_input.json"
    @param input_mapping_vector: according to villas_node_input.json (result of read_mapping_file)
    @param version:
    @param type:
    @return: json object formatted according to "sogno_input.json"
    """

    source_to_phase = {"channel_1": "A", "channel_2": "B", "channel_3": "C", "channel_N": "N"}
    device_source_type_to_comp = {  ("865374032859855-1", "channel_1", "activepower"): "line1", 
                                    ("865374032859855-1", "channel_1", "apparentpower"): "line1", 
                                    ("865374032859855-1", "channel_1", "current"): "line1", 
                                    ("865374032859855-1", "channel_1", "frequency"): "bus1", 
                                    ("865374032859855-1", "channel_1", "powerfactor"): "line1", 
                                    ("865374032859855-1", "channel_1", "reactivepower"): "line1", 
                                    ("865374032859855-1", "channel_1", "volt"): "bus1"}

    sogno_message = {}
    sogno_message["device"] = mac_message["identifier"]
    sogno_message["timestamp"] = mac_message["timestamp"]
    sogno_message["readings"] = []

    mac_readings = mac_message["readings"]

    for mac_elem in mac_readings:
        sogno_elem = {}      

        try:
            sogno_elem["component"] = device_source_type_to_comp[(mac_message["identifier"], mac_elem["source"], mac_elem["type"])]
        except KeyError:
            sogno_elem["component"] = "unspecified"
            print("Warning: mapping from ({}, {}, {}) to component not specified. Setting \"component\" field to \"{}\")".format(mac_message["identifier"], mac_elem["source"], mac_elem["type"], sogno_elem["component"]))
                
        sogno_elem["measurand"] = mac_elem["type"]
        sogno_elem["phase"] = source_to_phase[mac_elem["source"]]
        sogno_elem["data"] = float(mac_elem["data"])
        sogno_message["readings"].append(sogno_elem)

    return sogno_message
