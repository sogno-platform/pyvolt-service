# Mapping rules:
#
# MAC -> SOGNOv2
#
# identifier -> device
# identifier_to_cim_comp.conf -> meas_id
# timestamp + 000 as msmsms -> timestamp
# data as string -> data as float
# type + source + -> type
from json import dumps, loads
import numpy as np


def convertMacMessageToSognoMessage(message):
    """
    TBD
    """

    mac_message = loads(message.payload)

    source_to_phase = {"channel_1": "A", "channel_2": "B", "channel_3": "C", "channel_N": "N"}
    device_type_to_comp = {  ("862877034274926-1", "volt"): "_TopologicalNode_STN-032187Y",
                             ("865374038674019-1", "volt"): "_TopologicalNode_85946"}

    sogno_message = {}
    sogno_message["device"] = mac_message["identifier"]
    sogno_message["timestamp"] = mac_message["timestamp"]
    sogno_message["readings"] = []

    mac_readings = mac_message["readings"]

    for mac_elem in mac_readings:
        sogno_elem = {}      
        try:
            sogno_elem["component"] = device_type_to_comp[(mac_message["identifier"], mac_elem["type"])]
        except KeyError:
            sogno_elem["component"] = "unspecified"
            #print("Warning: mapping from ({}, {}, {}) to component not specified. Setting \"component\" field to \"{}\")".format(mac_message["identifier"], mac_elem["source"], mac_elem["type"], sogno_elem["component"]))
                
        sogno_elem["measurand"] = mac_elem["type"]
        sogno_elem["phase"] = source_to_phase[mac_elem["source"]]
        if sogno_elem["measurand"] == "volt":
            sogno_elem["data"] = float(mac_elem["data"])
        else:
            sogno_elem["data"] = float(mac_elem["data"])        
        sogno_message["readings"].append(sogno_elem)

    return sogno_message
