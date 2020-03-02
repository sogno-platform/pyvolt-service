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


def convertMacMessageToSognoMessage(mac_message, device_type_to_comp):
    """
    TBD
    """

    # Mapping of terminology: from mac source to sogno phase
    source_to_phase = {"channel_1": "A", "channel_2": "B", "channel_3": "C", "channel_N": "N"}

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
            print("Warning: mapping from ({}, {}) to CIM component not specified.".format(mac_message["identifier"], mac_elem["type"]))
        sogno_elem["measurand"] = mac_elem["type"]
        sogno_elem["phase"] = source_to_phase[mac_elem["source"]]
        if sogno_elem["measurand"] == "activepower" or sogno_elem["measurand"] == "reactivepower":
            # convert single-phase power in kW to single-phase power in W as expected by SOGNO interface
            sogno_elem["data"] = float(mac_elem["data"])*1e3
        else:
            # take data as they are
            sogno_elem["data"] = float(mac_elem["data"])        
        sogno_message["readings"].append(sogno_elem)

    return sogno_message
