# --- Converts MAC devices messages to SOGNO interface v3 messages
#
# Conversion rules:
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
    

    print("MAC interface:")
    for reading in mac_message['readings']:
        print("Received measurement value: {}, {}, {}, {}, {}".format(mac_message['timestamp'], mac_message['identifier'], reading['type'], reading['source'], reading['data']))

    # Mapping of terminology: from mac source to sogno phase
    source_to_phase = {"channel_1": "A", "channel_2": "B", "channel_3": "C"}

    # Mapping of terminology: from mac type to sogno measurand
    type_to_measurand = {"volt": "voltmagnitude",
                         "activepower": "activepower",
                         "reactivepower": "reactivepower"}

    sogno_message = {}
    sogno_message["device"] = mac_message["identifier"]
    sogno_message["timestamp"] = mac_message["timestamp"]
    sogno_message["readings"] = []
    mac_readings = mac_message["readings"]
    for mac_elem in mac_readings:
        sogno_elem = {}      

        # Supplement mac reading by CIM component UUID related to device and measurand
        if (mac_message["identifier"], mac_elem["type"]) in device_type_to_comp.keys():
            sogno_elem["component"] = device_type_to_comp[(mac_message["identifier"], mac_elem["type"])]
        else:
            sogno_elem["component"] = "unspecified"
            print("Warning: mapping from ({}, {}) to CIM component not specified.".format(mac_message["identifier"], mac_elem["type"]))

        # Map terms for type and source, if mapping available otherwise skip reading
        if mac_elem["type"] in type_to_measurand.keys():
            sogno_elem["measurand"] = type_to_measurand[mac_elem["type"]]
        else:
            pass
        if mac_elem["source"] in source_to_phase.keys():
            sogno_elem["phase"] = source_to_phase[mac_elem["source"]]
        else:
            pass

        # Actual measurement data assignment including necessary value conversion from MAC to SOGNO format
        if sogno_elem["measurand"] == "activepower" or sogno_elem["measurand"] == "reactivepower":
            # convert single-phase power in kW to single-phase power in W as expected by SOGNO interface
            sogno_elem["data"] = float(mac_elem["data"])*1e3
        else:
            # take data as they are
            sogno_elem["data"] = float(mac_elem["data"])        
        
        # Add element to output message in SOGNO format
        sogno_message["readings"].append(sogno_elem)

    return sogno_message
