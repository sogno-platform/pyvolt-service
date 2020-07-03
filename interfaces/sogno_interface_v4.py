from json import dumps, loads
from datetime import datetime, timedelta, timezone
from pyvolt.measurement import MeasType

import numpy as np
from pyvolt.results import Results


def receiveSognoInput(message, measurement_set, phase='A'):
    """
    to store the received data in an object of type pyvolt.measurement

    """

    map_measurand_to_meastype = {   "voltmagnitude": MeasType.Vpmu_mag,
                                    "voltangle": MeasType.Vpmu_phase,
                                    "currmagnitude": MeasType.Ipmu_mag,
                                    "currangle": MeasType.Ipmu_phase,
                                    "activepower": MeasType.S1_real,
                                    "reactivepower": MeasType.S1_imag,
                                    "apparentpower": None,
                                    "frequency": None}

    print("SOGNO interface:")
    for reading in message['readings']:
        if reading['phase'] == phase:
            print("Received measurement value: {}, {}, {}, {}, {}, {}".format(message['timestamp'], message['device'], reading["component"], reading['measurand'], reading['phase'], reading['data']))
            measurement_set.update_measurement(reading["component"], map_measurand_to_meastype[reading['measurand']], reading['data'], False)


def sendSognoOutput(client, topic_publish, state_estimation_results, phase='A'):
    """
    Creates the payload according to sogno_output_v4.json
    Includes magnitude conversion from SE voltage in phase-to-phase to SOGNO value in phase-to-ground
    Incldues unit conversion from SE results in kV, kA and MW to SOGNO value in V, A and W
    @param client: MQTT client instance to be used for publishing
    @param topic_publish: topic used for publishing
    @param state_estimation_results: results of state_estimation (type pyvolt.results.Results)
    """

    # create current timestamp in ISO 8601 format
    timestamp_sogno = datetime.now(timezone.utc).isoformat()

    # publish node results
    for node in state_estimation_results.nodes:
        node_id = node.topology_node.uuid

        SognoOutput = {}
        SognoOutput["component"] = node_id
        SognoOutput["timestamp"] = timestamp_sogno
        SognoOutput["readings"] = []
              
        # add node voltage magnitude
        read_elem = {}
        read_elem["measurand"] = "voltmagnitude"
        read_elem["phase"] = phase
        read_elem["data"] = np.absolute(node.voltage/np.sqrt(3)*1e3)
        SognoOutput["readings"].append(read_elem)
        
        # add node voltage angle
        read_elem = {}
        read_elem["measurand"] = "voltangle"
        read_elem["phase"] = phase
        read_elem["data"] = np.angle(node.voltage)
        SognoOutput["readings"].append(read_elem)

        # add node active power 
        read_elem = {}
        read_elem["measurand"] = "activepower"
        read_elem["phase"] = phase
        read_elem["data"] = np.real(node.power)*1e6
        SognoOutput["readings"].append(read_elem)

        # add node reactive power 
        read_elem = {}
        read_elem["measurand"] = "reactivepower"
        read_elem["phase"] = phase
        read_elem["data"] = np.imag(node.power)*1e6
        SognoOutput["readings"].append(read_elem)

        # publish message
        client.publish(topic_publish, dumps(SognoOutput), 1)
    
    # publish line results
    for branch in state_estimation_results.branches:
        branch_id = branch.topology_branch.uuid

        SognoOutput = {}
        SognoOutput["component"] = branch_id
        SognoOutput["timestamp"] = timestamp_sogno
        SognoOutput["readings"] = []

        # add current magnitude
        read_elem = {}
        read_elem["measurand"] = "currmagnitude"
        read_elem["phase"] = phase
        read_elem["data"] = np.absolute(branch.current*1e3)
        SognoOutput["readings"].append(read_elem)

        # add current angle
        read_elem = {}
        read_elem["measurand"] = "currangle"
        read_elem["phase"] = phase
        read_elem["data"] = np.angle(branch.current)
        SognoOutput["readings"].append(read_elem)

        # publish message
        client.publish(topic_publish, dumps(SognoOutput), 1)