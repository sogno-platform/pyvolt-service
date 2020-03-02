from json import dumps, loads
from datetime import datetime, timedelta
from acs.state_estimation.measurement import MeasType

import numpy as np
from acs.state_estimation.results import Results


def receiveSognoInput(message, measurement_set, phase="A"):
    """
    to store the received data in an object of type acs.state_estimation.measurement

    """

    map_measurand_to_meastype = {   "voltmagnitude": MeasType.Vpmu_mag,
                                    "voltangle": MeasType.Vpmu_phase,
                                    "currmagnitude": MeasType.Ipmu_mag,
                                    "currangle": MeasType.Ipmu_phase,
                                    "activepower": MeasType.S1_real,
                                    "reactivepower": MeasType.S1_imag,
                                    "apparentpower": None,
                                    "frequency": None}

    for meas in message['readings']:
        if meas['phase'] == phase:
            print("Received measurement value: {}, {}, {}, {}, {}".format(message['timestamp'], message['device'], meas['measurand'], meas['phase'], meas['data']))
            measurement_set.update_measurement(meas["component"], map_measurand_to_meastype[meas['measurand']], meas['data'], False)


def sendSognoOutput(client, topic_publish, state_estimation_results, phase="A"):
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

        # publish message
        client.publish(topic_publish, dumps(SognoOutput), 0)

    # # publish line currents
    # for branch in state_estimation_results.branches:
    #     branch_id = branch.topology_branch.uuid

    #     SognoOutput = {}
    #     SognoOutput["comp_id"] = branch_id
    #     SognoOutput["timestamp"] = timestamp_sogno

    #     # publish node voltage magnitude
    #     SognoOutput["data"] = np.absolute(branch.current*1e3)
    #     SognoOutput["type"] = "curr_phs" + phase.lower() + "_abs"
    #     client.publish(topic_publish, dumps(SognoOutput), 0)

    #     # publish node voltage angle
    #     SognoOutput["data"] = np.angle(branch.current)
    #     SognoOutput["type"] = "curr_phs" + phase.lower() + "_angle"
    #     client.publish(topic_publish, dumps(SognoOutput), 0)