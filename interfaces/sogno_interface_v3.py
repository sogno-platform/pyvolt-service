from json import dumps, loads
from datetime import datetime, timedelta
from acs.state_estimation.measurement import MeasType

import numpy as np
from acs.state_estimation.results import Results


def receiveSognoInput(message, measurement_set, phase='A'):
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

    print("SOGNO interface:")
    for reading in message['readings']:
        if reading['phase'] == phase:
            print("Received measurement value: {}, {}, {}, {}, {}".format(message['timestamp'], message['device'], reading['measurand'], reading['phase'], reading['data']))
            measurement_set.update_measurement(reading["component"], map_measurand_to_meastype[reading['measurand']], reading['data'], False)


def sendSognoOutput(client, topic_publish, state_estimation_results, phase='A'):
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
        # including conversion from SE value in phase-to-phase and kW
        # to SOGNO value in phase-to-ground and W
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