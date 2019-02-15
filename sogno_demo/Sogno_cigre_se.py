#!/usr/bin/env python

################
# Dependancies:
# paho, mqtt library for python (sudo pip install paho-mqtt)
import sys
import paho.mqtt.client as mqtt
import json
import numpy as np
import time
from acs.state_estimation.network import System, Ymatrix_calc
from acs.state_estimation.nv_state_estimator_cim import DsseCall
from acs.state_estimation.nv_state_estimator_cim import Complex_to_all
from acs.state_estimation.nv_state_estimator_cim import Real_to_all
from acs.state_estimation.measurement_generator_online import Zdata_structure_creation, Zdatameas_creation_fromPF
import cimpy
import mqtt_client

Sb = 25e6
Vb1 = 110e3
Vb2 = 20e3
Zb = (Vb2**2)/Sb

def on_message(mqttc, userdata, msg):
    message = json.loads(msg.payload)[0]
    values = message["data"]
    values_se = np.zeros(len(values))
    Vmag_est = np.zeros(len(system.nodes))
    Vphase_est = np.zeros(len(system.nodes))
    Vmag_true = np.zeros(len(system.nodes))
    for elem in range(len(system.nodes)):
        if int(system.nodes[elem].uuid[1:]) == 0:
            values_se[elem*2] = values[int(system.nodes[elem].uuid[1:])*2]/Vb1
        else:
            values_se[elem * 2] = values[int(system.nodes[elem].uuid[1:])*2]/Vb2
        values_se[elem*2+1] = values[int(system.nodes[elem].uuid[1:])*2+1]

    if message["sequence"] > 0:

        # To delete later
        nodes_num = len(system.nodes)
        branches_num = len(system.branches)
        Vt = np.zeros(len(system.nodes), dtype=np.complex)
        index = np.linspace(0, len(values) - 2, int(len(values) / 2)).astype(int)
        for elem in range(len(system.nodes)):
            Vt[elem] = values_se[index[elem]]*(np.cos(values_se[index[elem]+1]) + 1j*np.sin(values_se[index[elem]+1]))
        Vtrue = Complex_to_all(Vt)

        """ From here on, all the other quantities of the grid are calculated """
        Irx = np.zeros(branches_num, dtype=np.complex)
        for index in range(branches_num):
            fr = system.branches[index].start_node.index  # branch.start[idx]-1
            to = system.branches[index].end_node.index  # branch.end[idx]-1
            Irx[index] = - (Vtrue.complex[fr] - Vtrue.complex[to]) * Ymatr[fr][to]
        Ir = np.real(Irx)
        Ix = np.imag(Irx)

        Itrue = Real_to_all(Ir, Ix)
        Iinj_r = np.zeros(nodes_num)
        Iinj_x = np.zeros(nodes_num)
        for k in range(nodes_num):
            to = []
            fr = []
            for m in range(branches_num):
                if k == system.branches[m].start_node.index:
                    fr.append(m)
                if k == system.branches[m].end_node.index:
                    to.append(m)

            Iinj_r[k] = np.sum(Itrue.real[to]) - np.sum(Itrue.real[fr])
            Iinj_x[k] = np.sum(Itrue.imag[to]) - np.sum(Itrue.imag[fr])

        Iinjtrue = Real_to_all(Iinj_r, Iinj_x)
        Sinj_rx = np.multiply(Vtrue.complex, np.conj(Iinjtrue.complex))
        Sinjtrue = Real_to_all(np.real(Sinj_rx), np.imag(Sinj_rx))
        values_se = np.append(values_se, Sinjtrue.real)
        values_se = np.append(values_se, Sinjtrue.imag)

        # till here

        if message["sequence"] <60:
            zdatameas = Zdatameas_creation_fromPF(meas_config1, zdata1, values_se, message["sequence"])
        else:
            zdatameas = Zdatameas_creation_fromPF(meas_config2, zdata2, values_se, message["sequence"])

        Vest, Iest, Iinjest, S1est, S2est, Sinjest = DsseCall(system, zdatameas, Ymatr, Adj)

        for elem in range(len(system.nodes)):
            Vmag_est[int(system.nodes[elem].uuid[1:])] = Vest.mag[elem]
            Vphase_est[int(system.nodes[elem].uuid[1:])] = Vest.phase[elem]

        index = np.linspace(0, len(values) - 2, int(len(values) / 2)).astype(int)

        for elem in range(len(index)):
            Vmag_true[elem] = values[index[elem]]
            Vmag_est[elem] = Vmag_est[elem]*Vb2
            if elem == 0:
                Vmag_est[elem] = Vmag_est[elem]*Vb1/Vb2

        Vmag_err = np.absolute(np.subtract(Vmag_est,Vmag_true))
        Vmag_err =  100*np.divide(Vmag_err,Vmag_true)
        max_err = np.amax(Vmag_err)
        mean_err = np.mean(Vmag_err)

        payload["ts"]["origin"] = message["ts"]["origin"]
        payload["sequence"] = message["sequence"]
        payload["data"] = np.append(values, values)
        payload["data"] = np.append(payload["data"], [max_err, mean_err])
        payload["data"][index] = Vmag_est
        payload["data"][index + 1] = Vphase_est
        payload["data"] = list(payload["data"])

        mqttc.publish("sogno-estimator", "[" + json.dumps(payload) + "]", 0)


def on_log(mqttc, userdata, level, buf):
    print("log: " + buf)


def on_connect(mqttc, userdata, flags, rc):
    if rc == 0:
        mqttc.connected_flag = False  # set flag
        print("Publisher connection OK")
    else:
        print("Bad connection, error code= ", rc)


def connect_subscribe(topic):
    mqttc = mqtt.Client("SognoSE", True)
    mqttc.username_pw_set("villas", "s3c0sim4!")
    mqttc.on_log = on_log
    mqttc.on_connect = on_connect

    mqttc.connect("137.226.248.91")

    mqttc.loop_start()  # Start loop
    time.sleep(1)  # Wait for connection setup to complete
    mqttc.subscribe(topic)


xml_files = [
    r"..\cim-grid-data\CIGRE_MV\CIGRE_MV_no_tapchanger_With_LoadFlow_Results\Rootnet_FULL_NE_06J16h_EQ.xml",
    r"..\cim-grid-data\CIGRE_MV\CIGRE_MV_no_tapchanger_With_LoadFlow_Results\Rootnet_FULL_NE_06J16h_SV.xml",
    r"..\cim-grid-data\CIGRE_MV\CIGRE_MV_no_tapchanger_With_LoadFlow_Results\Rootnet_FULL_NE_06J16h_TP.xml"]

res = cimpy.cimread(xml_files)
cimpy.setNodes(res)
cimpy.setPowerTransformerEnd(res)
system = System()
system.load_cim_data(res)
system.branches[12].y = system.branches[12].y*(Vb1**2)/(Vb2**2)
system.branches[13].y = system.branches[13].y*(Vb1**2)/(Vb2**2)
Ymatrix, Adj = Ymatrix_calc(system)
Ymatr = Ymatrix*Zb

with open('Payload_config.json') as f1:
    payload = json.load(f1)

with open('Measurement_config2.json') as f2:
    meas_config1 = json.load(f2)

with open('Measurement_config3.json') as f3:
    meas_config2 = json.load(f3)

zdata1 = Zdata_structure_creation(meas_config1, system)
zdata2 = Zdata_structure_creation(meas_config2, system)

mqttc = mqtt_client.connect_subscribe("dpsim-powerflow")
mqttc.on_message = on_message

input("Press enter to stop client...")

mqttc.loop_stop()
mqttc.disconnect()
