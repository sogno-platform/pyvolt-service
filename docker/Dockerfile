FROM python:3.7

LABEL name=pyvolt-service
LABEL version=1.0
LABEL description='pyvolt-service'
LABEL maintainer_email='acs-software@eonerc.rwth-aachen.de'

RUN pip3 install paho-mqtt==1.4.0
RUN pip3 install git+https://github.com/cim-iec/cimpy.git
RUN pip3 install git+https://github.com/RWTH-ACS/pyvolt.git

ADD . /se
WORKDIR /se

RUN git clone https://github.com/RWTH-ACS/pyvolt.git

#ENTRYPOINT [ "python", "./examples/quickstart/run_state_estimation_service_villas_interface.py"]