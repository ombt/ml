#!/bin/bash -x
#
i=1
imax=${1:-5}
#
cd /opt/intel/openvino/deployment_tools/demo
#
while [ $i -le $imax ]
do
	echo "run  ... $i "
	#
	time ./demo_security_barrier_camera.sh -d CPU 2>&1
	i=$((i+1))
done | 
tee mem.test.cpu.out
#
exit 0
