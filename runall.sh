#!/bin/bash

RESs=(10min 06min 03min 01min)

for RES in ${RESs[@]}
do
	export STNdomain=Asia
	export STNres=$RES
	scons -i
done
