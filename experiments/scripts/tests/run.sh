#!/bin/bash
./scale_read_write.sh

rm ../deploy/cfg/clients
cp ../deploy/cfg/clients.1 ../deploy/cfg/clients
rm ../deploy/cfg/slaves
cp ../deploy/cfg/slaves.1 ../deploy/cfg/slaves

./seq_read_write.sh
