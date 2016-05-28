#!/bin/bash
dir=$1
scp -r -i qi.pem ubuntu@128.84.105.162:~/waterwave/$dir $dir
