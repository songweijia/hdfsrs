#!/bin/bash
hadoop jar FileTester.jar readspeed /`hostname`0 &
hadoop jar FileTester.jar readspeed /`hostname`1 &
hadoop jar FileTester.jar readspeed /`hostname`2 
