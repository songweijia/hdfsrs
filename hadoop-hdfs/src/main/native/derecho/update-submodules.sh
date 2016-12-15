#!/bin/bash
#Shortcut to update both submodules to their latest version

git submodule foreach git pull origin master
git add rdmc sst
