#!/bin/bash
#
# Initialized OpenMetadata Server startup script with DB + ES migrations enabled

echo "Initializing OpenMetadata Server...";
echo "Migrating the database to the latest version and the indexes in ElasticSearch...";
./bootstrap/bootstrap_storage.sh migrate-all

echo "    ||||||| "
echo "  ||||   ||||      ____ "
echo " ||||     ||||    / __ \ "
echo " ||||     ||||   | |  | | _ __    ___  _ __ "
echo " |||||   |||||   | |  | || '_ \  / _ \| '_ \ "
echo " |||||||||||||   | |__| || |_) ||  __/| | | | "
echo " |||||||||||||    \____/ | .__/  \___||_| |_| "
echo " ||| ||||| |||    __  __ | |    _              _         _ "
echo " |||  |||  |||   |  \/  ||_|   | |            | |       | | "
echo " |||   |   |||   | \  / |  ___ | |_  __ _   __| |  __ _ | |_  __ _ "
echo " |||       |||   | |\/| | / _ \| __|/ _\` | / _\` | / _\` || __|/ _\` | "
echo " ||| || || |||   | |  | ||  __/| |_| (_| || (_| || (_| || |_| (_| | "
echo " ||| ||||| |||   |_|  |_| \___| \__|\__,_| \__,_| \__,_| \__|\__,_| "
echo "  ||||||||||| "
echo "    ||||||| "
echo "Starting OpenMetadata Server"
exec ./bin/openmetadata-server-start.sh conf/openmetadata.yaml
