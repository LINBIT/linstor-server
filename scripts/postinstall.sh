#!/bin/sh

DEF_LOCATION="/opt/linstor-server"
# DEF_USER="linstor"
# DEF_PWD="linstor"
DEF_DB="${DEF_LOCATION}/linstordb" # this is actually a directory
DEF_DB_CFG="${DEF_LOCATION}/database.cfg"
DEF_DB_TYPE="h2"

[ -f ${DEF_DB_CFG} ] && { echo "Database config already created, exiting"; exit 0; }

${DEF_LOCATION}/bin/linstor-config create-db-file --dbtype=${DEF_DB_TYPE} ${DEF_DB} > ${DEF_DB_CFG}
