#!/bin/sh

DEF_LOCATION="/usr/share/linstor-server"
# DEF_USER="linstor"
# DEF_PWD="linstor"
DEF_VARLIB_LINSTOR="/var/lib/linstor"
DEF_DB="${DEF_VARLIB_LINSTOR}/linstordb"
DEF_LINSTOR_CFG="/etc/linstor"
DEF_DB_CFG="${DEF_LINSTOR_CFG}/database.cfg"
DEF_DB_TYPE="h2"

[ ! -d ${DEF_LINSTOR_CFG} ] && mkdir "$DEF_LINSTOR_CFG"
[ ! -d ${DEF_VARLIB_LINSTOR} ] && mkdir "$DEF_VARLIB_LINSTOR"

# always create a backup of the current DB
CURRENT_DB=${DEF_DB}.mv.db
[ -f "$CURRENT_DB" ] && cp "$CURRENT_DB" "${CURRENT_DB}-$(date --iso-8601=minutes).bak"

[ -f ${DEF_DB_CFG} ] && { echo "Database config already created, exiting"; exit 0; }

${DEF_LOCATION}/bin/linstor-config create-db-file --dbtype=${DEF_DB_TYPE} ${DEF_DB} > ${DEF_DB_CFG}
