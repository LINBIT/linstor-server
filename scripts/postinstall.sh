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

# migrate old databases locations
# remove after linstor-server 0.2.5
OLD_DB_LOCATION="/opt/linstor-server/linstordb.mv.db"
[ -f ${OLD_DB_LOCATION} ] && {
    mv "$OLD_DB_LOCATION" "$DEF_VARLIB_LINSTOR";
    echo "Moved old database to ${DEF_VARLIB_LINSTOR}";
    echo "Please review the new configuration file: ${DEF_DB_CFG}";
}

[ -f ${DEF_DB_CFG} ] && { echo "Database config already created, exiting"; exit 0; }

${DEF_LOCATION}/bin/linstor-config create-db-file --dbtype=${DEF_DB_TYPE} ${DEF_DB} > ${DEF_DB_CFG}
