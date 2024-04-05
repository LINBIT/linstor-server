#!/bin/sh

DEF_LOCATION="/usr/share/linstor-server"
# DEF_USER="linstor"
# DEF_PWD="linstor"
DEF_VARLIB_LINSTOR="/var/lib/linstor"
DEF_DB="${DEF_VARLIB_LINSTOR}/linstordb"
DEF_LINSTOR_CFG_DIR="/etc/linstor"
DEF_LINSTOR_CFG="${DEF_LINSTOR_CFG_DIR}/linstor.toml"
DEF_DB_TYPE="h2"

[ ! -d ${DEF_LINSTOR_CFG_DIR} ] && mkdir "$DEF_LINSTOR_CFG_DIR"
[ ! -d ${DEF_VARLIB_LINSTOR} ] && mkdir -m 750 "$DEF_VARLIB_LINSTOR"

# always create a backup of the current DB
CURRENT_DB=${DEF_DB}.mv.db
[ -f "$CURRENT_DB" ] && cp "$CURRENT_DB" "${CURRENT_DB}-$(date --iso-8601=minutes).bak"

[ ! -f ${DEF_LINSTOR_CFG} ] && ${DEF_LOCATION}/bin/linstor-config create-db-file --dbtype=${DEF_DB_TYPE} ${DEF_DB} > ${DEF_LINSTOR_CFG}

# DEBHELPER will be replaced by debian build system, adding systemd helper scripts and so on...
#DEBHELPER#

exit 0
