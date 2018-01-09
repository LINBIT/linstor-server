#!/bin/sh

DEF_LOCATION="/opt/linstor-server"
DEF_USER="linstor"
DEF_PWD="linstor"
DEF_DB="${DEF_LOCATION}/linstordb" # this is actually a directory
DEF_DB_CFG="${DEF_LOCATION}/database.cfg"

[ -d ${DEF_DB} ] && { echo "Database already created, exiting"; exit 0; }

cat <<EOF >${DEF_DB_CFG}
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
 <!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
 <properties>
	 <comment>LinStor default database configuration</comment>
	 <entry key="user">${DEF_USER}</entry>
	 <entry key="password">${DEF_PWD}</entry>
	 <entry key="connection-url">jdbc:derby:${DEF_DB};create=true</entry>
 </properties>
EOF

${DEF_LOCATION}/bin/RecreateDb ${DEF_DB_CFG} && echo "Database created"
