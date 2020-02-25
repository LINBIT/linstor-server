#!/bin/sh

case $1 in
	startSatellite)
		shift
		/usr/share/linstor-server/bin/Satellite --logs=/var/log/linstor-satellite --config-directory=/etc/linstor --skip-hostname-check "$@"
		;;
	startController)
		shift
		/usr/share/linstor-server/bin/Controller --logs=/var/log/linstor-controller --config-directory=/etc/linstor --rest-bind=0.0.0.0:3370 "$@"
		;;
	*) linstor "$@" ;;
esac
