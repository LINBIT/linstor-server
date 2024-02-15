#!/bin/sh
set -e

run_migration() {
  if /usr/share/linstor-server/bin/linstor-config all-migrations-applied --logs=/var/log/linstor-controller --config-directory=/etc/linstor "$@" ; then
    return 0
  fi

  VERSION=$(/usr/share/linstor-server/bin/linstor-config run-migration -v)

  # In k8s contexts, the host name will be the unique pod name, so we can assume
  # a unique backup name, which is still stable for a one specific attempted migration.
  BACKUP_NAME=${BACKUP_NAME:-linstor-backup-for-$(uname -n)}

  if ! kubectl get secrets "${BACKUP_NAME}"; then
    mkdir -p /run/migration
    cd /run/migration
    kubectl api-resources --api-group=internal.linstor.linbit.com -oname | xargs --no-run-if-empty kubectl get crds -oyaml > crds.yaml
    for CRD in $(kubectl api-resources --api-group=internal.linstor.linbit.com -oname); do
      kubectl get "${CRD}" -oyaml > "${CRD}.yaml"
    done
    tar -czvf backup.tar.gz -- *.yaml
    kubectl create secret generic "${BACKUP_NAME}" --type linstor.io/linstor-backup --from-file=backup.tar.gz
    kubectl annotate secrets "${BACKUP_NAME}" "linstor.io/linstor-version=${VERSION}"
  fi

  /usr/share/linstor-server/bin/linstor-config run-migration --config-directory=/etc/linstor "$@"
}

try_import_key() {
  indir=$1
  [ -d "$indir" ] || return 0
  destkeystore=$2
  destcrtstore=$3
  tmpfile=$(mktemp)

  rm -f "$destkeystore" "$destcrtstore"
  openssl pkcs12 -export -in "${indir}/tls.crt" -inkey "${indir}/tls.key" -out "$tmpfile" -name linstor -passin 'pass:linstor' -passout 'pass:linstor'
  keytool -importkeystore -noprompt -srcstorepass linstor -deststorepass linstor -keypass linstor -srckeystore "$tmpfile" -destkeystore "$destkeystore"
  keytool -importcert -noprompt -deststorepass linstor -keypass linstor -file "${indir}/ca.crt" -alias ca -destkeystore "$destcrtstore"
  rm -f "$tmpfile"
}

try_import_key /etc/linstor/ssl-pem /etc/linstor/ssl/keystore.jks /etc/linstor/ssl/certificates.jks
try_import_key /etc/linstor/https-pem /etc/linstor/https/keystore.jks /etc/linstor/https/truststore.jks

if mountpoint -q /host/proc; then
	lvmpath=$(command -v lvm)
	mv "$lvmpath" "${lvmpath}.distro"
	cat <<'EOF' > "$lvmpath"
#!/bin/sh
nsenter --mount=/host/proc/1/ns/mnt -- "$(basename $0)" "$@"
EOF
	chmod +x "$lvmpath"
fi

case $1 in
	startSatellite)
		declare -a EXEC_PREFIX
		if [ -n "$LB_FORCE_NODE_NAME" ]; then
			EXEC_PREFIX+=(unshare --uts -- sh -ec 'hostname "$LB_FORCE_NODE_NAME"; exec "$@"' --)
		fi

		shift
		exec "${EXEC_PREFIX[@]}" /usr/share/linstor-server/bin/Satellite --logs=/var/log/linstor-satellite --config-directory=/etc/linstor "$@"
		;;
	startController)
		shift
		exec /usr/share/linstor-server/bin/Controller --logs=/var/log/linstor-controller --config-directory=/etc/linstor "$@"
		;;
	runMigration)
		shift
		run_migration "$@"
		;;
	*) linstor "$@" ;;
esac
