#!/bin/bash

set -eu

LB_WAIT_FOR_BACKUP=${LB_WAIT_FOR_BACKUP:-"true"}

create_backup_secret() {
  BACKUP_NAME="$1"
  BACKUP_FILE="$2"
  VERSION="$3"

  if ! kubectl create secret generic "${BACKUP_NAME}" --type piraeus.io/linstor-backup --from-file=backup.tar.gz="${BACKUP_FILE}"; then
    echo "Backup ${BACKUP_FILE} may be too large to fit into a single secret, trying to split into chunks" >&2
    split -d -b 512k "${BACKUP_FILE}" "${BACKUP_FILE}."
    for SPLIT_FILE in "${BACKUP_FILE}".* ; do
      kubectl create secret generic "${BACKUP_NAME}-${SPLIT_FILE##"${BACKUP_FILE}".}" --type piraeus.io/linstor-backup-part --from-file=backup.tar.gz="${SPLIT_FILE}"
      kubectl label secret "${BACKUP_NAME}-${SPLIT_FILE##"${BACKUP_FILE}".}" piraeus.io/backup="${BACKUP_NAME}"
    done

    kubectl create secret generic "${BACKUP_NAME}" --type piraeus.io/linstor-backup
  else
    kubectl label secret "${BACKUP_NAME}" piraeus.io/backup="${BACKUP_NAME}"
  fi

  kubectl annotate secrets "${BACKUP_NAME}" "piraeus.io/linstor-version=${VERSION}" "piraeus.io/backup-version=2"
}

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

    if ! create_backup_secret "${BACKUP_NAME}" backup.tar.gz "${VERSION}"; then
      cat <<EOF >&2
===============================================================================
Backup backup.tar.gz too large, even after chunking, to fit into secrets.
Please manually copy it to a safe location, by running:

  kubectl cp -c run-migration $(hostname):/run/migration/backup.tar.gz .

EOF
      if [ "${LB_WAIT_FOR_BACKUP}" != "true" ]; then
        return 1
      fi
      cat <<EOF >&2
This container will wait until downloading of the backup has been confirmed.
To confirm the backup has been stored locally, create a secret using:

  kubectl create secret generic ${BACKUP_NAME} --type piraeus.io/linstor-backup

EOF
      while ! kubectl get secrets "${BACKUP_NAME}" 2>/dev/null ; do
        sleep 10
      done
    fi
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
		LB_FORCE_NODE_NAME="${LB_FORCE_NODE_NAME:-}"
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
