#!/bin/sh

try_import_key() {
  indir=$1
  [ -d "$indir" ] || return 0
  destkeystore=$2
  destcrtstore=$3
  tmpfile=$(mktemp)

  openssl pkcs12 -export -in "${indir}/tls.crt" -inkey "${indir}/tls.key" -out "$tmpfile" -name linstor -passin 'pass:linstor' -passout 'pass:linstor'
  keytool -importkeystore -srcstorepass linstor -deststorepass linstor -keypass linstor -srckeystore "$tmpfile" -destkeystore "$destkeystore"
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
		shift
		/usr/share/linstor-server/bin/Satellite --logs=/var/log/linstor-satellite --config-directory=/etc/linstor --skip-hostname-check "$@"
		;;
	startController)
		shift
		/usr/share/linstor-server/bin/Controller --logs=/var/log/linstor-controller --config-directory=/etc/linstor --rest-bind=0.0.0.0:3370 "$@"
		;;
	*) linstor "$@" ;;
esac
