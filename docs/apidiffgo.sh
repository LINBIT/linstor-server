#!/bin/bash

APIYAML=rest_v1_openapi.yaml
# the openapi generator writes some header. It looks like this has a static number of lines.
NRHDR=10

apigit=docs/$APIYAML
dstdir=godst

die() {
	>&2 echo "$1"
	exit 1
}

help() {
cat <<EOF
Usage:
  $(basename "$0") v1-git-treeish v2-git-treeish
EOF
exit 0
}

gen_go() {
	local d=$1
	(
		cd "$d" || die "Could not cd to $d"
		docker run --rm -v "${PWD}":/local \
			openapitools/openapi-generator-cli generate -i /local/$APIYAML --skip-validate-spec -g go -o /local/gosrc >/dev/null
		mkdir "$dstdir"
		cd gosrc || die "Could not cd to gosrc"
		for f in model_*.go; do
			tail -n+$NRHDR "$f" > "../${dstdir}/${f}"
		done
	) || die "Subshell did not terminate successfully"
}

[ $# -ne 2 ] && help

v1="$1"; v2="$2"

v1_dir=$(mktemp -d --suffix "$v1"); v2_dir=$(mktemp -d --suffix "$v2")

git show "${v1}:${apigit}" > "${v1_dir}/${APIYAML}"
git show "${v2}:${apigit}" > "${v2_dir}/${APIYAML}"

gen_go "$v1_dir"
gen_go "$v2_dir"

d1="${v1_dir}/${dstdir}"; d2="${v2_dir}/${dstdir}"

echo "diff -ruN $d1 $d2"
diff -ruN "$d1" "$d2"

echo "Feel free to rm -rf $v1_dir $v2_dir"
