#!/usr/bin/env bash
#
# Call /function endpoint of both graphite-web and carbonapi -> compare the results -> get diff

if [[ $# -ne 2 ]]; then
  echo "Usage: ./functions_diff.sh graphite-web-URL:PORT carbonapi-URL:PORT"
  exit 1
fi

gweb="$(curl -s --fail "$1/functions")" || { echo "ERROR: getting graphite-web function list failed"; exit 1; }
capi="$(curl -s --fail "$2/functions")" || { echo "ERROR: getting carbonapi function list failed"; exit 1; }
capi_funcs="$(echo "$capi" | jq '.[] | {name} | .name' | sort)" || { echo "ERROR: parsing carbonapi functions output failed"; exit 1; }
gweb_funcs="$(echo "$gweb" | jq '.[] | {name} | .name' | sort)" || { echo "ERROR: parsing graphite-web functions output failed"; exit 1; }

d="$(diff <(echo "$capi_funcs") <(echo "$gweb_funcs"))"

printf '\nIn graphite-web but not in carbonapi\n------------\n\n'
echo "${d}" | grep ">" | awk '{print $2}' | sed 's/\"//g'

printf '\nIn carbonapi but not in graphite-web\n------------\n\n'
echo "${d}" | grep "<" | awk '{print $2}' | sed 's/\"//g'
