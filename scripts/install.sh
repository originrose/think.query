#!/usr/bin/env bash
set -u
set -e

license_path=secret/thinktopic/datomic-license

export DATOMIC_USERNAME=$(vault read -field=DATOMIC_USERNAME $license_path)
export DATOMIC_PASSWORD=$(vault read -field=DATOMIC_PASSWORD $license_path)

lein with-profile test deps

