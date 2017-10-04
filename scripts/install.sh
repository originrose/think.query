#!/usr/bin/env bash
set -e

export PATH=$PATH:$PWD
export VAULT_ADDR=https://thinktopic.com:8200

# vault
wget https://releases.hashicorp.com/vault/0.6.1/vault_0.6.1_linux_amd64.zip -O vault.zip
unzip vault.zip
vault auth -method=github token=$GITHUB_TOKEN

license_path=secret/thinktopic/datomic-license
export DATOMIC_USERNAME=$(vault read -field=DATOMIC_USERNAME $license_path)
export DATOMIC_PASSWORD=$(vault read -field=DATOMIC_PASSWORD $license_path)

source scripts/core-access
lein deps
