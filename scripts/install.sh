#!/usr/bin/env bash
set -u
set -e

export PATH=$PATH:$PWD
export VAULT_ADDR=https://thinktopic.com:8200

# vault
wget https://releases.hashicorp.com/vault/0.6.1/vault_0.6.1_linux_amd64.zip -O vault.zip
unzip vault.zip
openssl aes-256-cbc -K $encrypted_6569357b0c98_key -iv $encrypted_6569357b0c98_iv -in .vault-token.enc -out /home/travis/.vault-token -d

license_path=secret/thinktopic/datomic-license

export DATOMIC_USERNAME=$(vault read -field=DATOMIC_USERNAME $license_path)
export DATOMIC_PASSWORD=$(vault read -field=DATOMIC_PASSWORD $license_path)

source scripts/core-access
lein with-profile test deps

cat /home/travis/build.sh
