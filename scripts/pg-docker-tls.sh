#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 ROOT_DIR CONTAINER_NAME PORT MODE" >&2
  exit 2
fi

ROOT_DIR=$1
CONTAINER_NAME=$2
PORT=$3
MODE=$4

case "$MODE" in
  plain|tls|mtls-required) ;;
  *)
    echo "invalid mode: $MODE" >&2
    exit 2
    ;;
esac

TLS_DIR="$ROOT_DIR/tls"
CONF_DIR="$ROOT_DIR/config"
INIT_DIR="$ROOT_DIR/initdb"
rm -rf "$ROOT_DIR"
mkdir -p "$TLS_DIR" "$CONF_DIR" "$INIT_DIR"

cat > "$TLS_DIR/server-ext.cnf" <<'CNF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth
CNF

cat > "$TLS_DIR/client-ext.cnf" <<'CNF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
CNF

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$TLS_DIR/ca.key" \
  -out "$TLS_DIR/ca.crt" \
  -days 365 \
  -subj '/CN=zpg test ca' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -addext 'subjectKeyIdentifier=hash' >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
  -keyout "$TLS_DIR/server.key" \
  -out "$TLS_DIR/server.csr" \
  -subj '/CN=localhost' >/dev/null 2>&1
openssl x509 -req \
  -in "$TLS_DIR/server.csr" \
  -CA "$TLS_DIR/ca.crt" \
  -CAkey "$TLS_DIR/ca.key" \
  -CAcreateserial \
  -out "$TLS_DIR/server.crt" \
  -days 365 \
  -extfile "$TLS_DIR/server-ext.cnf" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
  -keyout "$TLS_DIR/client.key" \
  -out "$TLS_DIR/client.csr" \
  -subj '/CN=postgres' >/dev/null 2>&1
openssl x509 -req \
  -in "$TLS_DIR/client.csr" \
  -CA "$TLS_DIR/ca.crt" \
  -CAkey "$TLS_DIR/ca.key" \
  -CAcreateserial \
  -out "$TLS_DIR/client.crt" \
  -days 365 \
  -extfile "$TLS_DIR/client-ext.cnf" >/dev/null 2>&1

# The init script runs inside the container as the `postgres` user. Make the
# source keys world-readable so it can copy them out of the read-only bind
# mount, then lock permissions down again after copying into $PGDATA.
chmod 644 "$TLS_DIR/server.key" "$TLS_DIR/client.key"

if [ "$MODE" = "plain" ]; then
  cat > "$CONF_DIR/postgresql.conf" <<'CONF'
listen_addresses = '*'
ssl = off
logging_collector = off
log_connections = on
CONF
else
  cat > "$CONF_DIR/postgresql.conf" <<'CONF'
listen_addresses = '*'
ssl = on
# Zig std TLS on the current toolchain does not interoperate reliably with
# PostgreSQL/OpenSSL's TLS 1.3 post-handshake ticket flow, so pin the Docker
# integration environment to TLS 1.2 for deterministic coverage.
ssl_min_protocol_version = 'TLSv1.2'
ssl_max_protocol_version = 'TLSv1.2'
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
ssl_ca_file = 'root.crt'
logging_collector = off
log_connections = on
CONF
fi

if [ "$MODE" = "plain" ]; then
  cat > "$CONF_DIR/pg_hba.conf" <<'CONF'
local   all             all                                     trust
host    all             all             0.0.0.0/0               trust
host    all             all             ::/0                    trust
CONF
elif [ "$MODE" = "tls" ]; then
  cat > "$CONF_DIR/pg_hba.conf" <<'CONF'
local   all             all                                     trust
hostnossl all           all             0.0.0.0/0               reject
hostnossl all           all             ::/0                    reject
hostssl all             all             0.0.0.0/0               trust
hostssl all             all             ::/0                    trust
CONF
else
  cat > "$CONF_DIR/pg_hba.conf" <<'CONF'
local   all             all                                     trust
hostnossl all           all             0.0.0.0/0               reject
hostnossl all           all             ::/0                    reject
hostssl all             all             0.0.0.0/0               cert clientcert=verify-full
hostssl all             all             ::/0                    cert clientcert=verify-full
CONF
fi

cat > "$INIT_DIR/10-setup-ssl.sh" <<'SH'
#!/bin/sh
set -eu
cp /tls/ca.crt "$PGDATA/root.crt"
cp /tls/server.crt "$PGDATA/server.crt"
cp /tls/server.key "$PGDATA/server.key"
chmod 600 "$PGDATA/server.key"
cp /config/postgresql.conf "$PGDATA/postgresql.conf"
cp /config/pg_hba.conf "$PGDATA/pg_hba.conf"
SH
chmod 755 "$INIT_DIR/10-setup-ssl.sh"

cat > "$ROOT_DIR/README.txt" <<TXT
mode=$MODE
port=$PORT
ca_cert=$TLS_DIR/ca.crt
client_cert=$TLS_DIR/client.crt
client_key=$TLS_DIR/client.key
TXT

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
exec docker run --rm -d \
  --name "$CONTAINER_NAME" \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p "127.0.0.1:${PORT}:5432" \
  -v "$TLS_DIR:/tls:ro" \
  -v "$CONF_DIR:/config:ro" \
  -v "$INIT_DIR:/docker-entrypoint-initdb.d:ro" \
  postgres:16-alpine \
  -c config_file=/var/lib/postgresql/data/postgresql.conf \
  -c hba_file=/var/lib/postgresql/data/pg_hba.conf
