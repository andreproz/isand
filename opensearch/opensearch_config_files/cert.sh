#!/bin/sh
# Создание папки certs, если её ещё нет
mkdir -p certs

# Root CA
openssl genrsa -out certs/root-ca-key.pem 2048
openssl req -new -x509 -sha256 -key certs/root-ca-key.pem -subj "/C=RU/ST=MOSCOW/L=MOSCOW/O=IPU/OU=ISAND/CN=root.dns.a-record" -out certs/root-ca.pem -days 36500

# Admin cert
openssl genrsa -out certs/admin-key-temp.pem 2048
openssl pkcs8 -inform PEM -outform PEM -in certs/admin-key-temp.pem -topk8 -nocrypt -v1 PBE-SHA1-3DES -out certs/admin-key.pem
openssl req -new -key certs/admin-key.pem -subj "/C=RU/ST=MOSCOW/L=MOSCOW/O=IPU/OU=ISAND/CN=A" -out certs/admin.csr
openssl x509 -req -in certs/admin.csr -CA certs/root-ca.pem -CAkey certs/root-ca-key.pem -CAcreateserial -sha256 -out certs/admin.pem -days 36500

# Node cert 1
openssl genrsa -out certs/node-key-temp.pem 2048
openssl pkcs8 -inform PEM -outform PEM -in certs/node-key-temp.pem -topk8 -nocrypt -v1 PBE-SHA1-3DES -out certs/node-key.pem
openssl req -new -key certs/node-key.pem -subj "/C=RU/ST=MOSCOW/L=MOSCOW/O=IPU/OU=ISAND/CN=node.dns.a-record" -out certs/node.csr
echo 'subjectAltName=DNS:node.dns.a-record' > certs/node.ext
openssl x509 -req -in certs/node.csr -CA certs/root-ca.pem -CAkey certs/root-ca-key.pem -CAcreateserial -sha256 -out certs/node.pem -days 36500 -extfile certs/node.ext

# Cleanup
rm certs/admin-key-temp.pem
rm certs/admin.csr
rm certs/node-key-temp.pem
rm certs/node.csr
rm certs/node.ext
