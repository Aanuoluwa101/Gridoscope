openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_tf_key.p8 -nocrypt
openssl rsa -in snowflake_tf_key.p8 -pubout -out snowflake_tf_key.pub
cat ~/.ssh/snowflake_tf_key.pub

ALTER USER "AEA" SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...<PASTE_KEY_HERE>...';
