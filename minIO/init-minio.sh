#!/bin/sh

minio server /data --console-address ":9001" & 

sleep 5

mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

if ! mc ls localminio/spotify-raw >/dev/null 2>&1; then
   mc mb local/spotify-raw
fi

if ! mc admin user info localminio App >/dev/null 2>&1; then
   mc admin user add local $MINIO_APP_USER $MINIO_APP_PASSWORD
fi

mc admin policy attach local readwrite --user $MINIO_APP_USER

wait