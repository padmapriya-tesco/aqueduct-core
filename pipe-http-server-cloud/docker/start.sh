#!/usr/bin/env bash

server=$(printf "%q" $1)
database=$(printf "%q" $2)
username=$(printf "%q" $3)
password=$(printf "%q" $4)
pipe_url=$(printf "%q" $5)
pipe_read_security_enabled=$(printf "%q" $6)
pipe_read_username=$(printf "%q" $7)
pipe_read_password=$(printf "%q" $8)
runscope_pipe_read_username=$(printf "%q" $9)
runscope_pipe_read_password=$(printf "%q" ${10})
support_username=$(printf "%q" ${11})
support_password=$(printf "%q" ${12})

if [[ "$pipe_read_security_enabled" = "''" ]]; then
    pipe_read_security_enabled="false"
fi

sed -i "s/{POSTGRE_SERVER}/$server/" /etc/aqueduct/pipe/application.yml
sed -i "s/{POSTGRE_DATABASE}/$database/" /etc/aqueduct/pipe/application.yml
sed -i "s/{POSTGRE_USERNAME}/$username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{POSTGRE_PASSWORD}/$password/" /etc/aqueduct/pipe/application.yml
sed -i "s|{PIPE_URL}|$pipe_url|" /etc/aqueduct/pipe/application.yml
sed -i "s/{PIPE_READ_SECURITY_ENABLED}/$pipe_read_security_enabled/" /etc/aqueduct/pipe/application.yml
sed -i "s/{PIPE_READ_USERNAME}/$pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{PIPE_READ_PASSWORD}/$pipe_read_password/" /etc/aqueduct/pipe/application.yml
sed -i "s/{RUNSCOPE_PIPE_READ_USERNAME}/$runscope_pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{RUNSCOPE_PIPE_READ_PASSWORD}/$runscope_pipe_read_password/" /etc/aqueduct/pipe/application.yml
sed -i "s/{SUPPORT_USERNAME}/$support_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{SUPPORT_PASSWORD}/$support_password/" /etc/aqueduct/pipe/application.yml

java \
    -Dmicronaut.config.files=/etc/aqueduct/pipe/application.yml \
    -jar $(ls -1 /opt/aqueduct/pipe/pipe-http-server-cloud-*all.jar | tail -n 1)
