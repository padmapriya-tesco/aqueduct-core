#!/usr/bin/env bash

#server=$(printf "%q" ${SERVER:-$1})
#database=$(printf "%q" ${DATABASE:-$2})
#username=$(printf "%q" ${USERNAME:-$3})
#password=$(printf "%q" ${PASSWORD:-$4})
#pipe_url=$(printf "%q" ${PIPE_URL:-$5})
#pipe_read_security_enabled=$(printf "%q" ${PIPE_READ_SECURITY_ENABLED:-$6})
pipe_read_username=$(printf "%q" ${PIPE_READ_USERNAME:-$7})
#pipe_read_password=$(printf "%q" ${PIPE_READ_PASSWORD:-$8})
runscope_pipe_read_username=$(printf "%q" ${RUNSCOPE_PIPE_READ_USERNAME:-$9})
#runscope_pipe_read_password=$(printf "%q" ${RUNSCOPE_PIPE_READ_PASSWORD:-${10}})
support_username=$(printf "%q" ${SUPPORT_USERNAME:-${11}})
#support_password=$(printf "%q" ${SUPPORT_PASSWORD:-${12}})
#till_client_uid=$(printf "%q" ${TILL_CLIENT_UID:-${13}})
#identity_url=$(printf "%q" ${IDENTITY_URL:-${14}})
#identity_validate_token_path=$(printf "%q" ${IDENTITY_VALIDATE_TOKEN_PATH:-${15}})

sed -i "s/{PIPE_READ_USERNAME}/$pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{RUNSCOPE_PIPE_READ_USERNAME}/$runscope_pipe_read_username/" /etc/aqueduct/pipe/application.yml
sed -i "s/{SUPPORT_USERNAME}/$support_username/" /etc/aqueduct/pipe/application.yml

exec java \
    -Dmicronaut.config.files=/etc/aqueduct/pipe/application.yml \
    -jar $(ls -1 /opt/aqueduct/pipe/pipe-http-server-cloud-*all.jar | tail -n 1)
