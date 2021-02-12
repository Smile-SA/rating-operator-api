#!/bin/sh

set -eu

. ./config.sh

ipaddr=0.0.0.0
# ipaddr=$(hostname --ip-address)

export FLASK_APP=rating_operator.api.app:create_app
export FLASK_ENV=development

echo ./venv/bin/flask run --host "${ipaddr}" --port 5042
./venv/bin/python -m flask run --host "${ipaddr}" --port 5042
