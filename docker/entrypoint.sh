#!/bin/sh

if [ -e /environment.sh ]; then
    echo "Sourcing /environment.sh"
    . /environment.sh
fi

set -eu

cd "${APP_PATH}"
flask run --host=0.0.0.0 --port=5012