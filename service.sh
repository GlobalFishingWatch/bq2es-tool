#!/bin/bash

case "$1" in

    develop)
      reflex -c ./reflex.conf
      ;;
    compile)
      go build -o bigquery-to-elasticsearch-tool main.go
      ;;
    *)
        echo "Usage: service.sh {develop|compile}" >&2
        exit 1
        ;;
esac

exit 0