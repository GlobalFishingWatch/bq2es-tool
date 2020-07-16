FROM golang:1.14-alpine as development

ENV CGO_ENABLED=1
ENV APP_NAME=bigquery-to-elasticsearch-tool

WORKDIR /go/src/app

RUN apk update && apk add git
RUN go get github.com/cespare/reflex
COPY . .
RUN go build -o $APP_NAME main.go

CMD ["reflex", "-c", "./reflex.conf"]

FROM alpine AS build
COPY --from=development /go/src/app/$APP_NAME /opt/$APP_NAME
ENTRYPOINT ["/opt/$APP_NAME"]