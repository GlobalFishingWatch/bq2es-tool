FROM golang:1.14-alpine as development

ENV CGO_ENABLED=1

WORKDIR /go/src/app

RUN apk update && apk add git
RUN go get github.com/cespare/reflex
COPY . .
RUN go build -o bq2es-tool main.go

CMD ["reflex", "-c", "./reflex.conf"]

FROM alpine AS build
WORKDIR /opt/
COPY --from=development /go/src/app/bq2es-tool bq2es-tool
ENTRYPOINT ["/opt/bq2es-tool"]