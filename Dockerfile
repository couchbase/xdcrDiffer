ARG BUILDER_IMAGE=docker.io/golang:1.23
ARG FINAL_IMAGE=docker.io/redhat/ubi9:9.4
ARG http_proxy
ARG https_proxy
ARG no_proxy

FROM $BUILDER_IMAGE as builder

ENV LANG=en_US.utf8
ENV LC_ALL=en_US.utf8

ARG TARGETARCH
ARG TARGETOS

ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH
ENV CGO_ENABLED=0

COPY . .

RUN echo "myuser:x:1001:1001::/:/xdcrDiffer" > /passwd

RUN go build -ldflags='-s -w -extldflags "-static"' -v \
    -o xdcrDiffer main.go

RUN chmod +x ./runDiffer.sh

FROM $FINAL_IMAGE as final

COPY --from=builder /go/xdcrDiffer /
COPY --from=builder /go/runDiffer.sh /
COPY --from=builder /passwd /etc/passwd

ENTRYPOINT ["/runDiffer.sh"]

