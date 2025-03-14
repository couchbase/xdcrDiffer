ARG BUILDER_IMAGE=docker-registry-proxy.corp.amdocs.com/golang:1.24

ARG FINAL_IMAGE=docker-registry-proxy.corp.amdocs.com/redhat/ubi9:9.5

ARG http_proxy
ARG https_proxy
ARG no_proxy


FROM $BUILDER_IMAGE as builder

ENV http_proxy=${http_proxy}
ENV https_proxy=${https_proxy}
ENV no_proxy=${no_proxy}


ENV LANG=en_US.utf8
ENV LC_ALL=en_US.utf8

ARG TARGETARCH
ARG TARGETOS

ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH
ENV CGO_ENABLED=0

COPY . .

RUN make all

RUN echo "xdcrdiffer:x:10001:10001::/:/xdcrDiffer" > /passwd

RUN go build -ldflags='-s -w -extldflags "-static"' -v \
    -o xdcrDiffer main.go

RUN chmod +x ./runDiffer.sh

FROM $FINAL_IMAGE as final

COPY --from=builder /go/xdcrDiffer /
COPY --from=builder /go/runDiffer.sh /
COPY --from=builder /passwd /etc/passwd

USER xdcrdiffer
ENTRYPOINT ["/runDiffer.sh"]
