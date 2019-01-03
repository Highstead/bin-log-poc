FROM golang:1.11-alpine
RUN apk update && apk add \
    build-base \
    ca-certificates \
    curl \
    git \
    geoip \
    geoip-dev \
    openssh

# Setup to clone private repositories.
# See https://apidocs.shopify.io/shopify-build/pipelines/building_containers_with_pipa/#ssh-keys
ARG GH_HOST_KEY
RUN mkdir -p ~/.ssh && echo ${GH_HOST_KEY} >> ~/.ssh/known_hosts
RUN curl https://storage.googleapis.com/k8s-utils/pipa-public/docker-ssh-exec-v1.0-shopify.tar.gz | tar -xz -C /usr/local/bin
RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

ENV GO111MODULE=on
COPY . /go/src/github.com/Shopify/reportify-streams
WORKDIR /go/src/github.com/Shopify/reportify-streams
RUN curl https://s3.amazonaws.com/starscream-dependencies/GeoIPCity.dat.gz | gunzip > config/GeoIPCity.dat
RUN docker-ssh-exec go get \
    github.com/Shopify/ejson/cmd/ejson

RUN docker-ssh-exec go install github.com/Shopify/reportify-streams/cmd/streams

ENTRYPOINT [ "script/entrypoint", "/go/bin/streams" ]

