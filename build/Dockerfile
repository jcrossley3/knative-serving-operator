FROM registry.access.redhat.com/ubi7-dev-preview/ubi-minimal:7.6

ARG version

ENV OPERATOR=/usr/local/bin/knative-serving-operator \
    USER_UID=1001 \
    USER_NAME=knative-serving-operator \
    KNATIVE_SERVING_VERSION=$version

# install operator binary
COPY build/_output/bin/knative-serving-operator ${OPERATOR}

COPY build/bin /usr/local/bin
RUN  /usr/local/bin/user_setup

# install knative-serving manifest
COPY deploy/knative-serving-$version.yaml /tmp/knative-serving.yaml

ENTRYPOINT ["/usr/local/bin/entrypoint"]

USER ${USER_UID}
