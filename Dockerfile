FROM python:3-alpine

RUN apk add --no-cache libpq postgresql-client \
    alpine-sdk libffi-dev openssl-dev tmux

RUN apk add --no-cache --virtual .build-deps \
    gcc \
    python3-dev \
    musl-dev \
    postgresql-dev \
    && pip install --no-cache-dir psycopg2 \
    && apk del --no-cache .build-deps

RUN pip install \
    awscli \
    yacron \
    python-intercom \
    boto3 \
    argparse \
    requests \
    jsonschema \
    slack_sdk \
    pysftp cryptography==2.5


# For musl compatibility with go reporting binary
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2

WORKDIR /work
COPY data-pipelines pipelines

RUN chmod +x /work/syncer/dwh/*.sh

CMD ["yacron", "-c", "crontab.yml"]