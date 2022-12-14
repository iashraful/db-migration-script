FROM python:3.10.6-alpine

RUN apk update && \
    apk add --virtual build-base gcc g++ python3-dev libressl-dev musl-dev libffi-dev && \
    apk add netcat-openbsd py-pip jpeg-dev zlib-dev

WORKDIR /api
COPY . /api/

RUN pip3 install --upgrade pip
RUN pip3 install -U pip poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi