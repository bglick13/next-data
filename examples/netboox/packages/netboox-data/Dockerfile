
# syntax=docker/dockerfile:1.4
FROM --platform=linux/amd64 public.ecr.aws/amazonlinux/amazonlinux:2023-minimal AS builder

RUN dnf install -y gcc python3 python3-devel
ENV VIRTUAL_ENV=/opt/venv

RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /requirements.txt
# copy the entire next-data project into the docker container and install
COPY ../../../../ /next-data/
WORKDIR /next-data
# Debug: List contents of /next-data directory
RUN ls -la /next-data &&     ls -la / &&     pwd
RUN python3 -m pip install -e .
RUN python3 -m pip install -r /requirements.txt

RUN mkdir /output && venv-pack -o /output/venv.tar.gz

FROM scratch
COPY --from=builder /output/venv.tar.gz /
