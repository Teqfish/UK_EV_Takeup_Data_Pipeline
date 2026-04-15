FROM kestra/kestra:latest

USER root

#### === INSTALL PYTHON 3.13 + TOOLS === ####
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    python3 \
    python3-pip \
    python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#### === INSTALL UV === ####
RUN pip3 install uv

USER kestra
