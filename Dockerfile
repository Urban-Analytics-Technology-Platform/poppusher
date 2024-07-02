# Instructions:
#
#    docker build -t popgetter-prod . --build-arg "SAS_TOKEN=$SAS_TOKEN"
#    docker run -it popgetter-prod
#
# Note the double quotes around the SAS_TOKEN is important because it contains
# ampersands, which the shell will interpret.

FROM python:3.12-slim

# System dependencies for Rustup + compilation of fiona from source
RUN apt-get update && apt-get install -y curl libgdal-dev g++
# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy files over.
WORKDIR /popgetter-prod
COPY ./pyproject.toml ./pyproject.toml
COPY ./workspace.yml ./workspace.yml
COPY ./python ./python
# These are not really needed, but can't build without them
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src
COPY ./README.md ./README.md

# Install popgetter library
RUN python -m pip install --upgrade pip setuptools \
    && python -m pip install .

# TODO: Not the safest!
ARG SAS_TOKEN
RUN [ -z "$SAS_TOKEN" ] && echo "SAS_TOKEN build arg is required" && exit 1 || true
ENV SAS_TOKEN=$SAS_TOKEN

ARG POPGETTER_COUNTRIES
ENV POPGETTER_COUNTRIES=$POPGETTER_COUNTRIES

COPY ./deploy.sh ./deploy.sh
ENTRYPOINT ["./deploy.sh"]
