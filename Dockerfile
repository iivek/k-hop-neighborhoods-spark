FROM python:3.8

WORKDIR /app

RUN apt-get update \
    && apt-get -y install openjdk-11-jdk
RUN curl -sSL https://install.python-poetry.org | python3 - \
 && ln -s ~/.local/bin/poetry /usr/bin/poetry

# Install project dependencies
ADD pyproject.toml .
RUN poetry install
