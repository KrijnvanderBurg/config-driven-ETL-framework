ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION} AS BASE

ENV \
    # Poetry
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    # Pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # Poetry
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_HOME='/usr/local' \
    POETRY_VERSION=1.7.1

RUN curl -sSL https://install.python-poetry.org | python3 -

RUN apt-get update && \
    apt-get clean

COPY ./pyproject.toml /usr/local/

RUN poetry config virtualenvs.create false

WORKDIR /repo
COPY ./pyproject.toml /repo/

RUN poetry install --no-interaction --no-ansi
RUN poetry add pytest-cov

COPY . .
