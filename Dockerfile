FROM python:3.11

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip && \
    pip install .
    pip install .[all]

RUN pip install debugpy


CMD ["echo", "Hello World"]
