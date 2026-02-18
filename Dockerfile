FROM python:3.11


# Install git for cloning the tiled repository
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY . /app

RUN pip install .[all]

RUN pip install debugpy

CMD ["echo", "Hello World"]
