FROM python:3.11

WORKDIR /app

COPY . /app

RUN pip install uv && \
    uv pip install --system torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    uv pip install --system ".[lse]"

RUN pip install debugpy


CMD ["echo", "Hello World"]
