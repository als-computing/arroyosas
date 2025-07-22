FROM python:3.11


# Install git for cloning the tiled repository
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Clone and install the custom Tiled branch with WebSocket support
RUN git clone https://github.com/vshekar/tiled.git /tmp/tiled && \
    cd /tmp/tiled && \
    git checkout websocket-endpoint && \
    pip install --upgrade pip && \
    TILED_BUILD_SKIP_UI=1 pip install '.[all]' && \
    rm -rf /tmp/tiled

COPY . /app

RUN pip install .[all]

RUN pip install debugpy

CMD ["echo", "Hello World"]
