# Streaming GISAXS Analysis

## Getting Started

### Local Development with Pixi

This project uses [Pixi](https://pixi.sh) for dependency management and local development.

1.  **Install Pixi**: Follow the instructions on the [Pixi website](https://pixi.sh).
2.  **Initialize the environment**:
    ```bash
    pixi install
    ```
3.  **Run tests**:
    ```bash
    pixi run test
    ```

### Running Locally

To run the LSE operator locally using the local configuration:

1.  **Start required services**: Ensure you have Redis (or Kvrocks) and Tiled running. You can use the provided `docker-compose.yml` to start them:
    ```bash
    docker-compose up -d kvrocks tiled
    ```
2.  **Run the operator**:
    ```bash
    pixi run arroyo run block_configs/lse_operator_block_local.yaml
    ```
    This uses `block_configs/lse_operator_block_local.yaml` which is configured to connect to services on `localhost`.

### Beamline Simulator

The `beamline_sim` service simulates data acquisition by reading logged Tiled events and replaying them.

To run it locally via Pixi:
```bash
pixi run python -m arroyosas.app.tiled_event_sim_cli tiled_event_logs/run_28b16c3a-5ec4-4c83-8e2e-7e52df303914/
```

When running via Docker Compose, it requires a log directory to be mounted or specified in the container.

### Deployment

Changes are typically deployed by updating the Docker images and restarting the services.

1.  **Build the Docker image**:
    ```bash
    docker build -t arroyosas .
    ```
2.  **Deploy using Docker Compose**:
    ```bash
    docker-compose up -d lse_operator
    ```
    The production configuration `block_configs/lse_operator_block.yaml` is used by default in the `docker-compose.yml`.

# Copyright

Arroyo Stream Processing Toolset (arroyopy) Copyright (c) 2025, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software, please contact Berkeley Lab's Intellectual Property Office at IPO@lbl.gov.

NOTICE. This Software was developed under funding from the U.S. Department of Energy and the U.S. Government consequently retains certain rights. As such, the U.S. Government has been granted for itself and others acting on its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the Software to reproduce, distribute copies to the public, prepare derivative works, and perform publicly and display publicly, and to permit others to do so.
