# Streaming GISAXS Analysis


## podman tips on ubunut
* Install homebrew
* `brew install podman`
* `brew services start podman`
* `sudo apt install runc`
* use run instead of crun
    * vi /etc/containers/containers.conf
    * add:

``` toml
[system]
runtime = "runc"

```
* install nvidia container [toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)

# Copyright

Arroyo Stream Processing Toolset (arroyopy) Copyright (c) 2025, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software, please contact Berkeley Lab's Intellectual Property Office at IPO@lbl.gov.

NOTICE. This Software was developed under funding from the U.S. Department of Energy and the U.S. Government consequently retains certain rights. As such, the U.S. Government has been granted for itself and others acting on its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the Software to reproduce, distribute copies to the public, prepare derivative works, and perform publicly and display publicly, and to permit others to do so.
