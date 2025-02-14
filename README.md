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
