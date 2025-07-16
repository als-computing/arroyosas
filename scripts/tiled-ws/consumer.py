from pprint import pprint

from tiled.client import from_uri
from tiled.client.stream import Subscription

# c = from_uri("http://localhost:8000", api_key="secret")
c = from_uri("https://tiled-dev.nsls2.bnl.gov/")
target = "img"


def on_new_run(sub, data):
    print_data(data)
    uid = data["key"]
    print(f"New run {uid}")
    run_sub = Subscription(c.context, [uid], start=0)
    run_sub.add_callback(on_streams_namespace)
    run_sub.start()


def on_streams_namespace(sub, data):
    print_data(data)
    streams_sub = Subscription(c.context, sub.segments + ["streams"], start=0)
    streams_sub.add_callback(on_new_stream)
    streams_sub.start()


def on_new_stream(sub, data):
    print_data(data)
    stream_name = data["key"]
    print(f"new stream {stream_name}")
    stream_sub = Subscription(c.context, sub.segments + [stream_name], start=0)
    stream_sub.add_callback(on_node_in_stream)
    stream_sub.start()


def on_node_in_stream(sub, data):
    print_data(data)
    key = data["key"]
    if key != target:
        return
    stream_sub = Subscription(c.context, sub.segments + [key], start=0)
    stream_sub.add_callback(print)
    stream_sub.start()


catalog_sub = Subscription(c.context)
catalog_sub.add_callback(on_new_run)
catalog_sub.start()


def print_data(data):
    # print(data['key'])
    pprint(data["data"])
    print("=" * 20)
    print(" ")


if __name__ == "__main__":
    import time

    while True:
        time.sleep(0.01)
