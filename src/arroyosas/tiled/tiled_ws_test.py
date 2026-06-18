import sys

from tiled.client import from_uri
from tiled.client.stream import LiveChildCreated


def on_child_created(event: LiveChildCreated) -> None:
    print(f"child_created key={event.key} seq={event.sequence}")


def main() -> None:
    uri = sys.argv[1] if len(sys.argv) > 1 else "https://tiled.nsls2.bnl.gov"

    client = from_uri(uri)
    # sub = ContainerSubscription(
    # 	client.context,
    # 	["smi/raw"],
    # 	structure_clients=client.structure_clients,
    # )
    sub = client["smi/migration"].subscribe()
    sub.child_created.add_callback(on_child_created)

    print(f"subscribing to {sub._uri}")
    sub.start()


if __name__ == "__main__":
    main()
