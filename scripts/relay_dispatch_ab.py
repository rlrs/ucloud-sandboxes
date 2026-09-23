"""Qualification-only relay launcher for isolating park HTTP fanout.

Run on an idle test fleet, with the normal installed package and config. This
does not change production defaults or bypass durable lifecycle ownership.
"""

import argparse
import asyncio

from ucloud_sandboxes import cli
from ucloud_sandboxes.relay_lifecycle import RelayLifecycleDispatcher


def dispatcher_with_park_concurrency(concurrency):
    if concurrency < 1:
        raise ValueError("park concurrency must be positive")

    class QualifiedDispatcher(RelayLifecycleDispatcher):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._qualification_park_slots = asyncio.Semaphore(concurrency)

        async def _notify(self, request, *, action):
            if action != "park":
                return await super()._notify(request, action=action)
            async with self._qualification_park_slots:
                return await super()._notify(request, action=action)

    return QualifiedDispatcher


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--park-concurrency", type=int, required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    if args.park_concurrency < 1:
        parser.error("park concurrency must be positive")
    cli.RelayLifecycleDispatcher = dispatcher_with_park_concurrency(
        args.park_concurrency
    )
    return cli.main(["serve-model-relay", "--config", args.config])


if __name__ == "__main__":
    raise SystemExit(main())
