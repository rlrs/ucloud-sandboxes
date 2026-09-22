import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.relay_dispatch_ab import dispatcher_with_park_concurrency
from ucloud_sandboxes import cli


class DispatchQualificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_parks_queue_and_obsolete_queued_park_is_skipped(self):
        dispatcher = dispatcher_with_park_concurrency(1)("http://gateway", "token")
        entered, release, woke = asyncio.Event(), asyncio.Event(), asyncio.Event()
        calls = []

        async def post(_session, _url, _token, request, *, action, **kwargs):
            calls.append((request.request_id, action))
            if action == "wake":
                woke.set()
                return "epoch"
            entered.set()
            await release.wait()

        def request(name):
            return SimpleNamespace(request_id=name, completed_at=None)
        first, obsolete = request("first"), request("obsolete")
        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async", side_effect=post):
            tasks = []
            try:
                tasks.append(asyncio.create_task(dispatcher.notify(first, action="park")))
                await asyncio.wait_for(entered.wait(), 1)
                tasks.append(asyncio.create_task(dispatcher.notify(obsolete, action="park")))
                tasks.append(asyncio.create_task(dispatcher.notify(request("wake"), action="wake")))
                await asyncio.wait_for(woke.wait(), 1)
                self.assertNotIn(("obsolete", "park"), calls)
                obsolete.completed_at = 1
            finally:
                release.set()
                await asyncio.gather(*tasks)
                await dispatcher.close()
        self.assertEqual(calls, [("first", "park"), ("wake", "wake")])
