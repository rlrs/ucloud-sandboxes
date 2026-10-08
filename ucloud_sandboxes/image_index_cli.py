"""``ucloud-sandboxes image-index``: the gateway's index of training image names
(docs/image-index.md), read through its API with the SDK key.

  summary                       names and tasks per environment, by state
  show NAME                     one name: its environment, tasks, source, image and state
  list [--environment E] [--state S]
  task-ids ENVIRONMENT [--out FILE]
                                the tasks a trainer may sample, as a taskset's task_ids_file
  export-task-ids DIR           every environment's task_ids_file, plus a summary

The gateway URL and key come from --gateway-url / --api-token-file, or from
UCLOUD_SANDBOX_URL and UCLOUD_SANDBOX_API_TOKEN as the SDK reads them.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from urllib import error, parse, request

STATE_COLUMNS = ("ready", "building", "not_built", "retrying", "failed")


class IndexClient:
    def __init__(self, url, token):
        self.url, self.token = url.rstrip("/"), token

    def get(self, path, **query):
        query = {key: value for key, value in query.items() if value is not None}
        target = f"{self.url}{path}" + (f"?{parse.urlencode(query)}" if query else "")
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        try:
            with request.urlopen(request.Request(target, headers=headers), timeout=120) as response:
                return json.loads(response.read())
        except error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            try:
                message = json.loads(body).get("error", body)
            except ValueError:
                message = body
            raise SystemExit(f"{path}: HTTP {exc.code}: {message}") from None

    def names(self, *, environment=None, state=None):
        after = ""
        while True:
            page = self.get("/v1/image-index/names", environment=environment, state=state, after=after, limit=5000)
            yield from page["names"]
            if not page["next"]:
                return
            after = page["next"]


def _client(args):
    url = args.gateway_url or os.environ.get("UCLOUD_SANDBOX_URL")
    if not url:
        raise SystemExit("set --gateway-url or UCLOUD_SANDBOX_URL")
    token = (args.api_token_file.read_text().strip() if args.api_token_file
             else os.environ.get("UCLOUD_SANDBOX_API_TOKEN", ""))
    return IndexClient(url, token)


def summary_table(summary):
    """The summary as an aligned text table."""
    header = ("environment", "names", "tasks", "prepared", "recipes") + STATE_COLUMNS
    rows = [header]
    for environment, row in summary["environments"].items():
        rows.append((environment, row["names"], row["tasks"], row["prepared"], row["recipes"],
                     *(row["states"].get(state, 0) for state in STATE_COLUMNS)))
    totals = summary["totals"]
    states = {state: sum(row["states"].get(state, 0) for row in summary["environments"].values())
              for state in STATE_COLUMNS}
    rows.append(("total", totals.get("names", 0), totals.get("tasks", 0), totals.get("prepared", 0),
                 totals.get("recipes", 0), *(states[state] for state in STATE_COLUMNS)))
    widths = [max(len(f"{row[i]:,}" if isinstance(row[i], int) else str(row[i])) for row in rows)
              for i in range(len(header))]
    lines = []
    for row in rows:
        cells = [(f"{cell:,}" if isinstance(cell, int) else str(cell)) for cell in row]
        lines.append("  ".join(cell.ljust(widths[0]) if i == 0 else cell.rjust(widths[i])
                               for i, cell in enumerate(cells)))
    return "\n".join(lines)


def cmd_summary(args):
    summary = _client(args).get("/v1/image-index")
    print(json.dumps(summary, indent=2) if args.json else summary_table(summary))


def cmd_show(args):
    detail = _client(args).get("/v1/image-index/name", name=args.name)
    if args.json:
        print(json.dumps(detail, indent=2))
        return
    made = (f"prepared image {detail['prepared_reference']}" if detail["kind"] == "prepared" else
            f"recipe {detail['dockerfile']} in context {detail['context_archive_digest']}"
            + (f", on {detail['base']['kind']} {detail['base']['reference']}" if detail.get("base") else ""))
    lines = [f"name         {detail['name']}",
             f"environment  {detail['environment']}",
             f"state        {detail['state']}" + (f" (attempts {detail['attempts']})" if detail["attempts"] else ""),
             f"image        {made}",
             f"source       {json.dumps(detail['source'], sort_keys=True)}",
             f"tasks        {detail['tasks']}: {', '.join(detail['task_ids'][:10])}"
             + (" ..." if detail["tasks"] > 10 else "")]
    if detail.get("error"):
        lines.append(f"last error   {detail['error']}")
    if detail.get("other_names"):
        lines.append(f"same image   {', '.join(detail['other_names'])}")
    print("\n".join(lines))


def cmd_list(args):
    for row in _client(args).names(environment=args.environment, state=args.state):
        print(json.dumps(row) if args.json else f"{row['state']:<10} {row['environment'] or '-':<16} {row['name']}")


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=1) + "\n")
    temporary.replace(path)


def cmd_task_ids(args):
    answer = _client(args).get("/v1/image-index/task-ids", environment=args.environment)
    if args.out:
        _write(args.out, answer["task_ids"])
        print(f"{len(answer['task_ids']):,} task ids -> {args.out}"
              + (f" (excluded: {answer['excluded']})" if answer["excluded"] else ""), file=sys.stderr)
    else:
        print(json.dumps(answer["task_ids"], indent=1))


def cmd_export_task_ids(args):
    client = _client(args)
    summary = client.get("/v1/image-index")
    report = {"gateway": client.url, "environments": {}}
    for environment in summary["environments"]:
        if environment == "(none)":
            continue
        answer = client.get("/v1/image-index/task-ids", environment=environment)
        if not answer["task_ids"]:
            continue
        _write(args.directory / f"{environment}.task-ids.json", answer["task_ids"])
        report["environments"][environment] = {"task_ids": len(answer["task_ids"]), "excluded": answer["excluded"]}
    _write(args.directory / "summary.json", {**report, "index": summary})
    for environment, row in report["environments"].items():
        print(f"{environment:<16} {row['task_ids']:>8,} task ids" + (f"  excluded {row['excluded']}"
                                                                     if row["excluded"] else ""))


def add_commands(subparsers):
    parser = subparsers.add_parser("image-index", help="The gateway's index of training image names.",
                                   description=__doc__.split("\n\n", 1)[1],
                                   formatter_class=__import__("argparse").RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="image_index_command", required=True)

    def add(name, func, help_text):
        sub = commands.add_parser(name, help=help_text)
        sub.add_argument("--gateway-url", default=None)
        sub.add_argument("--api-token-file", type=Path, default=None)
        sub.add_argument("--json", action="store_true", help="print JSON")
        sub.set_defaults(func=func)
        return sub

    add("summary", cmd_summary, "names and tasks per environment, by state")
    add("show", cmd_show, "one name in full").add_argument("name")
    listing = add("list", cmd_list, "names, filtered")
    listing.add_argument("--environment")
    listing.add_argument("--state", choices=STATE_COLUMNS)
    task_ids = add("task-ids", cmd_task_ids, "an environment's sampleable tasks (a task_ids_file)")
    task_ids.add_argument("environment")
    task_ids.add_argument("--out", type=Path)
    add("export-task-ids", cmd_export_task_ids, "every environment's task_ids_file").add_argument(
        "directory", type=Path)
