#!/usr/bin/env python3
"""s3ncdu — ncdu-like TUI for S3 / DO Spaces buckets."""
from __future__ import annotations

import argparse
import asyncio
import os
import string
import sys
from dataclasses import dataclass, field

import aiobotocore.session
import boto3
from rich.live import Live
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Static


# ── helpers ─────────────────────────────────────────────────────────────────

def hsize(n: int) -> str:
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}" if u != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PiB"


# ── Node ────────────────────────────────────────────────────────────────────

@dataclass
class Node:
    name: str
    dir: bool = False
    size: int = 0
    key: str = ""
    ch: dict[str, Node] = field(default_factory=dict)
    par: Node | None = None

    def sorted(self, by: str = "size") -> list[Node]:
        c = list(self.ch.values())
        c.sort(key=lambda x: x.size if by == "size" else x.name.lower(),
               reverse=(by == "size"))
        return c

    def recalc(self) -> None:
        if self.dir:
            self.size = sum(c.size for c in self.ch.values())
        if self.par:
            self.par.recalc()

    def all_keys(self) -> list[str]:
        if not self.dir:
            return [self.key] if self.key else []
        return [k for c in self.ch.values() for k in c.all_keys()]

    def count(self) -> int:
        return 1 if not self.dir else sum(c.count() for c in self.ch.values())


# ── Tree ────────────────────────────────────────────────────────────────────

class Tree:
    def __init__(self) -> None:
        self._root = Node("/", dir=True)

    @property
    def root(self) -> Node:
        return self._root

    def insert(self, key: str, size: int) -> None:
        parts = [p for p in key.split("/") if p]
        node = self._root
        for i, p in enumerate(parts):
            leaf = i == len(parts) - 1
            if p not in node.ch:
                node.ch[p] = Node(name=p, dir=not leaf, size=size if leaf else 0,
                                  key=key if leaf else "", par=node)
            elif leaf:
                node.ch[p].size, node.ch[p].key = size, key
            node = node.ch[p]
        # bubble sizes up
        p = node.par
        while p:
            p.size = sum(c.size for c in p.ch.values())
            p = p.par

    def remove(self, node: Node) -> None:
        if node.par and node.name in node.par.ch:
            del node.par.ch[node.name]
            node.par.recalc()


# ── Scanner ─────────────────────────────────────────────────────────────────

class Scanner:
    def __init__(self, endpoint: str, bucket: str, ak: str, sk: str) -> None:
        self.endpoint = endpoint
        self.bucket = bucket
        self.ak = ak
        self.sk = sk

    async def scan(self, progress=None) -> Tree:
        tree = Tree()
        sem = asyncio.Semaphore(16)
        session = aiobotocore.session.get_session()
        async with session.create_client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.ak,
            aws_secret_access_key=self.sk,
        ) as s3:
            prefixes = await self._discover(s3, "", depth=2)
            if len(prefixes) <= 1:
                base = prefixes[0] if prefixes else ""
                prefixes = self._split_prefix(base)

            async def _guarded(pfx: str) -> None:
                async with sem:
                    await self._list_prefix(s3, pfx, tree, progress)

            await asyncio.gather(*[_guarded(p) for p in prefixes])
        return tree

    async def _discover(self, s3, prefix: str, depth: int) -> list[str]:
        if depth <= 0:
            return [prefix]
        resp = await s3.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/",
        )
        common = resp.get("CommonPrefixes", [])
        if not common:
            return [prefix]
        results = await asyncio.gather(
            *[self._discover(s3, cp["Prefix"], depth - 1) for cp in common]
        )
        return [p for group in results for p in group]

    async def _list_prefix(self, s3, prefix: str, tree: Tree, progress) -> None:
        token = None
        while True:
            kw: dict = dict(Bucket=self.bucket, Prefix=prefix, MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            resp = await s3.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                tree.insert(obj["Key"], obj["Size"])
            if progress:
                progress(len(resp.get("Contents", [])))
            if not resp.get("IsTruncated"):
                break
            token = resp["NextContinuationToken"]

    @staticmethod
    def _split_prefix(prefix: str) -> list[str]:
        return [prefix + c for c in string.digits + string.ascii_lowercase]


# ── UI ──────────────────────────────────────────────────────────────────────

class UI(App):
    CSS = """
    #path   { height: 1; background: $primary; color: $text; padding: 0 1; }
    #status { height: 1; background: $panel; padding: 0 1; }
    DataTable { height: 1fr; }
    """
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("d", "delete", "Delete"),
        Binding("s", "sort_size", "Size"),
        Binding("n", "sort_name", "Name"),
        Binding("backspace", "go_up", "Back"),
        Binding("y", "confirm_yes", show=False),
        Binding("escape", "confirm_cancel", show=False),
    ]

    def __init__(self, ftree: Tree, s3_client, bucket: str) -> None:
        super().__init__()
        self._ftree = ftree
        self.s3 = s3_client
        self.bucket = bucket
        self.cwd = ftree.root
        self.sort_by = "size"
        self._items: list[Node] = []
        self._confirm_node: Node | None = None

    def compose(self) -> ComposeResult:
        yield Static("", id="path")
        yield DataTable(show_header=False, cursor_type="row")
        yield Static("", id="status")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(DataTable).add_column("entry")
        self._refresh()

    # ── helpers ───────────────────────────────────────────────────────────

    def _cancel_confirm(self) -> bool:
        if self._confirm_node:
            self._confirm_node = None
            self._update_status()
            return True
        return False

    def _sel(self) -> Node | None:
        r = self.query_one(DataTable).cursor_row
        return self._items[r] if 0 <= r < len(self._items) else None

    def _refresh(self) -> None:
        tbl = self.query_one(DataTable)
        tbl.clear()
        self._confirm_node = None
        self.query_one("#path", Static).update(f" {self.bucket}:{self._path()}")
        self._items = self.cwd.sorted(self.sort_by)
        mx = max((i.size for i in self._items), default=1) or 1
        bw = 10
        for item in self._items:
            filled = int(bw * item.size / mx)
            t = Text()
            t.append(hsize(item.size).rjust(10), "bold")
            t.append(" [")
            t.append("#" * filled, "green")
            t.append(" " * (bw - filled))
            t.append("] ")
            t.append(("/" if item.dir else " ") + item.name,
                     "bold cyan" if item.dir else "")
            tbl.add_row(t)
        self._update_status()

    def _path(self) -> str:
        parts, n = [], self.cwd
        while n and n.name != "/":
            parts.append(n.name)
            n = n.par
        return "/" + "/".join(reversed(parts)) if parts else "/"

    def _update_status(self) -> None:
        self.query_one("#status", Static).update(
            f" Total: {hsize(self._ftree.root.size)}  Items: {self._ftree.root.count():,}")

    # ── navigation ────────────────────────────────────────────────────────

    def on_data_table_row_selected(self, _) -> None:
        if self._cancel_confirm():
            return
        node = self._sel()
        if node and node.dir and node.ch:
            self.cwd = node
            self._refresh()

    def action_go_up(self) -> None:
        if self._cancel_confirm():
            return
        if not self.cwd.par:
            return
        nm = self.cwd.name
        self.cwd = self.cwd.par
        self._refresh()
        for i, item in enumerate(self._items):
            if item.name == nm:
                self.query_one(DataTable).move_cursor(row=i)
                break

    # ── delete ────────────────────────────────────────────────────────────

    def action_delete(self) -> None:
        if self._cancel_confirm():
            return
        node = self._sel()
        if not node:
            return
        self._confirm_node = node
        dn = node.name + ("/" if node.dir else "")
        cnt = node.count()
        self.query_one("#status", Static).update(
            f' Delete "{dn}" ({cnt:,} objects, {hsize(node.size)})? [y/N]')

    def action_confirm_yes(self) -> None:
        if not self._confirm_node:
            return
        node = self._confirm_node
        keys = node.all_keys()
        self.query_one("#status", Static).update(
            f" Deleting {len(keys):,} objects…")
        for i in range(0, len(keys), 1000):
            self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]},
            )
        self._ftree.remove(node)
        cur = self.query_one(DataTable).cursor_row
        self._refresh()
        if self._items:
            self.query_one(DataTable).move_cursor(
                row=min(cur, len(self._items) - 1))
        self.query_one("#status", Static).update(
            f" Deleted {len(keys):,} object(s)")
        self.set_timer(2, self._update_status)

    def action_confirm_cancel(self) -> None:
        self._cancel_confirm()

    # ── sort ──────────────────────────────────────────────────────────────

    def action_sort_size(self) -> None:
        if self._cancel_confirm():
            return
        self.sort_by = "size"
        self._refresh()

    def action_sort_name(self) -> None:
        if self._cancel_confirm():
            return
        self.sort_by = "name"
        self._refresh()


# ── main ────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="s3ncdu — ncdu for S3 / DO Spaces")
    ap.add_argument("--endpoint", default=os.environ.get("S3_ENDPOINT", ""))
    ap.add_argument("--bucket", default=os.environ.get("S3_BUCKET", ""))
    ap.add_argument("--ak", default=os.environ.get("AWS_ACCESS_KEY_ID", ""))
    ap.add_argument("--sk", default=os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    args = ap.parse_args()

    if not all([args.endpoint, args.bucket, args.ak, args.sk]):
        ap.error("--endpoint, --bucket, --ak, --sk required (or set S3_ENDPOINT, "
                 "S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")

    scanner = Scanner(args.endpoint, args.bucket, args.ak, args.sk)
    total = 0

    def on_progress(n: int) -> None:
        nonlocal total
        total += n
        live.update(Text(f"  Scanning… {total:,} objects"))

    with Live(Text("  Scanning…"), refresh_per_second=10) as live:
        tree = asyncio.run(scanner.scan(progress=on_progress))

    print(f"Scan complete: {total:,} objects, {hsize(tree.root.size)}")

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.ak,
        aws_secret_access_key=args.sk,
    )
    UI(tree, s3, args.bucket).run()


if __name__ == "__main__":
    main()
