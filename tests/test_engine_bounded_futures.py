from __future__ import annotations

from collector.engine import Engine


class _FakeFuture:
    def __init__(self, idx: int) -> None:
        self.idx = idx

    def exception(self):
        return None

    def __hash__(self) -> int:
        return hash(self.idx)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FakeFuture) and self.idx == other.idx


def test_drain_bounded_futures_caps_pending_work(monkeypatch):
    items = list(range(10))
    pending: list[_FakeFuture] = []
    completed: list[int] = []
    submitted: list[int] = []
    max_seen = 0

    def submit_item(item: int) -> _FakeFuture:
        nonlocal max_seen
        future = _FakeFuture(item)
        submitted.append(item)
        pending.append(future)
        max_seen = max(max_seen, len(pending))
        return future

    def fake_wait(futures, return_when=None):  # noqa: ARG001
        future = pending.pop(0)
        return {future}, set(pending)

    def on_complete(future: _FakeFuture, item: int) -> None:
        completed.append(item)

    monkeypatch.setattr("collector.engine.wait", fake_wait)

    Engine._drain_bounded_futures(
        items,
        max_in_flight=3,
        submit_item=submit_item,
        on_complete=on_complete,
    )

    assert submitted == items
    assert completed == items
    assert max_seen <= 3
