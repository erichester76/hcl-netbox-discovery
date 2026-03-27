"""Abstract base class for data source adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DataSource(ABC):
    """Interface that every source adapter must implement.

    The engine calls these methods in this order for each collector run:

    1. ``connect(config)``  — establish connection to the source system.
    2. ``get_objects(collection)`` — return a flat list of raw items for the
       named collection.  The collection name corresponds to the
       ``source_collection`` attribute in an HCL ``object`` block.
    3. (optional) ``get_nested(parent_obj, path)`` — walk *path* on
       *parent_obj* to return a list of nested items.  The engine usually
       handles this via the ``source()`` expression in field_resolvers, so
       most adapters do not need to override this method.
    4. ``close()``  — tear down the connection.
    """

    @abstractmethod
    def connect(self, config: Any) -> None:
        """Connect to the source system using settings from *config*."""

    @abstractmethod
    def get_objects(self, collection: str) -> list:
        """Return all items in *collection* as a flat list.

        The items may be plain Python dicts or source-system SDK objects —
        the field_resolvers ``source()`` function handles both transparently.
        """

    def get_nested(self, parent_obj: Any, path: str) -> list:
        """Return nested items at *path* on *parent_obj*.

        The default implementation delegates to the shared
        :func:`~collector.field_resolvers.walk_path` helper so that both
        dict and attribute access work without any adapter-specific code.
        Sub-classes may override for source-specific optimisations.
        """
        from collector.field_resolvers import walk_path  # lazy import
        result = walk_path(parent_obj, path)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    @abstractmethod
    def close(self) -> None:
        """Release any resources held by this adapter."""
