"""Per-run execution context.

A ``RunContext`` is a lightweight value object that carries all shared state
for a single collector run.  It is intentionally immutable after construction
(no in-place mutation) so that each thread working on an item gets its own
isolated copy via ``for_item`` or ``for_nested``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .config import CollectorOptions


@dataclass
class RunContext:
    """Shared state for a collector run or a single item within a run."""

    nb: Any                            # NetBoxExtendedClient / NetBoxAPI
    source_adapter: Any                # DataSource instance
    collector_opts: CollectorOptions
    regex_dir: str
    prereqs: dict                      # resolved prerequisite values for current item
    source_obj: Any                    # current source object being processed
    parent_nb_obj: Any                 # parent NetBox record (for nested items)
    dry_run: bool
    nb_main: Any | None = None         # Branchless NetBox client for global resources
    stop_requested: Callable[[], bool] | None = None

    def for_item(
        self,
        source_obj: Any,
        prereqs: dict | None = None,
    ) -> RunContext:
        """Return a copy scoped to a specific top-level source item.

        The new context gets a fresh (empty) prereq dict unless *prereqs* is
        supplied explicitly, and its ``parent_nb_obj`` is reset to ``None``.
        """
        return RunContext(
            nb=self.nb,
            nb_main=self.nb_main,
            source_adapter=self.source_adapter,
            collector_opts=self.collector_opts,
            regex_dir=self.regex_dir,
            prereqs=dict(prereqs or {}),
            source_obj=source_obj,
            parent_nb_obj=None,
            dry_run=self.dry_run,
            stop_requested=self.stop_requested,
        )

    def for_nested(
        self,
        source_obj: Any,
        parent_nb_obj: Any,
    ) -> RunContext:
        """Return a copy scoped to a nested source item (interface, inventory item, …).

        The prereq dict is inherited from the parent context so that expressions
        like ``prereq('manufacturer')`` continue to resolve correctly inside
        nested field blocks.
        """
        return RunContext(
            nb=self.nb,
            nb_main=self.nb_main,
            source_adapter=self.source_adapter,
            collector_opts=self.collector_opts,
            regex_dir=self.regex_dir,
            prereqs=dict(self.prereqs),
            source_obj=source_obj,
            parent_nb_obj=parent_nb_obj,
            dry_run=self.dry_run,
            stop_requested=self.stop_requested,
        )


def netbox_client_for_resource(ctx: RunContext, resource: str) -> Any:
    """Return the NetBox client that should handle *resource* writes/lookups."""
    nb_main = getattr(ctx, "nb_main", None)
    if resource.startswith("plugins.custom_objects.") and nb_main is not None:
        return nb_main
    return getattr(ctx, "nb", None)
