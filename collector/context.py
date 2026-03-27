"""Per-run execution context.

A ``RunContext`` is a lightweight value object that carries all shared state
for a single collector run.  It is intentionally immutable after construction
(no in-place mutation) so that each thread working on an item gets its own
isolated copy via ``for_item`` or ``for_nested``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

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

    def for_item(
        self,
        source_obj: Any,
        prereqs: Optional[dict] = None,
    ) -> "RunContext":
        """Return a copy scoped to a specific top-level source item.

        The new context gets a fresh (empty) prereq dict unless *prereqs* is
        supplied explicitly, and its ``parent_nb_obj`` is reset to ``None``.
        """
        return RunContext(
            nb=self.nb,
            source_adapter=self.source_adapter,
            collector_opts=self.collector_opts,
            regex_dir=self.regex_dir,
            prereqs=dict(prereqs or {}),
            source_obj=source_obj,
            parent_nb_obj=None,
            dry_run=self.dry_run,
        )

    def for_nested(
        self,
        source_obj: Any,
        parent_nb_obj: Any,
    ) -> "RunContext":
        """Return a copy scoped to a nested source item (interface, inventory item, …).

        The prereq dict is inherited from the parent context so that expressions
        like ``prereq('manufacturer')`` continue to resolve correctly inside
        nested field blocks.
        """
        return RunContext(
            nb=self.nb,
            source_adapter=self.source_adapter,
            collector_opts=self.collector_opts,
            regex_dir=self.regex_dir,
            prereqs=dict(self.prereqs),
            source_obj=source_obj,
            parent_nb_obj=parent_nb_obj,
            dry_run=self.dry_run,
        )
