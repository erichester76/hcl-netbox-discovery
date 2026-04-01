#!/usr/bin/env python3
"""Object-oriented extensions for pynetbox with pluggable cache/backends.

This module provides:
- Constructor-managed configuration
- Optional cache layer (none, Redis, or SQLite)
- CRUD wrappers that keep cache coherent
- Optional Diode write backend
- Per-connection API call throttling
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha256
import importlib
import json
import logging
import os
import pickle
import random
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Optional, Sequence

from deepdiff import DeepDiff
import pynetbox

logger = logging.getLogger(__name__)

# Map of resource to fields that should be normalized to *_id
FK_FIELDS = {
    # Devices
    "dcim.devices": ["site", "tenant", "role", "device_type", "platform", "location", "manufacturer", "rack"],
    "dcim.device_types": ["manufacturer"],
    "dcim.interfaces": ["device", "module"],
    "dcim.racks": ["site", "location"],
    "dcim.regions": [],
    "dcim.sites": [],
    "dcim.locations": ["site"],
    "dcim.manufacturers": [],
    "dcim.device_roles": [],
    "dcim.platforms": ["manufacturer"],
    "dcim.virtual_chassis": [],
    "dcim.inventory_items": ["device", "role"],
    "dcim.inventory_item_roles": [],
    "dcim.device_bays": ["device"],
    "dcim.module_type_profiles": [],
    "dcim.module_types": ["manufacturer", "profile"],
    "dcim.modules": ["device", "module_bay", "module_type"],
    "dcim.module_bays": ["device"],
    "dcim.module_bay_templates": ["device_type"],
    "dcim.power_feeds": ["power_panel"],
    "dcim.power_outlets": ["device", "power_port"],
    "dcim.power_panels": ["site"],
    "dcim.power_ports": ["device", "module"],
    "dcim.console_ports": ["device"],
    "dcim.console_server_ports": ["device"],
    "dcim.front_ports": ["device", "rear_port"],
    "dcim.rear_ports": ["device"],
    "dcim.virtual_device_contexts": ["device"],
    "dcim.mac_addresses": ["device", "interface"],
    "dcim.cables": ["termination_a_type", "termination_b_type"],

    # IPAM
    "ipam.ip_addresses": ["vrf", "tenant", "assigned_object", "assigned_object_type"],
    "ipam.prefixes": ["site", "tenant", "vlan", "vrf"],
    "ipam.vlans": ["site", "tenant", "group"],
    "ipam.vlan_groups": ["site"],
    "ipam.vrfs": ["tenant", "rd"],
    "ipam.rirs": [],
    "ipam.fhrp_groups": [],
    "ipam.route_targets": [],
    "ipam.ip_ranges": ["vrf", "tenant"],
    "ipam.services": ["device", "virtual_machine"],
    "ipam.roles": [],

    # Virtualization
    "virtualization.clusters": ["group", "type", "site"],
    "virtualization.cluster_groups": [],
    "virtualization.cluster_types": [],
    "virtualization.virtual_machines": ["cluster", "tenant", "role", "platform", "device"],
    "virtualization.virtual_machine_interfaces": ["virtual_machine"],
    "virtualization.virtual_disks": ["virtual_machine"],
    "virtualization.interfaces": ["virtual_machine"],

    # Wireless
    "wireless.wireless_lan_groups": ["parent"],
    "wireless.wireless_lans": ["group", "site", "tenant"],
    "wireless.wireless_links": ["interface_a", "interface_b"],

    # Extras
    "extras.config_contexts": [],
    "extras.config_templates": [],
    "extras.custom_fields": [],
    "extras.custom_links": [],
    "extras.journal_entries": [],
    "extras.tags": [],
    "extras.webhooks": [],

    # Tenancy
    "tenancy.contact_groups": [],
    "tenancy.contact_roles": [],
    "tenancy.contacts": ["contact_role", "contact_group", "tenant"],
    "tenancy.tenants": ["group"],
    "tenancy.tenant_groups": [],

    # Users
    "users.groups": [],

    # VPN
    "vpn.ike_policies": [],
    "vpn.ike_proposals": [],
    "vpn.ipsec_policies": [],
    "vpn.ipsec_profiles": [],
    "vpn.ipsec_proposals": [],
    "vpn.l2vpn_terminations": ["l2vpn", "termination_type"],
    "vpn.l2vpns": ["tenant", "type"],
    "vpn.tunnel_groups": [],
    "vpn.tunnel_terminations": ["tunnel", "termination_type"],
    "vpn.tunnels": ["tenant", "group"],
    "vpn.tunnel_termination_groups": [],
}

# Shared FK normalization function for use in all NetBox API calls
def normalize_fk_fields(resource: str, payload: dict, for_write: bool = False) -> dict:
    """Normalise FK fields in *payload* for the given *resource*.

    When *for_write* is ``False`` (the default, used for GET filter params)
    integer FK values are renamed from the plain field name to the ``_id``
    form expected by NetBox filter parameters (e.g. ``manufacturer`` →
    ``manufacturer_id``).

    When *for_write* is ``True`` (used for POST/PATCH write payloads) integer
    FK values are **kept under their original field name** because the NetBox
    write API expects ``{"manufacturer": 1}`` not ``{"manufacturer_id": 1}``.
    Nested object/dict values are still unwrapped to their integer IDs in both
    modes, but the field name is preserved in write mode.
    """
    fields = set(payload.keys())
    for field in list(fields):
        value = payload[field]
        if (
            (resource in FK_FIELDS and field in FK_FIELDS[resource])
            or field.endswith("_id")
        ):
            # Only append _id if not already present
            target_field = field if field.endswith("_id") else f"{field}_id"
            # If value is a dict with id, or an object with id, or an int
            if isinstance(value, dict) and "id" in value:
                payload[target_field] = value["id"]
                if target_field != field:
                    del payload[field]
            elif hasattr(value, "id"):
                payload[target_field] = getattr(value, "id")
                if target_field != field:
                    del payload[field]
            elif isinstance(value, int) and not field.endswith("_id"):
                if for_write:
                    # For POST/PATCH bodies keep the plain field name so that
                    # NetBox receives e.g. {"manufacturer": 1} which it accepts
                    # as an integer FK.  Renaming to manufacturer_id here would
                    # cause a 400 "This field is required" on older NetBox
                    # versions that do not recognise the _id alias in write ops.
                    pass
                else:
                    # For GET filter params NetBox expects the _id suffix form.
                    payload[target_field] = value
                    del payload[field]
    return payload

class RateLimiter:
    """Thread-safe token bucket limiter for API calls per connection.

    Also holds a shared global-cooldown timestamp.  When any thread calls
    :meth:`trigger_cooldown` (e.g. after receiving a 503/504 that signals the
    server is overwhelmed) all threads block in :meth:`acquire` until the
    cooldown expires.  A small per-thread random jitter is added on top so
    that threads don't all burst through at exactly the same moment once the
    cooldown lifts (thundering-herd prevention).
    """

    def __init__(self, calls_per_second: float = 0.0, burst: int = 1) -> None:
        self.calls_per_second = max(float(calls_per_second), 0.0)
        self.burst = max(int(burst), 1)
        self.tokens = float(self.burst)
        self.last_refill = time.perf_counter()
        self.lock = threading.Lock()
        # Global cooldown: perf_counter() timestamp after which calls are allowed.
        self._cooldown_until: float = 0.0

    def trigger_cooldown(self, seconds: float) -> None:
        """Set (or extend) a shared cross-thread cooldown for *seconds*.

        Only extends the cooldown; never shortens an already-longer one.
        Safe to call from any thread.
        """
        if seconds <= 0:
            return
        until = time.perf_counter() + seconds
        with self.lock:
            if until > self._cooldown_until:
                self._cooldown_until = until
                logger.warning(
                    "Global API cooldown triggered: all threads will pause for %.1fs",
                    seconds,
                )

    def acquire(self) -> float:
        """Acquire one token and sleep if needed. Returns total sleep seconds.

        If a global cooldown is active, this call blocks until the cooldown
        has expired.  A small per-thread random jitter (up to 10 % of the
        remaining cooldown, max 5 s) is added after the mandatory wait to
        stagger thread wake-ups and avoid a secondary thundering herd.
        """
        total_slept = 0.0

        # --- global cooldown check (shared across all threads) ---------------
        with self.lock:
            cooldown_remaining = self._cooldown_until - time.perf_counter()

        if cooldown_remaining > 0:
            # Per-thread jitter: up to 10% of the remaining cooldown, capped at 5 s.
            jitter = random.uniform(0.0, min(cooldown_remaining * 0.10, 5.0))
            wait = cooldown_remaining + jitter
            logger.warning(
                "Global API cooldown active: sleeping %.1fs (%.1fs remaining + %.1fs jitter)",
                wait,
                cooldown_remaining,
                jitter,
            )
            time.sleep(wait)
            total_slept += wait

        # --- token-bucket rate limiting (unchanged) --------------------------
        if self.calls_per_second <= 0:
            return total_slept

        sleep_for = 0.0
        with self.lock:
            now = time.perf_counter()
            elapsed = max(0.0, now - self.last_refill)
            self.tokens = min(self.burst, self.tokens + elapsed * self.calls_per_second)
            self.last_refill = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return total_slept

            sleep_for = (1.0 - self.tokens) / self.calls_per_second
            self.tokens = 0.0

        if sleep_for > 0:
            logger.debug(
                "Rate limiter throttling: sleeping %.3fs (limit=%.1f calls/s)",
                sleep_for,
                self.calls_per_second,
            )
            time.sleep(sleep_for)
            with self.lock:
                self.last_refill = time.perf_counter()
            total_slept += sleep_for

        return total_slept


class CacheBackend(ABC):
    """Cache backend contract."""

    @abstractmethod
    def get(self, key: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_prefix(self, key_prefix: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def count(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def keys(self) -> list:
        """Return all cache keys (without key_prefix)."""
        raise NotImplementedError

    @abstractmethod
    def get_ttl(self, key: str) -> Optional[int]:
        """Return seconds remaining for *key*, or None if unknown/no TTL."""
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class NullCacheBackend(CacheBackend):
    """No-op cache backend."""

    def get(self, key: str) -> Any:
        return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        return None

    def delete(self, key: str) -> None:
        return None

    def delete_prefix(self, key_prefix: str) -> None:
        return None

    def clear(self) -> None:
        return None

    def count(self) -> int:
        return 0

    def keys(self) -> list:
        return []

    def get_ttl(self, key: str) -> Optional[int]:
        return None

    def close(self) -> None:
        return None


class RedisCacheBackend(CacheBackend):
    """Redis cache backend using pickled object values."""

    def __init__(self, url: str, key_prefix: str = "nbx:", default_ttl: int = 300) -> None:
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis package is required for RedisCacheBackend") from exc

        self._redis_module = redis
        self.client = redis.from_url(url, decode_responses=False)
        self.key_prefix = key_prefix
        self.default_ttl = max(int(default_ttl), 1)
        self._failure_lock = threading.Lock()
        self._failure_count = 0
        self._disabled = False
        self._disable_logged = False
        self._disable_threshold = max(
            int(os.getenv("NETBOX_CACHE_DISABLE_ON_FAILURES", "5")),
            1,
        )
        try:
            self.client.ping()
        except redis.RedisError as exc:
            logging.getLogger(__name__).warning(
                "Redis cache unavailable at %s (%s); cache disabled for this run.",
                url,
                exc,
            )
            self._disabled = True

    def _record_success(self) -> None:
        with self._failure_lock:
            self._failure_count = 0

    def _record_failure(self, operation: str, exc: Exception) -> None:
        with self._failure_lock:
            self._failure_count += 1
            count = self._failure_count
            if self._disabled:
                return
            if count >= self._disable_threshold:
                self._disabled = True
                logger.error(
                    "Disabling Redis cache for this process after %s failures (last_op=%s): %s",
                    count,
                    operation,
                    exc,
                )
                return

        logger.warning(
            "Redis cache %s failed (%s/%s before disable): %s",
            operation,
            count,
            self._disable_threshold,
            exc,
        )

    def _is_disabled(self) -> bool:
        with self._failure_lock:
            disabled = self._disabled
            if disabled and not self._disable_logged:
                self._disable_logged = True
                logger.warning("Redis cache is disabled for this process run")
            return disabled

    def _k(self, key: str) -> str:
        return f"{self.key_prefix}{key}"

    def get(self, key: str) -> Any:
        if self._is_disabled():
            return None
        try:
            blob = self.client.get(self._k(key))
        except Exception as exc:
            self._record_failure("get", exc)
            return None
        self._record_success()
        if not blob:
            return None
        try:
            return pickle.loads(blob)
        except Exception:
            logger.warning("Failed to unpickle Redis cache key=%s; deleting entry", key)
            self.delete(key)
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        if self._is_disabled():
            return
        ttl = self.default_ttl if ttl_seconds is None else max(int(ttl_seconds), 1)
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        try:
            self.client.setex(self._k(key), ttl, blob)
        except Exception as exc:
            self._record_failure("set", exc)
            return
        self._record_success()

    def delete(self, key: str) -> None:
        if self._is_disabled():
            return
        try:
            self.client.delete(self._k(key))
        except Exception as exc:
            self._record_failure("delete", exc)
            return
        self._record_success()

    def delete_prefix(self, key_prefix: str) -> None:
        if self._is_disabled():
            return
        pattern = f"{self._k(key_prefix)}*"
        try:
            keys = list(self.client.scan_iter(match=pattern))
            if not keys:
                self._record_success()
                return
            pipe = self.client.pipeline()
            for item in keys:
                pipe.delete(item)
            pipe.execute()
            self._record_success()
        except Exception as exc:
            self._record_failure("delete_prefix", exc)

    def clear(self) -> None:
        self.delete_prefix("")

    def count(self) -> int:
        if self._is_disabled():
            return 0
        try:
            pattern = f"{self.key_prefix}*"
            keys = list(self.client.scan_iter(match=pattern))
            self._record_success()
            return len(keys)
        except Exception as exc:
            self._record_failure("count", exc)
            return 0

    def keys(self) -> list:
        if self._is_disabled():
            return []
        try:
            pattern = f"{self.key_prefix}*"
            prefix_len = len(self.key_prefix)
            raw_keys = list(self.client.scan_iter(match=pattern))
            self._record_success()
            result = []
            for k in raw_keys:
                if isinstance(k, bytes):
                    k = k.decode("utf-8", errors="replace")
                result.append(k[prefix_len:])
            return result
        except Exception as exc:
            self._record_failure("keys", exc)
            return []

    def get_ttl(self, key: str) -> Optional[int]:
        if self._is_disabled():
            return None
        try:
            ttl = self.client.ttl(self._k(key))
            self._record_success()
            # Redis returns -2 if key doesn't exist, -1 if no TTL set
            if ttl is None or ttl < 0:
                return None
            return int(ttl)
        except Exception as exc:
            self._record_failure("get_ttl", exc)
            return None

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


class SQLiteCacheBackend(CacheBackend):
    """SQLite cache backend with TTL support."""

    def __init__(self, db_path: str, key_prefix: str = "nbx:", default_ttl: int = 300) -> None:
        self.db_path = db_path
        self.key_prefix = key_prefix
        self.default_ttl = max(int(default_ttl), 1)
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL,
                expires_at INTEGER NOT NULL
            )
            """
        )
        self.conn.commit()

    def _k(self, key: str) -> str:
        return f"{self.key_prefix}{key}"

    @staticmethod
    def _now() -> int:
        return int(time.time())

    def get(self, key: str) -> Any:
        full_key = self._k(key)
        now = self._now()
        with self.lock:
            row = self.conn.execute(
                "SELECT value, expires_at FROM cache_entries WHERE key = ?",
                (full_key,),
            ).fetchone()
            if not row:
                return None

            blob, expires_at = row
            if expires_at <= now:
                self.conn.execute("DELETE FROM cache_entries WHERE key = ?", (full_key,))
                self.conn.commit()
                return None

        try:
            return pickle.loads(blob)
        except Exception:
            logger.warning("Failed to unpickle SQLite cache key=%s; deleting entry", key)
            self.delete(key)
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        ttl = self.default_ttl if ttl_seconds is None else max(int(ttl_seconds), 1)
        expires_at = self._now() + ttl
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        full_key = self._k(key)

        with self.lock:
            self.conn.execute(
                """
                INSERT INTO cache_entries(key, value, expires_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    expires_at = excluded.expires_at
                """,
                (full_key, blob, expires_at),
            )
            self.conn.commit()

    def delete(self, key: str) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM cache_entries WHERE key = ?", (self._k(key),))
            self.conn.commit()

    def delete_prefix(self, key_prefix: str) -> None:
        like_expr = f"{self._k(key_prefix)}%"
        with self.lock:
            self.conn.execute("DELETE FROM cache_entries WHERE key LIKE ?", (like_expr,))
            self.conn.commit()

    def clear(self) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM cache_entries")
            self.conn.commit()

    def count(self) -> int:
        with self.lock:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM cache_entries WHERE expires_at > ?",
                (self._now(),),
            ).fetchone()
            return int(row[0]) if row else 0

    def keys(self) -> list:
        prefix_len = len(self.key_prefix)
        now = self._now()
        with self.lock:
            rows = self.conn.execute(
                "SELECT key FROM cache_entries WHERE expires_at > ?",
                (now,),
            ).fetchall()
        return [row[0][prefix_len:] for row in rows]

    def get_ttl(self, key: str) -> Optional[int]:
        full_key = self._k(key)
        now = self._now()
        with self.lock:
            row = self.conn.execute(
                "SELECT expires_at FROM cache_entries WHERE key = ? AND expires_at > ?",
                (full_key, now),
            ).fetchone()
        if not row:
            return None
        return max(0, int(row[0]) - now)

    def cleanup_expired(self) -> int:
        with self.lock:
            cur = self.conn.execute("DELETE FROM cache_entries WHERE expires_at <= ?", (self._now(),))
            self.conn.commit()
            return int(cur.rowcount or 0)

    def close(self) -> None:
        with self.lock:
            self.conn.close()


class BackendAdapter(ABC):
    """Abstract backend adapter for CRUD operations."""

    def __init__(
        self,
        rate_limiter: RateLimiter,
        retry_attempts: int = 3,
        retry_initial_delay_seconds: float = 0.3,
        retry_backoff_factor: float = 2.0,
        retry_max_delay_seconds: float = 15.0,
        retry_jitter_seconds: float = 0.0,
        retry_on_4xx: Sequence[int] = (408, 409, 425, 429),
        retry_on_5xx: Sequence[int] = (502, 503, 504),
        retry_5xx_cooldown_seconds: float = 60.0,
    ) -> None:
        self.rate_limiter = rate_limiter
        self.retry_attempts = max(int(retry_attempts), 0)
        self.retry_initial_delay_seconds = max(float(retry_initial_delay_seconds), 0.0)
        self.retry_backoff_factor = max(float(retry_backoff_factor), 1.0)
        self.retry_max_delay_seconds = max(float(retry_max_delay_seconds), self.retry_initial_delay_seconds)
        self.retry_jitter_seconds = max(float(retry_jitter_seconds), 0.0)
        self.retry_on_4xx = {int(code) for code in retry_on_4xx}
        self.retry_on_5xx = {int(code) for code in retry_on_5xx}
        self.retry_5xx_cooldown_seconds = max(float(retry_5xx_cooldown_seconds), 0.0)

    @staticmethod
    def _extract_status_code(exc: Exception) -> Optional[int]:
        for attr_name in ("status_code", "status", "code"):
            value = getattr(exc, attr_name, None)
            if isinstance(value, int) and 100 <= value <= 599:
                return value

        req_obj = getattr(exc, "req", None)
        if req_obj is not None:
            for attr_name in ("status_code", "status", "code"):
                value = getattr(req_obj, attr_name, None)
                if isinstance(value, int) and 100 <= value <= 599:
                    return value

        response_obj = getattr(exc, "response", None)
        if response_obj is not None:
            value = getattr(response_obj, "status_code", None)
            if isinstance(value, int) and 100 <= value <= 599:
                return value

        match = re.search(r"\b([45]\d{2})\b", str(exc))
        if match:
            return int(match.group(1))
        return None

    def _should_retry_exception(self, exc: Exception) -> bool:
        status_code = self._extract_status_code(exc)
        if status_code is not None:
            if status_code in self.retry_on_5xx:
                return True
            if status_code in self.retry_on_4xx:
                return True
            return False

        text = str(exc).lower()
        transient_markers = (
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "max retries exceeded",
            "unavailable",
            "deadline_exceeded",
        )
        return any(marker in text for marker in transient_markers)

    def _compute_backoff(self, attempt: int) -> float:
        delay = self.retry_initial_delay_seconds * (self.retry_backoff_factor ** attempt)
        delay = min(delay, self.retry_max_delay_seconds)
        if self.retry_jitter_seconds > 0:
            delay += random.uniform(0.0, self.retry_jitter_seconds)
        return delay

    def _call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        for attempt in range(self.retry_attempts + 1):
            self.rate_limiter.acquire()
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                should_retry = self._should_retry_exception(exc)
                if attempt >= self.retry_attempts or not should_retry:
                    raise

                status_code = self._extract_status_code(exc)
                sleep_seconds = self._compute_backoff(attempt)

                # For 5xx overload codes (503/504) trigger a shared cross-thread
                # cooldown so that ALL threads back off together.  This prevents the
                # thundering-herd where every thread independently wakes up after the
                # same short delay and immediately re-storms the server.
                if status_code in self.retry_on_5xx and self.retry_5xx_cooldown_seconds > 0:
                    self.rate_limiter.trigger_cooldown(self.retry_5xx_cooldown_seconds)

                logger.warning(
                    "Retrying API call after transient error (status=%s, attempt=%s/%s, sleep=%.2fs): %s",
                    status_code,
                    attempt + 1,
                    self.retry_attempts + 1,
                    sleep_seconds,
                    exc,
                )
                time.sleep(sleep_seconds)

        raise RuntimeError("Unexpected retry loop termination")

    @abstractmethod
    def get(self, resource: str, **filters: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def list(self, resource: str, **filters: Any) -> list[Any]:
        raise NotImplementedError

    @abstractmethod
    def create(self, resource: str, data: Mapping[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def update(self, resource: str, object_id: Any, data: Mapping[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def delete(self, resource: str, object_id: Any) -> bool:
        raise NotImplementedError


class PynetboxAdapter(BackendAdapter):
    """CRUD adapter backed by pynetbox REST API."""

    def __init__(
        self,
        url: str,
        token: str,
        rate_limiter: RateLimiter,
        retry_attempts: int = 3,
        retry_initial_delay_seconds: float = 0.3,
        retry_backoff_factor: float = 2.0,
        retry_max_delay_seconds: float = 15.0,
        retry_jitter_seconds: float = 0.0,
        retry_on_4xx: Sequence[int] = (408, 409, 425, 429),
        retry_5xx_cooldown_seconds: float = 60.0,
        branch: Optional[str] = None,
    ) -> None:
        super().__init__(
            rate_limiter=rate_limiter,
            retry_attempts=retry_attempts,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_backoff_factor=retry_backoff_factor,
            retry_max_delay_seconds=retry_max_delay_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
            retry_on_4xx=retry_on_4xx,
            retry_5xx_cooldown_seconds=retry_5xx_cooldown_seconds,
        )
        self.api = pynetbox.api(url=url, token=token)
        self.branch = branch.strip() if isinstance(branch, str) and branch.strip() else None
        if self.branch and hasattr(self.api, "http_session"):
            self.api.http_session.headers.update({"X-NetBox-Branch": self.branch})

    def _endpoint(self, resource: str) -> Any:
        endpoint = self.api
        for attr in resource.split("."):
            endpoint = getattr(endpoint, attr, None)
            if endpoint is None:
                raise ValueError(f"Unknown NetBox resource path: {resource}")
        return endpoint

    def get(self, resource: str, **filters: Any) -> Any:
        endpoint = self._endpoint(resource)
        return self._call(endpoint.get, **filters)

    def list(self, resource: str, **filters: Any) -> list[Any]:
        endpoint = self._endpoint(resource)
        # Use explicit pagination for all resources (can be narrowed if needed)
        PAGE_SIZE = 1000
        results = []
        offset = 0
        total = None
        seen_ids = set()
        # First, get the count (limit=0) to determine total records
        try:
            count_resp = self._call(endpoint.filter, limit=0, offset=0, **filters)
            if hasattr(count_resp, 'count'):
                total = count_resp.count
            elif isinstance(count_resp, dict) and 'count' in count_resp:
                total = count_resp['count']
        except Exception:
            total = None  # fallback to unknown total
        # If count is not available, just loop until no more results
        while True:
            page_attempts = 0
            while page_attempts <= self.retry_attempts:
                try:
                    page = self._call(endpoint.filter, limit=PAGE_SIZE, offset=offset, **filters)
                    page_items = list(page)
                    # Remove duplicates (can happen if NetBox data changes during paging)
                    new_items = [item for item in page_items if getattr(item, 'id', None) not in seen_ids]
                    for item in new_items:
                        item_id = getattr(item, 'id', None)
                        if item_id is not None:
                            seen_ids.add(item_id)
                    results.extend(new_items)
                    break
                except Exception as exc:
                    page_attempts += 1
                    should_retry = self._should_retry_exception(exc) if hasattr(self, '_should_retry_exception') else False
                    if page_attempts > self.retry_attempts or not should_retry:
                        raise
                    sleep_seconds = self._compute_backoff(page_attempts - 1) if hasattr(self, '_compute_backoff') else 1.0
                    logger.warning(
                        "Retrying NetBox page fetch after error (resource=%s, offset=%s, attempt=%s/%s, sleep=%.2fs): %s",
                        resource, offset, page_attempts, self.retry_attempts + 1, sleep_seconds, exc,
                    )
                    time.sleep(sleep_seconds)
            # If fewer than PAGE_SIZE returned, or we've reached total, we're done
            if total is not None and len(results) >= total:
                break
            if len(new_items) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return results

    def create(self, resource: str, data: Mapping[str, Any]) -> Any:
        endpoint = self._endpoint(resource)
        return self._call(endpoint.create, dict(data))

    def update(self, resource: str, object_id: Any, data: Mapping[str, Any]) -> Any:
        endpoint = self._endpoint(resource)
        record = self._call(endpoint.get, id=object_id)
        if record is None:
            return None
        for key, value in data.items():
            setattr(record, key, value)
        self._call(record.save)
        return record

    def delete(self, resource: str, object_id: Any) -> bool:
        endpoint = self._endpoint(resource)
        record = self._call(endpoint.get, id=object_id)
        if record is None:
            return False
        result = self._call(record.delete)
        return bool(result)


def _default_diode_entity_builder(resource: str, data: Mapping[str, Any]) -> Any:
    """Default mapper for a small set of known resource types."""

    logger.debug("[EntityBuilder] Building entity for resource=%r with data=%r", resource, data)
    try:
        ingester_module = importlib.import_module("netboxlabs.diode.sdk.ingester")
    except ImportError as exc:
        raise RuntimeError("netboxlabs-diode-sdk is required for Diode support") from exc

    DiodeEntity = getattr(ingester_module, "Entity")
    normalized = resource.strip().lower()

    # Mapping from NetBox resource to Diode entity attribute
    resource_to_entity = {
        # DCIM
        "dcim.devices": "device",
        "dcim.device_types": "device_type",
        "dcim.interfaces": "interface",
        "dcim.racks": "rack",
        "dcim.regions": "region",
        "dcim.sites": "site",
        "dcim.locations": "location",
        "dcim.manufacturers": "manufacturer",
        "dcim.device_roles": "device_role",
        "dcim.platforms": "platform",
        "dcim.virtual_chassis": "virtual_chassis",
        "dcim.inventory_items": "inventory_item",
        "dcim.inventory_item_roles": "inventory_item_role",
        "dcim.device_bays": "device_bay",
        "dcim.module_types": "module_type",
        "dcim.module_type_profiles": "module_type_profile",
        "dcim.modules": "module",
        "dcim.module_bays": "module_bay",
        "dcim.module_bay_templates": "module_bay_template",
        "dcim.power_feeds": "power_feed",
        "dcim.power_outlets": "power_outlet",
        "dcim.power_panels": "power_panel",
        "dcim.power_ports": "power_port",
        "dcim.console_ports": "console_port",
        "dcim.console_server_ports": "console_server_port",
        "dcim.front_ports": "front_port",
        "dcim.rear_ports": "rear_port",
        "dcim.virtual_device_contexts": "virtual_device_context",
        "dcim.mac_addresses": "mac_address",
        "dcim.cables": "cable",
        "dcim.virtual_chassis": "virtual_chassis",
        # IPAM
        "ipam.ip_addresses": "ip_address",
        "ipam.prefixes": "prefix",
        "ipam.vlans": "vlan",
        "ipam.vlan_groups": "vlan_group",
        "ipam.vrfs": "vrf",
        "ipam.rirs": "rir",
        "ipam.fhrp_groups": "fhrp_group",
        "ipam.route_targets": "route_target",
        "ipam.ip_ranges": "ip_range",
        "ipam.services": "service",
        "ipam.roles": "role",
        # Virtualization
        "virtualization.clusters": "cluster",
        "virtualization.cluster_groups": "cluster_group",
        "virtualization.cluster_types": "cluster_type",
        "virtualization.virtual_machines": "virtual_machine",
        "virtualization.virtual_machine_interfaces": "vm_interface",
        "virtualization.virtual_disks": "virtual_disk",
        "virtualization.interfaces": "vm_interface",
        # Wireless
        "wireless.wireless_lan_groups": "wireless_lan_group",
        "wireless.wireless_lans": "wireless_lan",
        "wireless.wireless_links": "wireless_link",
        # Extras
        "extras.config_contexts": "config_context",
        "extras.config_templates": "config_template",
        "extras.custom_fields": "custom_field",
        "extras.custom_links": "custom_link",
        "extras.journal_entries": "journal_entry",
        "extras.tags": "tag",
        "extras.webhooks": "webhook",
        # Tenancy
        "tenancy.contact_groups": "contact_group",
        "tenancy.contact_roles": "contact_role",
        "tenancy.contacts": "contact",
        "tenancy.tenants": "tenant",
        "tenancy.tenant_groups": "tenant_group",
        # Users
        "users.groups": "group",
        # VPN
        "vpn.ike_policies": "ike_policy",
        "vpn.ike_proposals": "ike_proposal",
        "vpn.ipsec_policies": "ipsec_policy",
        "vpn.ipsec_profiles": "ipsec_profile",
        "vpn.ipsec_proposals": "ipsec_proposal",
        "vpn.l2vpn_terminations": "l2vpn_termination",
        "vpn.l2vpns": "l2vpn",
        "vpn.tunnel_groups": "tunnel_group",
        "vpn.tunnel_terminations": "tunnel_termination",
        "vpn.tunnels": "tunnel",
        "vpn.tunnel_termination_groups": "tunnel_termination_group",
    }

    # Try direct mapping
    entity_attr = resource_to_entity.get(normalized)
    if entity_attr:
        entity_cls = getattr(ingester_module, entity_attr[0].upper() + entity_attr[1:], None)
        # Remove non-NetBox fields and Diode-unsupported fields
        filtered_keys = {k for k in data.keys() if k not in ("resource", "status", "payload")}
        entity_data = {k: data[k] for k in filtered_keys}

        # Map assigned_object_id to correct assigned_object_* field for IPAddress
        if normalized == "ipam.ip_addresses" and "assigned_object_id" in data:
            assigned_id = data["assigned_object_id"]
            obj_type = str(data.get("assigned_object_type", "")).lower()
            type_map = {
                "dcim.interface": "assigned_object_interface",
                "virtualization.vminterface": "assigned_object_vm_interface",
                "ipam.fhrpgroup": "assigned_object_fhrp_group",
            }
            field = type_map.get(obj_type)
            if field:
                entity_data[field] = {"id": assigned_id}
            entity_data.pop("assigned_object_id", None)
            entity_data.pop("assigned_object_type", None)

        # Unwrap related fields if they are NetBox upsert result dicts
        def unwrap_related(val):
            if isinstance(val, dict):
                # Recursively unwrap NetBox upsert result dicts
                if set(val.keys()) >= {"resource", "status", "payload"}:
                    payload = val.get("payload")
                    return unwrap_related(payload)
                # If this is a dict with only 'id', return the id
                if set(val.keys()) == {"id"}:
                    return val["id"]
                # Recursively unwrap all dict values
                return {k: unwrap_related(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [unwrap_related(item) for item in val]
            return val

        # Apply recursive unwrapping to all fields in entity_data
        for k in list(entity_data.keys()):
            entity_data[k] = unwrap_related(entity_data[k])

        logger.debug("[Diode entity builder] resource=%s entity_data(after unwrap)=%r", resource, entity_data)

        # Recursively remap scoping fields for supported entities
        scope_map = {
            "site": "scope_site",
            "tenant": "scope_tenant",
        }
        scope_supported_entities = {
            "virtualization.clusters",
            "virtualization.virtual_machines",
            "ipam.vlans",
            "tenancy.tenants",
        }
        def remap_scoping_fields(data, resource_type):
            if isinstance(data, dict):
                norm = resource_type.lower() if resource_type else ""
                # Recursively apply to nested dicts BEFORE removing 'resource'
                for k, v in data.items():
                    # Use resource_to_entity mapping for all keys
                    nested_resource = v.get("resource") if isinstance(v, dict) and "resource" in v else (
                        next((r for r, ent in resource_to_entity.items() if ent == k), None) if k in resource_to_entity else None
                    )
                    data[k] = remap_scoping_fields(v, nested_resource)
                if norm in scope_supported_entities:
                    for direct, scoped in scope_map.items():
                        if direct in data:
                            data[scoped] = data.pop(direct)
                else:
                    data.pop("scope_site", None)
                    data.pop("scope_tenant", None)
                data.pop("resource", None)
            elif isinstance(data, list):
                return [remap_scoping_fields(item, resource_type) for item in data]
            return data

        entity_data = remap_scoping_fields(entity_data, normalized)

        def _is_empty_entity(obj):
            # Recursively check if obj is empty or only contains empty/nested empty values
            if obj is None:
                return True
            if isinstance(obj, dict):
                return all(_is_empty_entity(v) for v in obj.values()) or not obj
            if isinstance(obj, (list, tuple, set)):
                return all(_is_empty_entity(v) for v in obj) or not obj
            return False

        if _is_empty_entity(entity_data):
            logger.debug("[EntityBuilder] Skipping empty entity for resource=%r: %r", resource, entity_data)
            return None

        if entity_cls:
            # Filter out fields not supported by the proto class
            import inspect
            valid_fields = set(inspect.signature(entity_cls.__init__).parameters.keys())
            valid_fields.discard('self')
            filtered_entity_data = {k: v for k, v in entity_data.items() if k in valid_fields}
            logger.debug("[EntityBuilder] Built entity for resource=%r: %r", resource, filtered_entity_data)
            return DiodeEntity(**{entity_attr: entity_cls(**filtered_entity_data)})
        # Fallback: use dict directly if class is missing
        logger.debug("[EntityBuilder] Built entity for resource=%r: %r", resource, entity_data)
        return DiodeEntity(**{entity_attr: entity_data})

    # Fallback for common aliases
    alias_map = {
        "ip_address": "ipam.ip_addresses",
        "prefix": "ipam.prefixes",
        "mac_address": "dcim.mac_addresses",
        "device": "dcim.devices",
        "cluster": "virtualization.clusters",
        "virtual_machine": "virtualization.virtual_machines",
        "vm_interface": "virtualization.virtual_machine_interfaces",
        "virtual_disk": "virtualization.virtual_disks",
    }
    if normalized in alias_map:
        mapped_resource = alias_map[normalized]
        entity_attr = resource_to_entity.get(mapped_resource)
        if entity_attr:
            entity_cls = getattr(ingester_module, entity_attr[0].upper() + entity_attr[1:], None)
            if entity_cls:
                return DiodeEntity(**{entity_attr: entity_cls(**dict(data))})
            # Fallback: use dict directly if class is missing
            return DiodeEntity(**{entity_attr: dict(data)})

    raise ValueError(
        "No default Diode entity mapping for resource "
        f"'{resource}'. Provide diode_entity_builder in constructor."
    )


class DiodeAdapter(BackendAdapter):
    def _flush_entity_buffer(self):
        if not self._entity_buffer:
            return
        with self._diode_client_cls(**self._client_kwargs()) as client:
            try:
                if self.branch:
                    try:
                        response = self._call(client.ingest, entities=self._entity_buffer, branch=self.branch)
                    except TypeError:
                        if not self._branch_unsupported_warned:
                            logger.warning(
                                "Diode SDK ingest does not support branch argument; proceeding without branch routing"
                            )
                            self._branch_unsupported_warned = True
                        response = self._call(client.ingest, entities=self._entity_buffer)
                else:
                    response = self._call(client.ingest, entities=self._entity_buffer)
                errors = getattr(response, "errors", None)
                if errors:
                    raise RuntimeError(f"Diode ingest failed: {errors}")
                logger.debug("Flushed %d entities to Diode", len(self._entity_buffer))
            finally:
                self._entity_buffer.clear()
    """Adapter that writes through Diode and intentionally treats reads as misses."""

    def __init__(
        self,
        target: str,
        client_id: str,
        client_secret: str,
        cert_file: Optional[str],
        skip_tls_verify: bool,
        rate_limiter: RateLimiter,
        retry_attempts: int = 3,
        retry_initial_delay_seconds: float = 0.3,
        retry_backoff_factor: float = 2.0,
        retry_max_delay_seconds: float = 15.0,
        retry_jitter_seconds: float = 0.0,
        retry_on_4xx: Sequence[int] = (408, 409, 425, 429),
        retry_5xx_cooldown_seconds: float = 60.0,
        branch: Optional[str] = None,
        entity_builder: Optional[Callable[[str, Mapping[str, Any]], Any]] = None,
        read_fallback: Optional[PynetboxAdapter] = None,
        batch_size: int = 1,
    ) -> None:
        super().__init__(
            rate_limiter=rate_limiter,
            retry_attempts=retry_attempts,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_backoff_factor=retry_backoff_factor,
            retry_max_delay_seconds=retry_max_delay_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
            retry_on_4xx=retry_on_4xx,
            retry_5xx_cooldown_seconds=retry_5xx_cooldown_seconds,
        )
        self.target = target
        self.client_id = client_id
        self.client_secret = client_secret
        self.cert_file = cert_file
        self.skip_tls_verify = skip_tls_verify
        self.branch = branch.strip() if isinstance(branch, str) and branch.strip() else None
        self._branch_unsupported_warned = False
        self.entity_builder = entity_builder or _default_diode_entity_builder
        self.read_fallback = read_fallback
        self._batch_size = batch_size
        self._entity_buffer = []

        try:
            sdk_module = importlib.import_module("netboxlabs.diode.sdk")
        except ImportError as exc:
            raise RuntimeError("netboxlabs-diode-sdk is required for Diode backend") from exc

        self._diode_client_cls = getattr(sdk_module, "DiodeClient")

    def _client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "target": self.target,
            "app_name": "pynetbox2",
            "app_version": "1.0.0",
        }
        if self.client_id:
            kwargs["client_id"] = self.client_id
        if self.client_secret:
            kwargs["client_secret"] = self.client_secret
        if self.cert_file:
            kwargs["cert_file"] = self.cert_file
        if self.skip_tls_verify:
            kwargs["skip_tls_verify"] = True
        return kwargs

    def _ingest(self, resource: str, data: Mapping[str, Any]) -> Any:
        logger.debug("[Ingest] Entering _ingest for resource=%r with data=%r", resource, data)
        entity = self.entity_builder(resource, data)
        if entity is None:
            logger.debug("[Ingest] Skipping empty entity for resource=%r", resource)
            return
        self._entity_buffer.append(entity)
        logger.debug("[Ingest] Added entity to buffer (size now %d) for resource=%r", len(self._entity_buffer), resource)
        if len(self._entity_buffer) >= self._batch_size:
            logger.debug("[Ingest] Buffer reached batch size (%d), flushing", self._batch_size)
            self._flush_entity_buffer()

    def get(self, resource: str, **filters: Any) -> Any:
        logger.debug(
            "Diode get returns empty result resource=%s filters=%s",
            resource,
            filters,
        )
        return None

    def list(self, resource: str, **filters: Any) -> list[Any]:
        logger.debug(
            "Diode list returns empty result resource=%s filters=%s",
            resource,
            filters,
        )
        return []

    def create(self, resource: str, data: Mapping[str, Any]) -> Any:
        self._ingest(resource, data)
        # If batch size is 1, flush immediately for legacy behavior
        if self._batch_size == 1:
            self._flush_entity_buffer()
        return {"resource": resource, "status": "ingested", "payload": dict(data)}

    def flush(self):
        """Manually flush any remaining entities in the buffer."""
        self._flush_entity_buffer()

    def update(self, resource: str, object_id: Any, data: Mapping[str, Any]) -> Any:
        payload = dict(data)
        payload.setdefault("id", object_id)
        self._ingest(resource, payload)
        return {"resource": resource, "id": object_id, "status": "ingested", "payload": payload}

    def delete(self, resource: str, object_id: Any) -> bool:
        raise NotImplementedError("Diode backend delete operation is not implemented")


@dataclass
class NetBoxExtendedConfig:
    """Configuration container for NetBoxExtendedClient."""

    url: str
    token: str
    branch: Optional[str] = None

    backend: str = "pynetbox"  # "pynetbox" or "diode"

    cache_backend: str = "none"  # "none", "redis", "sqlite"
    cache_ttl_seconds: int = 300
    cache_key_prefix: str = "netbox:"

    redis_url: str = "redis://localhost:6379/0"

    sqlite_path: str = ".nbx_cache.sqlite3"

    rate_limit_per_second: float = 0.0
    rate_limit_burst: int = 1

    retry_attempts: int = 3
    retry_initial_delay_seconds: float = 0.3
    retry_backoff_factor: float = 2.0
    retry_max_delay_seconds: float = 15.0
    retry_jitter_seconds: float = 0.0
    retry_on_4xx: Sequence[int] = (408, 409, 425, 429)
    retry_5xx_cooldown_seconds: float = 60.0
    prewarm_sentinel_ttl_seconds: Optional[int] = None

    diode_target: str = "grpcs://localhost:8080"
    diode_client_id: str = ""
    diode_client_secret: str = ""
    diode_cert_file: Optional[str] = None
    diode_skip_tls_verify: bool = False
    diode_read_fallback: bool = False
    diode_entity_builder: Optional[Callable[[str, Mapping[str, Any]], Any]] = None
    diode_batch_size: int = 1  # Default: 1 (no batching)


class CachedEndpoint:
    """Resource-scoped CRUD wrapper bound to NetBoxExtendedClient."""

    def __init__(self, client: "NetBoxExtendedClient", resource: str) -> None:
        self.client = client
        self.resource = resource

    def get(self, use_cache: bool = True, **filters: Any) -> Any:
        return self.client.get(self.resource, use_cache=use_cache, **filters)

    def list(self, use_cache: bool = True, **filters: Any) -> list[Any]:
        return self.client.list(self.resource, use_cache=use_cache, **filters)

    def create(self, data: Mapping[str, Any]) -> Any:
        return self.client.create(self.resource, data)

    def update(self, object_id: Any, data: Mapping[str, Any]) -> Any:
        return self.client.update(self.resource, object_id, data)

    def upsert(
        self,
        data: Mapping[str, Any],
        *,
        lookup_fields: Optional[Sequence[str]] = None,
        preserve_fields: Optional[Sequence[str]] = None,
        use_cache_for_lookup: bool = False,
    ) -> Any:
        return self.client.upsert(
            self.resource,
            data,
            lookup_fields=lookup_fields,
            preserve_fields=preserve_fields,
            use_cache_for_lookup=use_cache_for_lookup,
        )
    def prewarm(self, **filters: Any) -> int:
        summary = self.client.prewarm({self.resource: filters})
        return int(summary.get(self.resource, 0))


class _CompatNode:
    """Dynamic namespace/endpoint proxy for pynetbox-style attribute access."""

    def __init__(
        self,
        client: "NetBoxAPI",
        path_parts: list[str],
        raw_obj: Optional[Any] = None,
    ) -> None:
        self._client = client
        self._path_parts = path_parts
        self._raw_obj = raw_obj
    def _resource(self) -> str:
        if not self._path_parts:
            raise ValueError("Resource path cannot be empty")
        return ".".join(self._path_parts)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        if self._raw_obj is not None:
            attr = getattr(self._raw_obj, name, None)
            if attr is not None:
                if callable(attr):
                    def throttled(*args: Any, **kwargs: Any) -> Any:
                        adapter = getattr(self._client, "adapter", None)
                        if adapter is not None and hasattr(adapter, "_call"):
                            return adapter._call(attr, *args, **kwargs)
                        self._client.rate_limiter.acquire()
                        return attr(*args, **kwargs)

                    return throttled
                return attr

        return _CompatNode(self._client, [*self._path_parts, name], None)

    def get(self, **filters: Any) -> Any:
        return self._client.get(self._resource(), **filters)

    def filter(self, **filters: Any) -> list[Any]:
        return self._client.list(self._resource(), **filters)

    def all(self) -> list[Any]:
        return self._client.list(self._resource())

    def create(self, data: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Any:
        payload = dict(data or {})
        payload.update(kwargs)
        return self._client.create(self._resource(), payload)

    def update(self, object_id: Any, data: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Any:
        payload = dict(data or {})
        payload.update(kwargs)
        return self._client.update(self._resource(), object_id, payload)

    def upsert(
        self,
        data: Optional[Mapping[str, Any]] = None,
        *,
        lookup_fields: Optional[Sequence[str]] = None,
        preserve_fields: Optional[Sequence[str]] = None,
        use_cache_for_lookup: bool = False,
        **kwargs: Any,
    ) -> Any:
        payload = dict(data or {})
        payload.update(kwargs)
        return self._client.upsert(
            self._resource(),
            payload,
            lookup_fields=lookup_fields,
            preserve_fields=preserve_fields,
            use_cache_for_lookup=use_cache_for_lookup,
        )

    def delete(self, object_id: Any) -> bool:
        return self._client.delete(self._resource(), object_id)

    def prewarm(self, **filters: Any) -> int:
        summary = self._client.prewarm({self._resource(): filters})
        return int(summary.get(self._resource(), 0))

class NetBoxExtendedClient:
    """High-level NetBox client with cache, throttle, and backend pluggability."""

    _RESOURCE_TO_PRECACHE_OBJECT_TYPE: dict[str, str] = {
        "circuits.circuit_types": "circuittype",
        "circuits.circuits": "circuit",
        "circuits.providers": "provider",
        "circuits.provider_accounts": "provideraccount",
        "core.data_sources": "datasource",
        "dcim.cables": "cable",
        "dcim.console_ports": "consoleport",
        "dcim.console_server_ports": "consoleserverport",
        "ipam.prefixes": "prefix",
        "ipam.ip_addresses": "ip_address",
        "ipam.ip_ranges": "iprange",
        "ipam.fhrp_groups": "fhrpgroup",
        "ipam.route_targets": "routetarget",
        "ipam.rirs": "rir",
        "ipam.service_templates": "servicetemplate",
        "ipam.services": "service",
        "ipam.vrfs": "vrf",
        "ipam.roles": "role",
        "dcim.mac_addresses": "mac_address",
        "dcim.racks": "rack",
        "dcim.regions": "region",
        "dcim.site_groups": "sitegroup",
        "dcim.sites": "site",
        "dcim.locations": "location",
        "dcim.device_roles": "devicerole",
        "dcim.manufacturers": "manufacturer",
        "dcim.module_bays": "modulebay",
        "dcim.module_type_profiles": "moduletypeprofile",
        "dcim.module_types": "moduletype",
        "dcim.modules": "module",
        "dcim.module_bay_templates": "modulebaytemplates",
        "dcim.power_feeds": "powerfeed",
        "dcim.power_outlets": "poweroutlet",
        "dcim.power_panels": "powerpanel",
        "dcim.power_ports": "powerport",
        "dcim.rear_ports": "rearport",
        "dcim.front_ports": "frontport",
        "dcim.virtual_chassis": "virtualchassis",
        "dcim.device_bays": "devicebay",
        "dcim.inventory_items": "inventoryitem",
        "dcim.inventory_item_roles": "inventoryitemrole",
        "dcim.virtual_device_contexts": "virtualdevicecontext",
        "dcim.platforms": "platform",
        "extras.config_contexts": "configcontext",
        "extras.config_templates": "configtemplate",
        "extras.custom_fields": "customfield",
        "extras.custom_links": "customlink",
        "extras.journal_entries": "journalentry",
        "extras.tags": "tag",
        "extras.webhooks": "webhook",
        "tenancy.contact_groups": "contactgroup",
        "tenancy.contact_roles": "contactrole",
        "tenancy.contacts": "contact",
        "tenancy.tenants": "tenant",
        "tenancy.tenant_groups": "tenantgroup",
        "users.groups": "group",
        "dcim.devices": "device",
        "dcim.device_types": "devicetype",
        "dcim.interfaces": "interface",
        "virtualization.virtual_machine_interfaces": "vminterface",
        "virtualization.virtual_machines": "virtualmachine",
        "virtualization.interfaces": "vminterface",
        "virtualization.virtual_disks": "virtualdisk",
        "ipam.vlans": "vlan",
        "ipam.vlan_groups": "vlangroup",
        "wireless.wireless_lan_groups": "wirelesslangroup",
        "wireless.wireless_lans": "wirelesslan",
        "wireless.wireless_links": "wirelesslink",
        "virtualization.clusters": "cluster",
        "virtualization.cluster_groups": "clustergroup",
        "virtualization.cluster_types": "clustertype",
        "vpn.ike_policies": "ikepolicy",
        "vpn.ike_proposals": "ikeproposal",
        "vpn.ipsec_policies": "ipsecpolicy",
        "vpn.ipsec_profiles": "ipsecprofile",
        "vpn.ipsec_proposals": "ipsecproposal",
        "vpn.l2vpn_terminations": "l2vpntermination",
        "vpn.l2vpns": "l2vpn",
        "vpn.tunnel_groups": "tunnelgroup",
        "vpn.tunnel_terminations": "tunneltermination",
        "vpn.tunnels": "tunnel",
        "vpn.tunnel_termination_groups": "tunnelterminationgroup",
    }

    def __init__(
        self,
        url: str,
        token: str,
        branch: Optional[str] = None,
        *,
        backend: str = "pynetbox",
        cache_backend: str = "none",
        cache_ttl_seconds: int = 300,
        cache_key_prefix: str = "nbx:",
        redis_url: str = "redis://localhost:6379/0",
        sqlite_path: str = ".nbx_cache.sqlite3",
        rate_limit_per_second: float = 0.0,
        rate_limit_burst: int = 1,
        retry_attempts: int = 3,
        retry_initial_delay_seconds: float = 0.3,
        retry_backoff_factor: float = 2.0,
        retry_max_delay_seconds: float = 15.0,
        retry_jitter_seconds: float = 0.0,
        retry_on_4xx: Sequence[int] = (408, 409, 425, 429),
        retry_5xx_cooldown_seconds: float = 60.0,
        prewarm_sentinel_ttl_seconds: Optional[int] = None,
        diode_target: str = "grpcs://localhost:8080",
        diode_client_id: str = "",
        diode_client_secret: str = "",
        diode_cert_file: Optional[str] = None,
        diode_skip_tls_verify: bool = False,
        diode_read_fallback: bool = False,
        diode_entity_builder: Optional[Callable[[str, Mapping[str, Any]], Any]] = None,
        diode_batch_size: int = 1,
    ) -> None:
        self.config = NetBoxExtendedConfig(
            url=url,
            token=token,
            branch=branch,
            backend=backend,
            cache_backend=cache_backend,
            cache_ttl_seconds=cache_ttl_seconds,
            cache_key_prefix=cache_key_prefix,
            redis_url=redis_url,
            sqlite_path=sqlite_path,
            rate_limit_per_second=rate_limit_per_second,
            rate_limit_burst=rate_limit_burst,
            retry_attempts=retry_attempts,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_backoff_factor=retry_backoff_factor,
            retry_max_delay_seconds=retry_max_delay_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
            retry_on_4xx=retry_on_4xx,
            retry_5xx_cooldown_seconds=retry_5xx_cooldown_seconds,
            prewarm_sentinel_ttl_seconds=prewarm_sentinel_ttl_seconds,
            diode_target=diode_target,
            diode_client_id=diode_client_id,
            diode_client_secret=diode_client_secret,
            diode_cert_file=diode_cert_file,
            diode_skip_tls_verify=diode_skip_tls_verify,
            diode_read_fallback=diode_read_fallback,
            diode_entity_builder=diode_entity_builder,
            diode_batch_size=diode_batch_size,
        )

        self.rate_limiter = RateLimiter(
            calls_per_second=self.config.rate_limit_per_second,
            burst=self.config.rate_limit_burst,
        )
        if self.config.rate_limit_per_second > 0:
            logger.info(
                "NetBox rate limiting enabled: %.1f calls/s (burst=%d)",
                self.config.rate_limit_per_second,
                self.config.rate_limit_burst,
            )
        self._cache_metrics_lock = threading.Lock()
        self._cache_metrics: dict[str, int] = {
            "get_hits": 0,
            "get_misses": 0,
            "get_bypass": 0,
            "list_hits": 0,
            "list_misses": 0,
            "list_bypass": 0,
        }
        self._cache_key_locks_guard = threading.Lock()
        self._cache_key_locks: dict[str, threading.Lock] = {}
        self.cache = self._build_cache_backend(self.config)
        self.adapter = self._build_backend_adapter(self.config)

    def _inc_cache_metric(self, key: str) -> None:
        with self._cache_metrics_lock:
            self._cache_metrics[key] = self._cache_metrics.get(key, 0) + 1

    def _cache_metric_snapshot(self) -> dict[str, int]:
        with self._cache_metrics_lock:
            return dict(self._cache_metrics)

    def _cache_metric_delta(self, before: Mapping[str, int], after: Mapping[str, int]) -> dict[str, int]:
        keys = set(before.keys()) | set(after.keys())
        return {k: int(after.get(k, 0)) - int(before.get(k, 0)) for k in sorted(keys)}

    def _get_cache_key_lock(self, cache_key: str) -> threading.Lock:
        with self._cache_key_locks_guard:
            lock = self._cache_key_locks.get(cache_key)
            if lock is None:
                lock = threading.Lock()
                self._cache_key_locks[cache_key] = lock
            return lock

    def _build_cache_backend(self, cfg: NetBoxExtendedConfig) -> CacheBackend:
        backend = cfg.cache_backend.strip().lower()
        if backend == "none":
            return NullCacheBackend()
        if backend == "redis":
            return RedisCacheBackend(
                url=cfg.redis_url,
                key_prefix=cfg.cache_key_prefix,
                default_ttl=cfg.cache_ttl_seconds,
            )
        if backend == "sqlite":
            return SQLiteCacheBackend(
                db_path=cfg.sqlite_path,
                key_prefix=cfg.cache_key_prefix,
                default_ttl=cfg.cache_ttl_seconds,
            )
        raise ValueError(f"Unsupported cache_backend '{cfg.cache_backend}'")

    def _build_backend_adapter(self, cfg: NetBoxExtendedConfig) -> BackendAdapter:
        normalized_backend = cfg.backend.strip().lower()
        if normalized_backend == "pynetbox":
            return PynetboxAdapter(
                cfg.url,
                cfg.token,
                self.rate_limiter,
                retry_attempts=cfg.retry_attempts,
                retry_initial_delay_seconds=cfg.retry_initial_delay_seconds,
                retry_backoff_factor=cfg.retry_backoff_factor,
                retry_max_delay_seconds=cfg.retry_max_delay_seconds,
                retry_jitter_seconds=cfg.retry_jitter_seconds,
                retry_on_4xx=cfg.retry_on_4xx,
                retry_5xx_cooldown_seconds=cfg.retry_5xx_cooldown_seconds,
                branch=cfg.branch,
            )

        if normalized_backend == "diode":
            read_fallback = None
            if cfg.diode_read_fallback:
                read_fallback = PynetboxAdapter(
                    cfg.url,
                    cfg.token,
                    self.rate_limiter,
                    retry_attempts=cfg.retry_attempts,
                    retry_initial_delay_seconds=cfg.retry_initial_delay_seconds,
                    retry_backoff_factor=cfg.retry_backoff_factor,
                    retry_max_delay_seconds=cfg.retry_max_delay_seconds,
                    retry_jitter_seconds=cfg.retry_jitter_seconds,
                    retry_on_4xx=cfg.retry_on_4xx,
                    retry_5xx_cooldown_seconds=cfg.retry_5xx_cooldown_seconds,
                    branch=cfg.branch,
                )
            return DiodeAdapter(
                target=cfg.diode_target,
                client_id=cfg.diode_client_id,
                client_secret=cfg.diode_client_secret,
                cert_file=cfg.diode_cert_file,
                skip_tls_verify=cfg.diode_skip_tls_verify,
                rate_limiter=self.rate_limiter,
                retry_attempts=cfg.retry_attempts,
                retry_initial_delay_seconds=cfg.retry_initial_delay_seconds,
                retry_backoff_factor=cfg.retry_backoff_factor,
                retry_max_delay_seconds=cfg.retry_max_delay_seconds,
                retry_jitter_seconds=cfg.retry_jitter_seconds,
                retry_on_4xx=cfg.retry_on_4xx,
                retry_5xx_cooldown_seconds=cfg.retry_5xx_cooldown_seconds,
                branch=cfg.branch,
                entity_builder=cfg.diode_entity_builder,
                read_fallback=read_fallback,
                batch_size=getattr(cfg, 'diode_batch_size', 1),
            )

        raise ValueError(f"Unsupported backend '{cfg.backend}'")

    @staticmethod
    def _normalize_for_key(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: NetBoxExtendedClient._normalize_for_key(v) for k, v in sorted(value.items())}
        if isinstance(value, (list, tuple, set)):
            return [NetBoxExtendedClient._normalize_for_key(v) for v in value]
        if hasattr(value, "id"):
            return getattr(value, "id")
        return value

    def _cache_key(self, resource: str, operation: str, params: Mapping[str, Any]) -> str:
        normalized = self._normalize_for_key(dict(params))
        payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
        digest = sha256(payload.encode("utf-8")).hexdigest()
        return f"{resource}:{operation}:{digest}"

    def _external_prewarm_sentinel_key(self, resource: str, filters: Mapping[str, Any]) -> Optional[str]:
        # Shared sentinel keys are only defined for unfiltered full-resource prewarm.
        if filters:
            return None
        object_type = self._RESOURCE_TO_PRECACHE_OBJECT_TYPE.get(resource)
        if not object_type:
            return None
        return f"{self._PREWARM_SENTINEL_KEY_PREFIX}{object_type}"

    @staticmethod
    def _extract_id(record: Any) -> Optional[Any]:
        if record is None:
            return None
        if isinstance(record, dict):
            return record.get("id")
        return getattr(record, "id", None)

    @staticmethod
    def _record_field_value(record: Any, field: str) -> Any:
        if isinstance(record, dict):
            return record.get(field)
        return getattr(record, field, None)

    def _normalize_for_compare(value, resource=None, key=None):
        """
        Robust normalization for NetBox compare:
        - Dicts: sort keys, normalize values
        - Lists: sort if unordered, normalize elements
        - FKs: compare by id or name
        - MACs: lower, no separator
        - VLAN desc: split, regex, sort, rejoin
        - Tags: lower, sort
        - Numbers: int/float equivalence
        """
        
        normalize = NetBoxExtendedClient._normalize_for_compare
        
        # Helper: normalize MAC
        def norm_mac(val):
            if not isinstance(val, str):
                return val
            return val.upper()

        # Helper: normalize VLAN description
        def norm_vlan_desc(desc):
            if not isinstance(desc, str):
                return desc
            # Example: split on |, strip, regex replace, sort, rejoin
            segments = [s.strip() for s in desc.split('|')]
            # Optionally apply regex rules here (load from file if needed)
            segments = [re.sub(r'\\b\\d+\\b', '', s).strip() for s in segments]  # Example: remove numbers
            segments = sorted(filter(None, segments))
            return ' | '.join(segments)

        # Helper: normalize tags/lists
        def norm_list(lst):
            return sorted([normalize(x) for x in lst])

        # NetBox choice fields are returned as {"value": ..., "label": ...} dicts.
        # Normalize them to just the value string so that e.g. "active" compares
        # equal to {"value": "active", "label": "Active"} and spurious updates
        # for fields like status/face are avoided.
        if isinstance(value, dict) and "value" in value and "label" in value:
            return normalize(value["value"])

        # Dict normalization
        if isinstance(value, dict):
            normed = {}
            for k, v in sorted(value.items()):
                # FK normalization: id/name
                if k.endswith('_id') or k in ('site', 'tenant', 'role', 'device_type', 'platform', 'location', 'rack', 'cluster', 'group', 'type', 'vlan', 'vrf', 'assigned_object'):
                    # Always prefer id if present, else name, else original value
                    if isinstance(v, dict):
                        if 'id' in v and v['id'] is not None:
                            v = v['id']
                        elif 'name' in v and v['name'] is not None:
                            v = v['name']
                        else:
                            v = v
                    elif hasattr(v, 'id') and getattr(v, 'id', None) is not None:
                        v = getattr(v, 'id')
                    elif hasattr(v, 'name') and getattr(v, 'name', None) is not None:
                        v = getattr(v, 'name')
                # MAC normalization
                if k in ('mac_address', 'macaddress'):
                    v = norm_mac(v)
                # VLAN description normalization
                if k == 'description' and resource == 'ipam.vlans':
                    v = norm_vlan_desc(v)
                # Tags normalization
                if k in ('tags', 'tagged_vlans'):
                    v = norm_list(v)
                # Number normalization
                if isinstance(v, float) and v.is_integer():
                    v = int(v)
                normed[k] = normalize(v, resource=resource, key=k)
            return normed

        # List normalization
        if isinstance(value, (list, tuple, set)):
            return norm_list(value)

        # MAC normalization (standalone)
        if key in ('mac_address', 'macaddress'):
            return norm_mac(value)

        # VLAN description normalization (standalone)
        if key == 'description' and resource == 'ipam.vlans':
            return norm_vlan_desc(value)

        # Tags normalization (standalone)
        if key in ('tags', 'tagged_vlans'):
            return norm_list(value)

        # Number normalization
        if isinstance(value, float) and value.is_integer():
            return int(value)

        # Case-insensitive for tags
        if isinstance(value, str) and key in ('tags',):
            return value.lower()

        # Normalise FK objects (e.g. pynetbox Record instances) to their integer
        # ID so that comparisons against an integer FK value in the desired
        # payload do not produce a spurious diff.  We deliberately check that
        # the value is not one of the scalar/collection types already handled
        # above to avoid false positives.
        if (
            not isinstance(value, (int, float, str, bool, type(None), dict, list, tuple, set))
            and hasattr(value, "id")
        ):
            id_val = getattr(value, "id", None)
            if isinstance(id_val, int):
                return id_val

        return value

    def _build_existing_subset(self, existing: Any, keys: Sequence[str]) -> dict[str, Any]:
        subset: dict[str, Any] = {}
        for key in keys:
            subset[key] = self._normalize_for_compare(self._record_field_value(existing, key))
        return subset

    def _invalidate_resource_cache(self, resource: str) -> None:
        self.cache.delete_prefix(f"{resource}:")

    def _invalidate_resource_list_cache(self, resource: str) -> None:
        self.cache.delete_prefix(f"{resource}:list:")

    def _invalidate_resource_prewarm_sentinel(self, resource: str) -> None:
        logger.debug("Invalidating prewarm sentinel cache for resource=%s", resource)
        self.cache.delete_prefix(f"{resource}:prewarm_sentinel:")

    def _invalidate_get_cache_key(self, resource: str, filters: Mapping[str, Any]) -> None:
        key = self._cache_key(resource, "get", filters)
        self.cache.delete(key)

    def _set_get_cache_key(self, resource: str, filters: Mapping[str, Any], record: Any) -> None:
        if record is None:
            return
        key = self._cache_key(resource, "get", filters)
        self.cache.set(key, record)

    def _set_get_cache_by_id(self, resource: str, record: Any) -> None:
        object_id = self._extract_id(record)
        if object_id is None:
            return
        key = self._cache_key(resource, "get", {"id": object_id})
        self.cache.set(key, record)

    @staticmethod
    def _extract_related_id(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, dict):
            raw = value.get("id")
            return raw if isinstance(raw, int) else None
        raw = getattr(value, "id", None)
        return raw if isinstance(raw, int) else None

    def _derived_lookup_filters_for_record(self, resource: str, record: Any) -> list[dict[str, Any]]:
        """
        For a given record, generate all plausible lookup filter dicts for cache prewarm:
        - For each FK, generate both _id and non-_id forms (if possible)
        - For each FK, generate both id and name values (if available)
        - For all combinations of these forms
        - Always include single-field and multi-field lookups
        """
        filters: list[dict[str, Any]] = []
        # Define resource-specific lookup fields (extend as needed)
        resource_fields = {
            "dcim.devices": ["name", "site"],
            "dcim.device_roles": ["name", "slug"],
            "dcim.device_types": ["model", "manufacturer"],
            "dcim.interfaces": ["name", "device"],
            "dcim.locations": ["name", "site"],
            "dcim.manufacturers": ["name", "slug"],
            "dcim.module_bays": ["device", "name"],
            "dcim.module_bay_templates": ["device_type", "name"],
            "dcim.module_types": ["manufacturer", "slug"],
            "dcim.modules": ["device", "module_bay"],
            "dcim.platforms": ["name", "slug"],
            "dcim.racks": ["name", "site"],
            "dcim.sites": ["name", "slug"],
            "extras.tags": ["name", "slug"],
            "virtualization.clusters": ["name", "group"],
            "virtualization.virtual_machines": ["name", "cluster"],
            "virtualization.interfaces": ["name", "virtual_machine"],
            "virtualization.virtual_disks": ["name", "virtual_machine"],
            # Add more as needed
        }
        fields = resource_fields.get(resource, [])
        if not fields:
            return []

        # Gather all possible values for each field: id, name, object
        field_values = {}
        for field in fields:
            value = self._record_field_value(record, field)
            id_val = self._extract_related_id(value)
            name_val = None
            if isinstance(value, dict):
                name_val = value.get("name")
            elif isinstance(value, str):
                name_val = value
            elif value is not None:
                # Read "name" directly from __dict__ to avoid triggering pynetbox's
                # Record.__getattr__, which calls full_details() (an HTTP GET request)
                # when the attribute is not in the pre-loaded data. For example,
                # device_type Records have "model" but not "name"; accessing "name"
                # via hasattr/getattr would fetch the full object from the API once
                # per record during cache warm. 2026-03-31 #cache-warm
                name_val = getattr(value, "__dict__", {}).get("name")
            # Always include the raw value as well
            field_values[field] = {"id": id_val, "name": name_val, "raw": value}

        # Build all combinations of lookup fields (single and pairs)
        from itertools import combinations, product
        combos = []
        for r in range(1, len(fields) + 1):
            combos.extend(combinations(fields, r))

        for combo in combos:
            # For each field in combo, try all plausible forms (_id, non-_id, id, name, raw)
            value_options = []
            for field in combo:
                opts = []
                vals = field_values[field]
                # _id form (if id available)
                if vals["id"] is not None:
                    opts.append((f"{field}_id", vals["id"]))
                # non-_id form (id, name, raw)
                if vals["id"] is not None:
                    opts.append((field, vals["id"]))
                if vals["name"] is not None:
                    opts.append((field, vals["name"]))
                # Only include raw primitive values to avoid storing nested Records
                # in filter combinations (non-primitives are already covered by id/name).
                raw = vals["raw"]
                if isinstance(raw, (str, int, float, bool)) and raw != vals["id"] and raw != vals["name"]:
                    opts.append((field, raw))
                value_options.append(opts)
            # Cartesian product of all field options
            for prod in product(*value_options):
                filter_dict = dict(prod)
                # Only include if at least one value is not None/empty
                if any(v not in (None, "") for v in filter_dict.values()):
                    filters.append(filter_dict)
        return filters

    def endpoint(self, resource: str) -> CachedEndpoint:
        """Create a resource-scoped endpoint wrapper, e.g. endpoint('dcim.devices')."""
        return CachedEndpoint(self, resource)


    @staticmethod
    def _lookup_filter_key(resource: str, field: str, value: Any) -> str:
        """Translate payload field names to API filter keys when NetBox expects *_id for FK lookups."""
        if not isinstance(value, int):
            return field

        if resource == "virtualization.clusters" and field == "group" and isinstance(value, int):
            return "group_id"
        if resource == "dcim.device_types" and field == "manufacturer" and isinstance(value, int):
            return "manufacturer_id"
        if resource in ("dcim.devices", "dcim.locations", "ipam.vlans") and field == "site":
            return "site_id"
        if resource == "dcim.interfaces" and field == "device":
            return "device_id"
        if resource in ("dcim.module_bays", "dcim.modules", "dcim.inventory_items") and field == "device":
            return "device_id"
        if resource == "dcim.modules" and field == "module_bay":
            return "module_bay_id"
        if resource == "dcim.module_bay_templates" and field == "device_type":
            return "device_type_id"
        if resource == "dcim.module_types" and field == "manufacturer":
            return "manufacturer_id"
        if resource == "virtualization.interfaces" and field == "virtual_machine":
            return "virtual_machine_id"
        if resource == "virtualization.virtual_machines" and field == "cluster":
            return "cluster_id"
        if resource == "virtualization.virtual_disks" and field == "virtual_machine":
            return "virtual_machine_id"
        return field

    def get(self, resource: str, use_cache: bool = True, **filters: Any) -> Any:
        filters = normalize_fk_fields(resource, dict(filters))
        key = self._cache_key(resource, "get", filters)
        if isinstance(self.adapter, DiodeAdapter):
            cached = self.cache.get(key)
            if cached is not None:
                self._inc_cache_metric("get_hits")
                logger.debug("NetBox get cache hit (diode) resource=%s filters=%s", resource, filters)
                return cached
            self._inc_cache_metric("get_misses")
            logger.debug("NetBox get cache miss (diode) resource=%s filters=%s", resource, filters)
            return None

        if use_cache:
            cached = self.cache.get(key)
            if cached is not None:
                self._inc_cache_metric("get_hits")
                logger.debug("NetBox get cache hit resource=%s filters=%s", resource, filters)
                return cached
            self._inc_cache_metric("get_misses")
            logger.debug("NetBox get cache miss resource=%s filters=%s", resource, filters)

            key_lock = self._get_cache_key_lock(key)
            with key_lock:
                cached = self.cache.get(key)
                if cached is not None:
                    self._inc_cache_metric("get_hits")
                    logger.debug("NetBox get cache hit-after-wait resource=%s filters=%s", resource, filters)
                    return cached

                result = self.adapter.get(resource, **filters)
                logger.debug(
                    "NetBox get result resource=%s filters=%s found=%s",
                    resource,
                    filters,
                    result is not None,
                )
                if result is not None:
                    self.cache.set(key, result)
                    self._set_get_cache_by_id(resource, result)
                    for derived_filters in self._derived_lookup_filters_for_record(resource, result):
                        self._set_get_cache_key(resource, derived_filters, result)
                return result
        else:
            self._inc_cache_metric("get_bypass")
            logger.debug("NetBox get bypass cache resource=%s filters=%s", resource, filters)

        result = self.adapter.get(resource, **filters)
        logger.debug(
            "NetBox get result resource=%s filters=%s found=%s",
            resource,
            filters,
            result is not None,
        )
        if result is not None:
            self.cache.set(key, result)
            self._set_get_cache_by_id(resource, result)
            for derived_filters in self._derived_lookup_filters_for_record(resource, result):
                self._set_get_cache_key(resource, derived_filters, result)
        return result

    def list(self, resource: str, use_cache: bool = True, **filters: Any) -> list[Any]:
        filters = normalize_fk_fields(resource, dict(filters))
        key = self._cache_key(resource, "list", filters)
        if isinstance(self.adapter, DiodeAdapter):
            cached = self.cache.get(key)
            if cached is not None:
                self._inc_cache_metric("list_hits")
                logger.debug("NetBox list cache hit (diode) resource=%s filters=%s count=%s", resource, filters, len(cached))
                return cached
            self._inc_cache_metric("list_misses")
            logger.debug("NetBox list cache miss (diode) resource=%s filters=%s", resource, filters)
            return []

        if not use_cache:
            self._inc_cache_metric("list_bypass")
            logger.debug("NetBox list bypass cache resource=%s filters=%s", resource, filters)
            results = self.adapter.list(resource, **filters)
            logger.debug("NetBox list result resource=%s filters=%s count=%s", resource, filters, len(results))
            return results

        cached = self.cache.get(key)
        if cached is not None:
            self._inc_cache_metric("list_hits")
            logger.debug("NetBox list cache hit resource=%s filters=%s count=%s", resource, filters, len(cached))
            return cached
        self._inc_cache_metric("list_misses")
        logger.debug("NetBox list cache miss resource=%s filters=%s", resource, filters)

        key_lock = self._get_cache_key_lock(key)
        with key_lock:
            cached = self.cache.get(key)
            if cached is not None:
                self._inc_cache_metric("list_hits")
                logger.debug("NetBox list cache hit-after-wait resource=%s filters=%s count=%s", resource, filters, len(cached))
                return cached

            results = self.adapter.list(resource, **filters)
            logger.debug("NetBox list result resource=%s filters=%s count=%s", resource, filters, len(results))
            self.cache.set(key, results)
            for record in results:
                self._set_get_cache_by_id(resource, record)
                for derived_filters in self._derived_lookup_filters_for_record(resource, record):
                    self._set_get_cache_key(resource, derived_filters, record)
            return results

    def create(self, resource: str, data: Mapping[str, Any]) -> Any:
        payload = normalize_fk_fields(resource, dict(data), for_write=True)
        logger.debug("NetBox create resource=%s payload_keys=%s", resource, sorted(payload.keys()))
        created = self.adapter.create(resource, payload)
        logger.debug("NetBox create complete resource=%s id=%s", resource, self._extract_id(created))
        self._invalidate_resource_list_cache(resource)
        self._set_get_cache_by_id(resource, created)
        return created

    def update(self, resource: str, object_id: Any, data: Mapping[str, Any]) -> Any:
        payload = normalize_fk_fields(resource, dict(data), for_write=True)
        logger.debug(
            "NetBox update resource=%s id=%s payload_keys=%s",
            resource,
            object_id,
            sorted(payload.keys()),
        )
        updated = self.adapter.update(resource, object_id, payload)
        logger.debug("NetBox update complete resource=%s id=%s", resource, object_id)
        self._invalidate_resource_list_cache(resource)
        if updated is not None:
            self._set_get_cache_by_id(resource, updated)
        else:
            self._invalidate_get_cache_key(resource, {"id": object_id})
        return updated

    def upsert(
        self,
        resource: str,
        data: Mapping[str, Any],
        *,
        lookup_fields: Optional[Sequence[str]] = None,
        preserve_fields: Optional[Sequence[str]] = None,
        use_cache_for_lookup: bool = True,
    ) -> Any:
        payload = normalize_fk_fields(resource, dict(data), for_write=True)
        if isinstance(self.adapter, DiodeAdapter):
            logger.debug(
                "Diode upsert passthrough resource=%s payload_keys=%s",
                resource,
                sorted(payload.keys()),
            )
            return self.create(resource, payload)

        if lookup_fields is None:
            if payload.get("id") is not None:
                lookup_fields = ("id",)
            elif payload.get("name") is not None:
                lookup_fields = ("name",)
            else:
                lookup_fields = ()

        filters: dict[str, Any] = {}
        for field in lookup_fields:
            if field not in payload:
                continue
            value = payload[field]

            normalized_field = field
            if not field.endswith("id") and isinstance(value, int) and field in FK_FIELDS[resource]:
                normalized_field = f"{field}_id"
            if hasattr(value, "id"):
                value = getattr(value, "id")
            filters[self._lookup_filter_key(resource, normalized_field, value)] = value

        logger.debug(
            "NetBox upsert start resource=%s lookup_fields=%s filters=%s payload_keys=%s",
            resource,
            tuple(lookup_fields),
            filters,
            sorted(payload.keys()),
        )

        existing = None
        if filters:
            cache_before = self._cache_metric_snapshot()
            existing = self.get(resource, use_cache=use_cache_for_lookup, **filters)
            cache_after = self._cache_metric_snapshot()
            cache_delta = self._cache_metric_delta(cache_before, cache_after)
            logger.debug(
                "NetBox upsert lookup resource=%s filters=%s use_cache=%s existing_id=%s cache_delta=%s cache_totals=%s",
                resource,
                filters,
                use_cache_for_lookup,
                self._extract_id(existing),
                cache_delta,
                cache_after,
            )

        if existing is None:
            logger.debug("NetBox upsert creating resource=%s", resource)
            created = self.create(resource, payload)
            if filters:
                if created is not None:
                    self._set_get_cache_key(resource, filters, created)
                else:
                    self._invalidate_get_cache_key(resource, filters)
            return created

        object_id = self._extract_id(existing)
        if object_id is None:
            logger.debug("NetBox upsert existing object missing id, creating resource=%s", resource)
            created = self.create(resource, payload)
            if filters:
                if created is not None:
                    self._set_get_cache_key(resource, filters, created)
                else:
                    self._invalidate_get_cache_key(resource, filters)
            return created

        update_payload = dict(payload)
        if preserve_fields:
            for field in preserve_fields:
                if field not in update_payload:
                    continue

                preserved_value = None
                if isinstance(existing, dict):
                    preserved_value = existing.get(field)
                else:
                    preserved_value = getattr(existing, field, None)

                if preserved_value is not None:
                    update_payload[field] = preserved_value

        desired_subset = {
            key: self._normalize_for_compare(value)
            for key, value in update_payload.items()
        }
        existing_subset = self._build_existing_subset(existing, list(update_payload.keys()))
        payload_diff = DeepDiff(existing_subset, desired_subset, ignore_order=True)
        logger.debug(
            "NetBox upsert diff resource=%s id=%s changed=%s deepdiff=%s",
            resource,
            object_id,
            bool(payload_diff),
            payload_diff.to_dict() if hasattr(payload_diff, "to_dict") else payload_diff,
        )

        if not payload_diff:
            logger.debug("NetBox upsert no-op resource=%s id=%s; skipping update", resource, object_id)
            self._set_get_cache_by_id(resource, existing)
            if filters:
                self._set_get_cache_key(resource, filters, existing)
            return existing

        logger.debug("NetBox upsert updating resource=%s id=%s", resource, object_id)
        updated = self.update(resource, object_id, update_payload)
        if filters:
            if updated is not None:
                self._set_get_cache_key(resource, filters, updated)
            else:
                self._invalidate_get_cache_key(resource, filters)
        return updated

    def delete(self, resource: str, object_id: Any) -> bool:
        logger.debug("NetBox delete resource=%s id=%s", resource, object_id)
        deleted = self.adapter.delete(resource, object_id)
        logger.debug("NetBox delete complete resource=%s id=%s deleted=%s", resource, object_id, deleted)
        self._invalidate_resource_list_cache(resource)
        self._invalidate_get_cache_key(resource, {"id": object_id})
        return deleted

    def prewarm(
        self,
        resources: Sequence[str] | Mapping[str, Mapping[str, Any]],
    ) -> dict[str, int]:
        """Preload cache entries from NetBox.

        Supported input formats:
        - Sequence of resource paths: ["dcim.devices", "ipam.prefixes"]
        - Mapping with per-resource filters: {"dcim.devices": {"site": "nyc"}}
        """
        if isinstance(resources, Mapping):
            resource_filters = dict(resources)
        else:
            resource_filters = {name: {} for name in resources}

        summary: dict[str, int] = {}
        for resource, filters in resource_filters.items():
            external_sentinel_key = self._external_prewarm_sentinel_key(resource, filters)
            sentinel_key = external_sentinel_key
            if sentinel_key is None:
                sentinel_key = self._cache_key(resource, "prewarm_sentinel", filters)

            list_key = self._cache_key(resource, "list", filters)

            sentinel = self.cache.get(sentinel_key)
            if sentinel is not None:
                cached_list = self.cache.get(list_key)
                cached_count = len(cached_list) if isinstance(cached_list, list) else 0
                summary[resource] = cached_count
                logger.debug(
                    "Skipping prewarm for resource=%s filters=%s; prewarm sentinel cache hit key=%s",
                    resource,
                    filters,
                    sentinel_key,
                )
                continue

            # Under memory pressure, Redis can evict sentinel keys earlier than list/get keys.
            # If list cache is still present, restore sentinel and avoid a full API refresh.
            cached_list = self.cache.get(list_key)
            if isinstance(cached_list, list):
                cached_count = len(cached_list)
                self.cache.set(
                    sentinel_key,
                    {
                        "resource": resource,
                        "filters": dict(filters),
                        "count": cached_count,
                        "refreshed_at": int(time.time()),
                        "recovered": True,
                    },
                    ttl_seconds=self.config.prewarm_sentinel_ttl_seconds,
                )
                summary[resource] = cached_count
                logger.debug(
                    "Recovered missing prewarm sentinel from existing list cache resource=%s filters=%s count=%s key=%s",
                    resource,
                    filters,
                    cached_count,
                    sentinel_key,
                )
                continue

            logger.debug(
                "Prewarm sentinel miss for resource=%s filters=%s; refreshing from NetBox",
                resource,
                filters,
            )

            objects = None
            last_exc: Optional[Exception] = None
            for attempt in range(self.config.retry_attempts + 1):
                try:
                    objects = self.adapter.list(resource, **dict(filters))
                    break
                except Exception as exc:
                    last_exc = exc
                    should_retry = False
                    if hasattr(self.adapter, "_should_retry_exception"):
                        should_retry = bool(self.adapter._should_retry_exception(exc))

                    if attempt >= self.config.retry_attempts or not should_retry:
                        raise

                    sleep_seconds = 0.0
                    if hasattr(self.adapter, "_compute_backoff"):
                        sleep_seconds = float(self.adapter._compute_backoff(attempt))

                    logger.warning(
                        "Retrying prewarm after transient error resource=%s attempt=%s/%s sleep=%.2fs error=%s",
                        resource,
                        attempt + 1,
                        self.config.retry_attempts + 1,
                        sleep_seconds,
                        exc,
                    )
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)

            if objects is None:
                if last_exc is not None:
                    raise last_exc
                raise RuntimeError(f"Prewarm failed for resource={resource} without details")

            self.cache.set(list_key, objects)

            count = 0
            derived_key_count = 0
            for obj in objects:
                obj_id = self._extract_id(obj)
                if obj_id is not None:
                    get_key = self._cache_key(resource, "get", {"id": obj_id})
                    self.cache.set(get_key, obj)

                for lookup_filters in self._derived_lookup_filters_for_record(resource, obj):
                    derived_key = self._cache_key(resource, "get", lookup_filters)
                    self.cache.set(derived_key, obj)
                    derived_key_count += 1
                count += 1

            logger.debug(
                "Prewarm derived lookup keys resource=%s filters=%s derived_keys=%s objects=%s",
                resource,
                filters,
                derived_key_count,
                count,
            )
            self.cache.set(
                sentinel_key,
                {
                    "resource": resource,
                    "filters": dict(filters),
                    "count": count,
                    "refreshed_at": int(time.time()),
                },
                ttl_seconds=self.config.prewarm_sentinel_ttl_seconds,
            )

            summary[resource] = count
            logger.info("Prewarmed %s records for resource=%s", count, resource)

        return summary

    def clear_cache(self, resource: Optional[str] = None) -> None:
        if resource:
            self._invalidate_resource_cache(resource)
        else:
            self.cache.clear()

    _PREWARM_SENTINEL_KEY_PREFIX: str = "precache:complete:"

    def cache_stats(self) -> dict[str, Any]:
        """Return a summary dict describing the current cache state.

        Returns a dict with:
        - ``total``: total number of cached entries
        - ``by_resource``: mapping of resource name → dict with ``count`` and
          ``sentinel_ttl`` (seconds remaining on the prewarm sentinel record,
          or ``None`` if no sentinel exists for that resource; 0 means expired).
        """
        all_keys = self.cache.keys()
        total = len(all_keys)
        sentinel_prefix = self._PREWARM_SENTINEL_KEY_PREFIX

        # Build reverse mapping: object_type → resource
        _object_type_to_resource: dict[str, str] = {
            v: k for k, v in self._RESOURCE_TO_PRECACHE_OBJECT_TYPE.items()
        }

        # Tally counts per resource (skip sentinel keys)
        counts: dict[str, int] = {}
        for key in all_keys:
            if key.startswith(sentinel_prefix):
                continue
            parts = key.split(":")
            if len(parts) >= 2:
                resource = parts[0]
                if resource:
                    counts[resource] = counts.get(resource, 0) + 1

        # Collect sentinel TTLs keyed by resource
        sentinel_ttls: dict[str, Optional[int]] = {}
        for key in all_keys:
            if not key.startswith(sentinel_prefix):
                continue
            object_type = key[len(sentinel_prefix):]
            resource = _object_type_to_resource.get(object_type)
            if resource is not None:
                sentinel_ttls[resource] = self.cache.get_ttl(key)

        # Merge counts and sentinel TTLs
        all_resources = sorted(set(counts) | set(sentinel_ttls))
        by_resource: dict[str, Any] = {
            res: {
                "count": counts.get(res, 0),
                "sentinel_ttl": sentinel_ttls.get(res),
            }
            for res in all_resources
        }

        return {"total": total, "by_resource": by_resource}

    def cache_flush(self, resource: Optional[str] = None) -> None:
        """Flush the cache for *resource* (or all if *None*)."""
        self.clear_cache(resource)

    def close(self) -> None:
        self.cache.close()

    def __enter__(self) -> "NetBoxExtendedClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class NetBoxAPI(NetBoxExtendedClient):
    """Drop-in compatible facade for common pynetbox.api usage patterns."""

    @property
    def _raw_api(self) -> Optional[Any]:
        if isinstance(self.adapter, PynetboxAdapter):
            return self.adapter.api
        return None

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        raw_api = self._raw_api
        raw_attr = getattr(raw_api, name, None) if raw_api is not None else None
        return _CompatNode(self, [name], raw_attr)


def api(
    url: str,
    token: str,
    branch: Optional[str] = None,
    *,
    backend: str = "pynetbox",
    cache_backend: str = "none",
    cache_ttl_seconds: int = 300,
    cache_key_prefix: str = "nbx:",
    redis_url: str = "redis://localhost:6379/0",
    sqlite_path: str = ".nbx_cache.sqlite3",
    rate_limit_per_second: float = 0.0,
    rate_limit_burst: int = 1,
    retry_attempts: int = 3,
    retry_initial_delay_seconds: float = 0.3,
    retry_backoff_factor: float = 2.0,
    retry_max_delay_seconds: float = 15.0,
    retry_jitter_seconds: float = 0.0,
    retry_on_4xx: Sequence[int] = (408, 409, 425, 429),
    prewarm_sentinel_ttl_seconds: Optional[int] = None,
    diode_target: str = "grpcs://localhost:8080",
    diode_client_id: str = "",
    diode_client_secret: str = "",
    diode_cert_file: Optional[str] = None,
    diode_skip_tls_verify: bool = False,
    diode_read_fallback: bool = False,
    diode_entity_builder: Optional[Callable[[str, Mapping[str, Any]], Any]] = None,
    diode_batch_size: int = 1,
) -> NetBoxAPI:
    """Create a NetBox API client with pynetbox-like constructor semantics.

    Example:
        import pynetbox_ext as pynetbox
        nb = pynetbox.api(url="https://netbox", token="...")
        device = nb.dcim.devices.get(name="edge01")
    """
    return NetBoxAPI(
        url=url,
        token=token,
        branch=branch,
        backend=backend,
        cache_backend=cache_backend,
        cache_ttl_seconds=cache_ttl_seconds,
        cache_key_prefix=cache_key_prefix,
        redis_url=redis_url,
        sqlite_path=sqlite_path,
        rate_limit_per_second=rate_limit_per_second,
        rate_limit_burst=rate_limit_burst,
        retry_attempts=retry_attempts,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_backoff_factor=retry_backoff_factor,
        retry_max_delay_seconds=retry_max_delay_seconds,
        retry_jitter_seconds=retry_jitter_seconds,
        retry_on_4xx=retry_on_4xx,
        prewarm_sentinel_ttl_seconds=prewarm_sentinel_ttl_seconds,
        diode_target=diode_target,
        diode_client_id=diode_client_id,
        diode_client_secret=diode_client_secret,
        diode_cert_file=diode_cert_file,
        diode_skip_tls_verify=diode_skip_tls_verify,
        diode_read_fallback=diode_read_fallback,
        diode_entity_builder=diode_entity_builder,
        diode_batch_size=diode_batch_size,
    )


__all__ = [
    "NetBoxExtendedClient",
    "NetBoxAPI",
    "NetBoxExtendedConfig",
    "CachedEndpoint",
    "api",
]

