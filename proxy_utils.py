# -*- coding: utf-8 -*-
"""Proxy control utilities for requests."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

import requests


_no_proxy_ctx: ContextVar[bool] = ContextVar("no_proxy_ctx", default=False)
_patched = False


def _patch_requests() -> None:
    global _patched
    if _patched:
        return

    original_request = requests.sessions.Session.request

    def _wrapped_request(self, method, url, **kwargs):
        if _no_proxy_ctx.get():
            previous_trust_env = getattr(self, "trust_env", True)
            self.trust_env = False
            kwargs["proxies"] = {}
            try:
                return original_request(self, method, url, **kwargs)
            finally:
                self.trust_env = previous_trust_env
        return original_request(self, method, url, **kwargs)

    requests.sessions.Session.request = _wrapped_request
    _patched = True


@contextmanager
def no_proxy() -> Iterator[None]:
    _patch_requests()
    token = _no_proxy_ctx.set(True)
    try:
        yield
    finally:
        _no_proxy_ctx.reset(token)
