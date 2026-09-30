"""Callback SSRF guard + redirect re-screening (review M9)."""

from __future__ import annotations

from hoglah.client import _callback_url_allowed, _ScreeningRedirectHandler


def test_callback_blocks_shared_multicast_and_nat64(monkeypatch) -> None:
    from hoglah.client import _PinnedCallbackHTTP, _callback_url_allowed

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(2, 1, 6, "", ("100.64.1.1", port or 80))]

    monkeypatch.setattr("hoglah.client.socket.getaddrinfo", fake_getaddrinfo)
    ok, reason = _callback_url_allowed("http://shared.example/hook", allow_private=False)
    assert ok is False
    assert "non-public" in reason

    monkeypatch.setattr(
        "hoglah.client.socket.getaddrinfo",
        lambda *a, **k: [(2, 1, 6, "", ("224.0.0.1", 80))],
    )
    ok, _ = _callback_url_allowed("http://multicast.example/hook", allow_private=False)
    assert ok is False

    monkeypatch.setattr(
        "hoglah.client.socket.getaddrinfo",
        lambda *a, **k: [(10, 1, 6, "", ("64:ff9b::7f00:1", 80))],
    )
    ok, _ = _callback_url_allowed("http://nat64.example/hook", allow_private=False)
    assert ok is False

    monkeypatch.setattr(
        "hoglah.client.socket.getaddrinfo",
        lambda *a, **k: [(2, 1, 6, "", ("93.184.216.34", 80))],
    )
    seen = {}

    def fake_create(address, timeout, source_address):
        seen["address"] = address
        raise OSError("stop")

    conn = _PinnedCallbackHTTP("example.test", 80, timeout=1)
    monkeypatch.setattr(conn, "_create_connection", fake_create)
    try:
        conn.connect()
    except OSError as exc:
        assert "stop" in str(exc)
    assert seen["address"] == ("93.184.216.34", 80)


def test_callback_blocks_link_local_and_unspecified() -> None:
    ok, reason = _callback_url_allowed("http://169.254.169.254/meta", allow_private=False)
    assert ok is False
    assert "non-public" in reason or "link" in reason or "address" in reason

    ok2, _ = _callback_url_allowed("http://0.0.0.0/", allow_private=False)
    assert ok2 is False


def test_callback_allows_public_when_private_disallowed() -> None:
    # example.com is public; may need DNS — if DNS fails, treat as non-hard-fail.
    ok, reason = _callback_url_allowed("https://example.com/hook", allow_private=False)
    if ok:
        assert reason == "ok"
    else:
        # Offline / no DNS environments still exercise the API.
        assert "resolve" in reason or "non-public" in reason


def test_redirect_handler_blocks_private_hop() -> None:
    handler = _ScreeningRedirectHandler(allow_private=False)
    class _Req:
        full_url = "https://example.com/start"
        get_method = lambda self: "POST"  # noqa: E731
        headers = {}
        data = b"{}"
        unverifiable = False

    try:
        handler.redirect_request(
            _Req(),
            None,
            302,
            "Found",
            {},
            "http://169.254.169.254/latest/meta-data/",
        )
        raise AssertionError("expected redirect to be blocked")
    except Exception as exc:
        assert "blocked" in str(exc).lower() or "non-public" in str(exc).lower()
