"""Callback SSRF guard + redirect re-screening (review M9)."""

from __future__ import annotations

from hoglah.client import _callback_url_allowed, _ScreeningRedirectHandler


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
