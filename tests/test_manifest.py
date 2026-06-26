from keturah import validate_manifest

from hoglah.manifest import build_manifest


def test_manifest_conforms_and_exposes_tools():
    m = build_manifest()
    assert validate_manifest(m) == []
    assert m.product == "hoglah"
    tool_names = [t["name"] for t in m.to_mcp()["tools"]]
    assert "submit_job" in tool_names and "job_result" in tool_names
