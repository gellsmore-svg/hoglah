# Domains Registry Update for Hoglah

**Date**: 2026-06-12

Hoglah has been added as an active domain in the family. Because the top-level registry files (`DOMAINS.md` and `domains.json` in `~/domains/`) are currently owned by root, the following updates could not be applied automatically by the agent.

## Recommended exact edits

### 1. ~/domains/DOMAINS.md

Add the hoglah entry after the mnemosyne section (and clean up the stale Mnemosyne path note if desired):

```markdown
### mnemosyne
- Path: `Mnemosyne` (active folder on disk: `Tirzah`)
- Aliases: none
- Status: active

### hoglah
- Path: `Hoglah`
- Aliases: none
- Status: active (initial requirements + metadata scaffolded 2026-06-12)
- GitHub: https://github.com/gellsmore-svg/hoglah
- Description: Lightweight local-first job queue manager and Ollama wrapper for resource-constrained environments.

## Notes
```

(Full current file content was captured at the time of this note; the addition keeps the existing structure.)

### 2. ~/domains/domains.json

Add the object to the `domains` array (before the final `]`):

```json
    {
      "name": "mnemosyne",
      "path": "Mnemosyne",
      "aliases": [],
      "status": "active"
    },
    {
      "name": "hoglah",
      "path": "Hoglah",
      "aliases": [],
      "status": "active",
      "github": "https://github.com/gellsmore-svg/hoglah",
      "description": "Lightweight local-first job queue manager and Ollama wrapper for resource-constrained environments."
    }
```

Then update the top-level `updated_at` timestamp.

## How to apply (when you have sudo or root shell)

```bash
cd ~/domains

# Take ownership (one-time)
sudo chown cello:cello DOMAINS.md domains.json

# Then either manually edit, or (example) apply via cat from these reference copies:
# sudo cp domains/Hoglah/docs/domains-registry-DOMAINS.md DOMAINS.md
# sudo cp domains/Hoglah/docs/domains-registry-domains.json domains.json
```

Reference copies of the **full desired files** are also provided alongside this note for convenience (see `domains-registry-full-DOMAINS.md` and `domains-registry-full-domains.json` in the same directory if generated).
