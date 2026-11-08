# Lucky reliable notes (3.6.1-lucky.1)

Pair with the Lucky Obsidian plugin 2.99.3. `MergeCapabilities` advertises JSON protocol 1. Uploads carry exact ancestor text, immutable request ID and optional acknowledged tombstone revision for explicit recreation. A per-note lock covers merge, durable storage, receipt and broadcast. Deletes and renames use source revision preconditions and persistent operation receipts. Renames commit the target before deleting the exact source, retaining share/history links.

Each request first persists its complete intent under `storage/merge-receipts/`. Conflicting drafts become deterministic synced conflict notes. Replays return the original receipt and do not repeat a write or rebroadcast an obsolete body. Existing REST and legacy clients retain their original API semantics; every participating Obsidian device should upgrade.

The DAO writes immutable SHA256 blobs below `storage/vault/u_<uid>/note/bodies-sha256/` before committing the database `Content` reference `fns-blob-sha256-v1:<digest>`. Reads verify digest, UTF-16 hash32 and byte length. `n_<id>/content.txt` remains a compatibility projection for upstream 3.6.1. The authoritative reference/body commit remains readable if projection fails.

Back up PostgreSQL and the complete `storage/` plus configuration together while FNS is stopped. A SQL dump alone does not contain the body blobs or request receipts. A normal rollback preserves data written after deployment; do not overwrite it with the pre-deployment backup.

Before starting upstream 3.6.1, stop FNS, keep PostgreSQL running and execute the following on its Docker host. It uses the existing DB container's own credentials, queries every tenant read-only, validates all referenced blobs before writing, saves replaced projections, then reads back every projection. If it fails, keep FNS stopped and resolve the reported storage issue before starting old code.

```bash
sudo -n python3 scripts/lucky/rebuild-projections.py --storage /volume2/docker/fast-note-sync/storage
sudo -n python3 scripts/lucky/rebuild-projections.py --storage /volume2/docker/fast-note-sync/storage --check-only
```

Defaults: DB container `fast-note-sync-db`, service container `fast-note-sync-service`. Override their names with the corresponding CLI options when needed.
