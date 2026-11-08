#!/usr/bin/env python3
"""Rebuild legacy content.txt projections from verified PostgreSQL blob references.

Run as root on NAS after stopping FNS, while PostgreSQL remains running. Database
access is read-only. Secrets resolve inside the existing database container.
"""
import argparse, hashlib, json, os, pathlib, re, subprocess, tempfile, time

PREFIX = 'fns-blob-sha256-v1:'

def hash32(body):
    units = body.decode('utf-8').encode('utf-16-le')
    value = 0
    for i in range(0, len(units), 2):
        value = (31 * value + int.from_bytes(units[i:i+2], 'little')) & 0xffffffff
    return str(value if value < 2**31 else value - 2**32)

def atomic_write(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.rebuild-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            os.fchmod(f.fileno(), 0o644)
            f.write(body); f.flush(); os.fsync(f.fileno())
        os.replace(name, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        if os.path.exists(name): os.unlink(name)

def rebuild(storage, rows, check_only=False):
    # Validate every body before changing even one projection.
    planned = []
    for row in rows:
        uid, note_id = int(row['uid']), int(row['id'])
        if uid < 0 or note_id < 1: raise RuntimeError('invalid tenant/note identifier')
        reference = row['ref']
        if not reference.startswith(PREFIX): raise RuntimeError('unknown body reference')
        digest = reference[len(PREFIX):]
        if not re.fullmatch('[0-9a-f]{64}', digest): raise RuntimeError('invalid blob digest')
        base = storage / 'vault' / f'u_{uid}' / 'note'
        blob = base / 'bodies-sha256' / (digest + '.txt')
        body = blob.read_bytes()
        if hashlib.sha256(body).hexdigest() != digest or len(body) != int(row['size']) or hash32(body) != row['hash']:
            raise RuntimeError('blob digest, size, or content hash verification failed')
        projection = base / f'n_{note_id}' / 'content.txt'
        if projection.is_symlink() or blob.is_symlink(): raise RuntimeError('unexpected storage symlink')
        if projection.exists() and not projection.is_file(): raise RuntimeError('projection path is occupied by a non-file')
        if not projection.exists() or projection.read_bytes() != body:
            planned.append((uid, note_id, projection, body))
    if check_only:
        if planned: raise RuntimeError(f'{len(planned)} compatibility projections need rebuild')
    else:
        backup = storage / 'projection-rebuild-backups' / time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        for uid, note_id, projection, body in planned:
            if projection.exists(): atomic_write(backup / f'u_{uid}' / f'n_{note_id}.txt', projection.read_bytes())
            atomic_write(projection, body)
    # Read back all projections, including those already correct.
    for row in rows:
        projection = storage / 'vault' / f"u_{int(row['uid'])}" / 'note' / f"n_{int(row['id'])}" / 'content.txt'
        if hashlib.sha256(projection.read_bytes()).hexdigest() != row['ref'][len(PREFIX):]:
            raise RuntimeError('projection readback failed')
    return len(rows), len(planned)

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--storage', type=pathlib.Path, required=True)
    ap.add_argument('--db-container', default='fast-note-sync-db')
    ap.add_argument('--service-container', default='fast-note-sync-service')
    ap.add_argument('--check-only', action='store_true')
    ap.add_argument('--manifest', type=pathlib.Path, help='Offline fixture only; skips Docker/DB access')
    args = ap.parse_args()
    if args.manifest:
        rows = json.loads(args.manifest.read_text())
    else:
        status = subprocess.run(['docker','inspect','--format','{{.State.Running}}',args.service_container], capture_output=True, text=True, check=True)
        if status.stdout.strip() != 'false': raise RuntimeError('stop FNS service before projection rebuild')
        def query(sql):
            p = subprocess.run(['docker','exec','-i',args.db_container,'sh','-c',
                'exec psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
                input=sql, capture_output=True, text=True)
            if p.returncode: raise RuntimeError('read-only PostgreSQL metadata query failed')
            return p.stdout.strip()
        schemas = json.loads(query("SELECT COALESCE(json_agg(schemaname), '[]'::json)::text FROM pg_tables WHERE tablename='note' AND schemaname ~ '^user_[0-9]+$';"))
        rows = []
        for schema in schemas:
            if not re.fullmatch(r'user_[0-9]+', schema): raise RuntimeError('unexpected tenant schema')
            uid = int(schema[5:])
            sql = f'''SELECT COALESCE(json_agg(json_build_object('uid',{uid},'id',id,'ref',content,'hash',content_hash,'size',size)), '[]'::json)::text FROM "{schema}".note WHERE content LIKE '{PREFIX}%';'''
            rows.extend(json.loads(query(sql)))
    checked, changed = rebuild(args.storage.resolve(), rows, args.check_only)
    print(json.dumps({'verified_blob_rows': checked, 'rebuilt_projections': changed, 'database_writes': 0}))

if __name__ == '__main__':
    try: main()
    except Exception as e:
        # Exception strings contain no credential or note body.
        raise SystemExit('Projection rebuild failed: ' + str(e))
