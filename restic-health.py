#!/usr/bin/env python3

from dataclasses import dataclass
from operator import itemgetter
import argparse
import json
from pathlib import Path
import sys
import yaml
import os
from datetime import datetime, timezone
import logging
import asyncio

parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', metavar='CONFIG', type=str, default='/etc/restic-health.yml')
parser.add_argument('--skip-current', action='store_true', help='Skip (not wait/fail) repos that don\'t have a new snapshot')
parser.add_argument('--verbose', '-v', action='store_true')
args = parser.parse_args()

class LevelPrefixFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        original = record.msg
        if record.levelno >= logging.WARNING:
            record.msg = f'{record.levelname.capitalize()}: {original}'

        try:
            return super().format(record)
        finally:
            record.msg = original

stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(LevelPrefixFormatter())
level = logging.DEBUG if args.verbose else logging.INFO
logging.basicConfig(format='%(message)s', level=level, handlers=[stdout_handler])

# Suppress noise from asyncio:
logging.getLogger('asyncio').setLevel(logging.WARNING)

@dataclass
class GeneralConfig:
    state_dir: str
    cache_dir: str|None
    retries: int
    retry_delay: int

@dataclass
class BackendConfig:
    name: str
    repository: str

@dataclass
class LocationConfig:
    name: str
    password_file: str
    backends: dict[str, BackendConfig]

class ResticHealthError(Exception):
    pass

with open(args.config, 'r') as fh:
    config_yaml = yaml.safe_load(fh)

# Add default values (in a stupid but simple way)
if not 'defaults' in config_yaml:
    config_yaml['defaults'] = dict()

if not 'retries' in config_yaml['defaults']:
    config_yaml['defaults']['retries'] = 30

if not 'retry_delay' in config_yaml['defaults']:
    config_yaml['defaults']['retry_delay'] = 120

config = GeneralConfig(
        state_dir = config_yaml['state_dir'],
        cache_dir = config_yaml['defaults']['cache_dir'] or None,
        retries = config_yaml['defaults']['retries'],
        retry_delay = config_yaml['defaults']['retry_delay'],
        )

locations: dict[str, LocationConfig] = {}
for location_name, location in config_yaml['locations'].items():
    backends: dict[str, BackendConfig] = {}
    for backend_name, backend in (location.get('backends') or dict()).items():
        backends[backend_name] = BackendConfig(
                name = backend_name,
                repository = backend)
    locations[location_name] = LocationConfig(
            name = location_name,
            password_file = location['password_file'],
            backends = backends)


async def restic(backend: BackendConfig, location: LocationConfig, args: list[str]) -> str:
    cache_dir_args = []
    if config.cache_dir:
        cache_dir_args = ['--cache-dir', config_yaml['defaults']['cache_dir']]

    env = {
            'RESTIC_REPOSITORY': backend.repository,
            'RESTIC_PASSWORD_FILE': location.password_file,
            }
    cmd = ['restic', '--quiet', '--no-lock'] + cache_dir_args + args

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    stdout = bytes.decode(stdout_bytes, 'utf8')
    stderr = bytes.decode(stderr_bytes, 'utf8')

    if proc.returncode != 0:
        repo = f'{location.name}@{backend.name}'
        logging.error(f'{repo}: Restic command {args} returned non-zero exit status {proc.returncode}. Standard error:\n{stderr.strip()}')
        raise ResticHealthError()

    return stdout

async def restic_json(backend: BackendConfig, location: LocationConfig, args: list[str]) -> str:
    return await restic(backend, location, ['--json', *args])

async def get_snapshots(location: LocationConfig, backend: BackendConfig) -> str:
    stdout = await restic_json(backend, location, ['snapshots'])
    return stdout

async def get_stats(location: LocationConfig, backend: BackendConfig, mode: str, snapshot: str|None = None) -> str:
    args = ['stats', '--mode', mode]
    if snapshot is not None:
        args.append(snapshot)
    stdout = await restic_json(backend, location, args)
    return stdout

async def get_diff_stats(location: LocationConfig, backend: BackendConfig, snapshot_ids: list[str]) -> str:
    stdout = await restic_json(backend, location, ['diff'] + snapshot_ids)
    lastline = stdout.splitlines()[-1]
    return lastline

async def write_state_file(location: LocationConfig, backend: BackendConfig, category: str, content: str) -> None:
    now = datetime.now()
    time_str = now.strftime("%Y-%m-%d-%s")
    base_dir = Path(config.state_dir) / f'{location.name}@{backend.name}'
    file_path =  base_dir / f'{category}-{time_str}.json'
    latest_link = base_dir / f'{category}.latest.json'

    base_dir.mkdir(parents=True, exist_ok=True)
    logging.debug(f'Writing {file_path}')
    file_path.write_text(content)
    if latest_link.is_symlink():
        os.remove(latest_link)
    logging.debug(f'Adding symlink {latest_link}')
    latest_link.symlink_to(os.path.relpath(file_path, latest_link.parent))

async def unlock(location, backend):
    await restic_json(backend, location, ['unlock'])

async def get_locks(location, backend):
    stdout = await restic_json(backend, location, ['list', 'locks'])
    return stdout.splitlines()

async def get_latest_snapshot_timestamp(location, backend):
    raw_snapshots = await get_snapshots(location, backend)
    snapshots = json.loads(raw_snapshots)
    if len(snapshots) == 0:
        return datetime.min
    latest_snapshot = snapshots[-1]
    snapshot_time = datetime.fromisoformat(snapshots[-1]['summary']['backup_end'])
    return snapshot_time

async def get_latest_statefile_timestamp(location, backend):
    # Just checking *any* latest state file here, assuming they are all the same age.
    base_dir = Path(config.state_dir) / f'{location.name}@{backend.name}'
    latest_link = base_dir / 'raw-snapshots.latest.json'
    if not latest_link.is_symlink():
        return datetime.min.replace(tzinfo=timezone.utc)
    mtime = datetime.fromtimestamp(latest_link.stat().st_mtime, tz=timezone.utc)
    return mtime

async def has_fresh_snapshot(location, backend):
    latest_snapshot_timestamp = await get_latest_snapshot_timestamp(location, backend)
    latest_statefile_timestamp = await get_latest_statefile_timestamp(location, backend)
    return latest_snapshot_timestamp > latest_statefile_timestamp, latest_snapshot_timestamp

async def wait_until_fresh_snapshot(location, backend):
    repo = f'{location.name}@{backend.name}'
    retries_remaining = config.retries
    while True:
        logging.debug(f'Checking if latest snapshot in {repo} is newer than our latest data')
        has_fresh, latest_snapshot_timestamp = await has_fresh_snapshot(location, backend)
        if has_fresh:
            return
        if retries_remaining == 0:
            logging.error(f'Giving up on {repo}: No new snapshot appeared, latest is from {latest_snapshot_timestamp}')
            raise ResticHealthError()
        logging.debug(f'{repo} has no new snapshot, waiting {config.retry_delay} seconds before checking up to {retries_remaining} more time(s)')
        retries_remaining -= 1
        await asyncio.sleep(config.retry_delay)

async def wait_until_unlocked(location, backend):
    repo = f'{location.name}@{backend.name}'
    retries_remaining = config.retries
    while True:
        logging.debug(f'Checking if {repo} is locked')
        # Calling "restic unlock" is always safe because it only removes stale
        # locks, and lock holders are required to periodically refresh their
        # locks. In the future, restic probably simply ignore stale locks.
        await unlock(location, backend)
        locks = await get_locks(location, backend)
        if len(locks) > 0:
            if retries_remaining == 0:
                logging.error(f'Giving up on {repo} (still locked) after dumping existing locks:')
                for lock in locks:
                    lock_content = await restic_json(backend, location, ['cat', 'lock', lock])
                    print(lock_content)
                raise ResticHealthError()
            logging.debug(f'{repo} is locked, waiting {config.retry_delay} seconds before retrying up to {retries_remaining} more time(s)')
            retries_remaining -= 1
            await asyncio.sleep(config.retry_delay)
        else:
            break

async def handle_repo(location, backend, skip_current):
    repo = f'{location.name}@{backend.name}'
    logging.info(f'Handling {repo}')

    if skip_current:
        has_fresh, latest_snapshot_timestamp = await has_fresh_snapshot(location, backend)
        if has_fresh:
            logging.debug(f'Latest snapshot in {repo} is newer ({latest_snapshot_timestamp}) than our state file')
        else:
            logging.info(f'Skipping {repo} because there is no new snapshot (as per --skip-current)')
            return  # no error
    else:
        # We can't really know when the fresh snapshot is going to be created, but
        # if there isn't one, there's not much point in gathering health data. If
        # this heuristic goes wrong, the lack of new data should correctly trigger
        # alerts to investigate. Polling the latest snapshot timestamp is a bit of
        # a hack, but it should work:
        await wait_until_fresh_snapshot(location, backend)

    await wait_until_unlocked(location, backend)

    logging.debug(f'Querying snapshot list for {repo}')
    raw_snapshots = await get_snapshots(location, backend)
    await write_state_file(location, backend, 'raw-snapshots', raw_snapshots)

    snapshots = json.loads(raw_snapshots)
    await write_state_file(location, backend, 'snapshot-count', json.dumps({'snapshot_count': len(snapshots)}))

    if len(snapshots) >= 1:
        logging.debug(f'Querying restore-size stats for latest snapshot in {repo}')
        await write_state_file(location, backend, 'raw-stats-restore-size-latest', await get_stats(location, backend, 'restore-size', 'latest'))

        # Collecting raw-data stats is disabled for now (since 2025-06-27)
        # because it's too expensive. See also the munin plugin for some more
        # thoughts.
        #  logging.debug(f'Querying raw-data stats for latest snapshot in {repo}')
        #  await write_state_file(location, backend, 'raw-stats-raw-data-latest', await get_stats(location, backend, 'raw-data', 'latest'))
        #  logging.debug(f'Querying raw-data stats for all snapshots in {repo}')
        #  await write_state_file(location, backend, 'raw-stats-raw-data-all', await get_stats(location, backend, 'raw-data'))

    if len(snapshots) >= 2:
        latest_two = list(map(itemgetter('id'), snapshots[-2:]))
        logging.debug(f'Querying latest diff stats for {repo}')
        diff_stats_latest = await get_diff_stats(location, backend, latest_two)
        await write_state_file(location, backend, 'raw-diff-stats-latest', diff_stats_latest)

async def main():
    handlers = []
    for location_name, location in locations.items():
        for backend_name, backend in location.backends.items():
            handler = asyncio.create_task(handle_repo(location, backend, skip_current=args.skip_current))
            handlers.append(handler)

    fails = 0
    for handler in asyncio.as_completed(handlers):
        try:
            await handler
        except ResticHealthError as e:
            fails += 1
    if fails > 0:
        logging.error(f'Encountered {fails} error{"s" if fails != 1 else ""} in total')
        sys.exit(1)

asyncio.run(main())
