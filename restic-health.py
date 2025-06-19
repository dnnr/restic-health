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
parser.add_argument('--verbose', '-v', action='store_true')
args = parser.parse_args()

logging.basicConfig(format='%(message)s', level=logging.INFO)
if args.verbose:
    logging.getLogger().setLevel(logging.DEBUG)

@dataclass
class GeneralConfig:
    state_dir: str

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

config = GeneralConfig(state_dir = config_yaml['state_dir'])

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

async def restic_json(backend: BackendConfig, password_file: str, args: list[str]) -> str:
    cache_dir_args = []
    if 'defaults' in config_yaml and 'cache_dir' in config_yaml['defaults']:
        cache_dir_args = ['--cache-dir', config_yaml['defaults']['cache_dir']]

    env = {
            'RESTIC_REPOSITORY': backend.repository,
            'RESTIC_PASSWORD_FILE': password_file,
            }
    cmd = ['restic', '--json', '--quiet', '--no-lock'] + cache_dir_args + args

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
        logging.error(f'Command {cmd} returned non-zero exit status {proc.returncode}. Standard error:\n{stderr.strip()}')
        raise ResticHealthError()

    return stdout

async def get_snapshots(location: LocationConfig, backend: BackendConfig) -> str:
    stdout = await restic_json(backend, location.password_file, ['snapshots'])
    return stdout

async def get_stats(location: LocationConfig, backend: BackendConfig, mode: str, snapshot: str|None = None) -> str:
    args = ['stats', '--mode', mode]
    if snapshot is not None:
        args.append(snapshot)
    stdout = await restic_json(backend, location.password_file, args)
    return stdout

async def get_diff_stats(location: LocationConfig, backend: BackendConfig, snapshot_ids: list[str]) -> str:
    stdout = await restic_json(backend, location.password_file, ['diff'] + snapshot_ids)
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
    latest_link.symlink_to(file_path)

async def get_locks(location, backend):
    stdout = await restic_json(backend, location.password_file, ['list', 'locks'])
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

async def wait_until_fresh_snapshot(location, backend):
    repo = f'{location.name}@{backend.name}'
    retry_delay = 120
    retries_remaining = 30
    while True:
        logging.debug(f'Checking if latest snapshot in {repo} is newer than our latest data')
        latest_snapshot_timestamp = await get_latest_snapshot_timestamp(location, backend)
        latest_statefile_timestamp = await get_latest_statefile_timestamp(location, backend)
        if latest_snapshot_timestamp < latest_statefile_timestamp:
            if retries_remaining == 0:
                logging.error(f'Giving up on {repo}: No new snapshot appeared, latest is from {latest_snapshot_timestamp}')
                raise ResticHealthError()
            logging.debug(f'{repo} has no new snapshot, waiting {retry_delay} seconds before checking up to {retries_remaining} more time(s)')
            retries_remaining -= 1
            await asyncio.sleep(retry_delay)
        else:
            break

async def wait_until_unlocked(location, backend):
    repo = f'{location.name}@{backend.name}'
    retry_delay = 120
    retries_remaining = 30
    while True:
        logging.debug(f'Checking if {repo} is locked')
        locks = await get_locks(location, backend)
        if len(locks) > 0:
            if retries_remaining == 0:
                logging.error(f'Giving up on {repo} (still locked) after dumping existing locks:')
                for lock in locks:
                    lock_content = await restic_json(backend, location.password_file, ['cat', 'lock', lock])
                    print(lock_content)
                raise ResticHealthError()
            logging.debug(f'{repo} is locked, waiting {retry_delay} seconds before retrying up to {retries_remaining} more time(s)')
            retries_remaining -= 1
            await asyncio.sleep(retry_delay)
        else:
            break

async def handle_repo(location, backend):
    repo = f'{location.name}@{backend.name}'
    logging.info(f'Handling {repo}')

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
        logging.debug(f'Querying raw-data stats for latest snapshot in {repo}')
        await write_state_file(location, backend, 'raw-stats-raw-data-latest', await get_stats(location, backend, 'raw-data', 'latest'))
        logging.debug(f'Querying raw-data stats for all snapshots in {repo}')
        await write_state_file(location, backend, 'raw-stats-raw-data-all', await get_stats(location, backend, 'raw-data'))

    if len(snapshots) >= 2:
        latest_two = list(map(itemgetter('id'), snapshots[-2:]))
        logging.debug(f'Querying latest diff stats for {repo}')
        diff_stats_latest = await get_diff_stats(location, backend, latest_two)
        await write_state_file(location, backend, 'raw-diff-stats-latest', diff_stats_latest)

async def main():
    handlers = []
    for location_name, location in locations.items():
        for backend_name, backend in location.backends.items():
            handler = asyncio.create_task(handle_repo(location, backend))
            handlers.append(handler)

    fails = 0
    for handler in asyncio.as_completed(handlers):
        try:
            await handler
        except ResticHealthError:
            fails += 1
    if fails > 0:
        logging.error(f'Encountered {fails} error(s) (see log)')
        sys.exit(1)

asyncio.run(main())
