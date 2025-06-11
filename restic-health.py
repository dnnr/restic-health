#!/usr/bin/env python3

from dataclasses import dataclass
from operator import itemgetter
import argparse
import json
from pathlib import Path
import subprocess
import sys
import yaml
import os
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', metavar='CONFIG', type=str, default='/etc/restic-health.yml')
args = parser.parse_args()

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

with open(args.config, 'r') as fh:
    config_yaml = yaml.safe_load(fh)

config = GeneralConfig(state_dir = config_yaml['state_dir'])

locations: dict[str, LocationConfig] = {}
for location_name, location in config_yaml['locations'].items():
    backends: dict[str, BackendConfig] = {}
    for backend_name, backend in location['backends'].items():
        backends[backend_name] = BackendConfig(
                name = backend_name,
                repository = backend)
    locations[location_name] = LocationConfig(
            name = location_name,
            password_file = location['password_file'],
            backends = backends)

def restic_json(backend: BackendConfig, password_file: str, args: list[str]) -> str:
    cache_dir_args = []
    if 'defaults' in config_yaml and 'cache_dir' in config_yaml['defaults']:
        cache_dir_args = ['--cache-dir', config_yaml['defaults']['cache_dir']]

    env = {
            'RESTIC_REPOSITORY': backend.repository,
            'RESTIC_PASSWORD_FILE': password_file,
            }
    cmd = ['restic', '--json', '--quiet'] + cache_dir_args + args

    proc = subprocess.run(cmd, env=env, text=True, capture_output=True)

    if proc.returncode != 0:
        print(f'Command {cmd} returned non-zero exit status {proc.returncode}. Standard error:\n{proc.stderr}')
        sys.exit(1)

    return proc.stdout

def get_snapshots(location: LocationConfig, backend: BackendConfig) -> str:
    stdout = restic_json(backend, location.password_file, ['snapshots'])
    return stdout

def get_stats(location: LocationConfig, backend: BackendConfig, mode: str, snapshot: str|None = None) -> str:
    args = ['stats', '--mode', mode]
    if snapshot is not None:
        args.append(snapshot)
    stdout = restic_json(backend, location.password_file, args)
    return stdout

def get_diff_stats(location: LocationConfig, backend: BackendConfig, snapshot_ids: list[str]) -> str:
    stdout = restic_json(backend, location.password_file, ['diff'] + snapshot_ids)
    lastline = stdout.splitlines()[-1]
    return lastline

def write_state_file(location: LocationConfig, backend: BackendConfig, category: str, content: str) -> None:
    now = datetime.now()
    time_str = now.strftime("%Y-%m-%d-%s")
    base_dir = Path(config.state_dir) / f'{location.name}@{backend.name}'
    file_path =  base_dir / f'{category}-{time_str}.json'
    latest_link = base_dir / f'{category}.latest.json'

    base_dir.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)
    if latest_link.is_symlink():
        os.remove(latest_link)
    latest_link.symlink_to(file_path)

for location_name, location in locations.items():
    for backend_name, backend in location.backends.items():
        print(f'Handling {location_name}@{backend_name}')

        raw_snapshots = get_snapshots(location, backend)
        write_state_file(location, backend, 'raw-snapshots', raw_snapshots)

        snapshots = json.loads(raw_snapshots)
        write_state_file(location, backend, 'snapshot-count', json.dumps({'snapshot_count': len(snapshots)}))

        if len(snapshots) >= 1:
            write_state_file(location, backend, 'raw-stats-restore-size-latest', get_stats(location, backend, 'restore-size', 'latest'))
            write_state_file(location, backend, 'raw-stats-raw-data-latest', get_stats(location, backend, 'raw-data', 'latest'))
            write_state_file(location, backend, 'raw-stats-raw-data-all', get_stats(location, backend, 'raw-data'))

        if len(snapshots) >= 2:
            latest_two = list(map(itemgetter('id'), snapshots[-2:]))
            diff_stats_latest = get_diff_stats(location, backend, latest_two)
            write_state_file(location, backend, 'raw-diff-stats-latest', json.dumps(diff_stats_latest))
