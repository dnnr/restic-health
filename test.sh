#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

cd "$(dirname "$0")"

rm -rf test-restic-health
mkdir test-restic-health
echo foo > test-restic-health/pw

function test_restic {
    restic --quiet --repo test-restic-health/repo --password-file test-restic-health/pw --cache-dir test-restic-health/cache $@
}
test_restic init
test_restic backup /dev/zero

./restic-health.py -c restic-health.test.yml -v collect

function assert_state_exists {
    test -e test-restic-health/state_dir/$1
}

cat > test-restic-health/expected_state_file_list_sanitized << EOF
test-restic-health/state_dir
test-restic-health/state_dir/testloc@testbe
test-restic-health/state_dir/testloc@testbe/raw-snapshots-YYYY-MM-DD-xxx.json
test-restic-health/state_dir/testloc@testbe/raw-snapshots.latest.json
test-restic-health/state_dir/testloc@testbe/raw-stats-restore-size-latest-YYYY-MM-DD-xxx.json
test-restic-health/state_dir/testloc@testbe/raw-stats-restore-size-latest.latest.json
test-restic-health/state_dir/testloc@testbe/snapshot-count-YYYY-MM-DD-xxx.json
test-restic-health/state_dir/testloc@testbe/snapshot-count.latest.json
EOF

find test-restic-health/state_dir | sort > test-restic-health/actual_state_file_list
sed -r "s/$(date '+%Y-%m-%d')-[0-9]+/YYYY-MM-DD-xxx/" test-restic-health/actual_state_file_list > test-restic-health/actual_state_file_list_sanitized
diff test-restic-health/expected_state_file_list_sanitized test-restic-health/actual_state_file_list_sanitized

# Assert that there aren't any broken symlinks in the state directory:
find test-restic-health/state_dir -xtype l -print -exec false '{}' +

# Fails because there's no new snapshot
./restic-health.py -c restic-health.test.yml -v collect && exit 1

# Succeeds thanks to --skip-current
./restic-health.py -c restic-health.test.yml -v --skip-current collect

./restic-health.py -c restic-health.test.yml -v check
./restic-health.py -c restic-health.test.yml -v check-read-data

# Cause some corruption in the repo (Note to self: I've tried to surgically break
# something that can only be found with --read-data, but it seemed too hard to
# be worth the effort):
find test-restic-health/repo/data -type f | shuf | head -n1 | xargs rm -v
./restic-health.py -c restic-health.test.yml -v check && exit 1
./restic-health.py -c restic-health.test.yml -v check-read-data && exit 1


echo 'Success!'
