#!/usr/bin/env bash
# Runs pytest against throwaway Redis containers: scripts/run_redis_tests.sh [pytest argument...]
#
# The tests find the shared server in TANDEM_TEST_REDIS_URL and a second one, which a test may shut
# down and restart, in TANDEM_TEST_RESTARTABLE_REDIS_URL with its container name in
# TANDEM_TEST_RESTARTABLE_REDIS_CONTAINER. Both containers go when the run ends, is stopped or the
# caller dies; an image this run pulled goes with them. Keep the image equal to the platform base's.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE='docker.io/library/redis:7.4.12-alpine@sha256:338fbfc036dc5517ac0b04d6de21521085935336e565f223406180e9d84166e2'
# The cap and its 30 s grace are the repository-wide rule; the containers stop within the grace.
CAP_SECONDS=1800
GRACE_SECONDS=30
LOAD_WAIT_SECONDS=1800
READY_WAIT_SECONDS=60

# Re-run this script under the cap, so that the trap below also runs when the cap or the caller's death ends it.
if [[ -z "${TANDEM_REDIS_TESTS_CAPPED:-}" ]]; then
    TANDEM_REDIS_TESTS_CAPPED=1 exec setpriv --pdeathsig TERM timeout -k "${GRACE_SECONDS}" "${CAP_SECONDS}" "$0" "$@"
fi

wait_for_low_load() {
    local limit waited=0 load
    limit=$(awk -v cores="$(nproc)" 'BEGIN { print 1.5 * cores }')
    while load=$(cut -d' ' -f1 /proc/loadavg) && awk -v load="${load}" -v limit="${limit}" 'BEGIN { exit !(load > limit) }'; do
        if [[ "${waited}" -ge "${LOAD_WAIT_SECONDS}" ]]; then
            echo "load ${load} stayed above ${limit}" >&2
            exit 1
        fi
        sleep 10
        waited=$((waited + 10))
    done
}

wait_for_low_load

pulled_image=''
if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    docker pull --quiet "${IMAGE}" >/dev/null
    pulled_image=1
fi

shared="tandem-test-redis-$$"
restartable="tandem-test-redis-restartable-$$"
cleanup() {
    docker rm --force "${shared}" "${restartable}" >/dev/null 2>&1 || true
    if [[ -n "${pulled_image}" ]]; then
        docker rmi "${IMAGE}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT TERM

# A restarted container keeps its published port only when the port is fixed, so this one is chosen.
restartable_port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
docker run --quiet --detach --name "${shared}" --publish 127.0.0.1::6379 "${IMAGE}" --save '' --appendonly no >/dev/null
docker run --quiet --detach --name "${restartable}" --publish "127.0.0.1:${restartable_port}:6379" "${IMAGE}" --save '' --appendonly no >/dev/null

for container in "${shared}" "${restartable}"; do
    for _ in $(seq "${READY_WAIT_SECONDS}"); do
        [[ "$(docker exec "${container}" redis-cli ping 2>/dev/null || true)" == PONG ]] && break
        sleep 1
    done
    docker exec "${container}" redis-cli ping >/dev/null
done

shared_port=$(docker port "${shared}" 6379/tcp | head -n 1 | sed 's/.*://')
# Run as a child, not exec'd, so that the trap above survives the command.
TANDEM_TEST_REDIS_URL="redis://127.0.0.1:${shared_port}" \
TANDEM_TEST_RESTARTABLE_REDIS_URL="redis://127.0.0.1:${restartable_port}" \
TANDEM_TEST_RESTARTABLE_REDIS_CONTAINER="${restartable}" \
    uv run --no-sync pytest "$@"
