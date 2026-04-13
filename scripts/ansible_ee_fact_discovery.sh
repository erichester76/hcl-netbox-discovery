#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLAYBOOK_PATH="${REPO_ROOT}/scripts/ansible_fact_discovery.yml"

: "${ANSIBLE_INVENTORY:?Set ANSIBLE_INVENTORY to an inventory file or directory.}"

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-podman}"
ANSIBLE_EE_IMAGE="${ANSIBLE_EE_IMAGE:-quay.io/ansible/creator-ee:latest}"
ANSIBLE_OUTPUT_DIR="${ANSIBLE_OUTPUT_DIR:-${REPO_ROOT}/artifacts/ansible-facts}"
ANSIBLE_LIMIT="${ANSIBLE_LIMIT:-}"
ANSIBLE_REMOTE_USER="${ANSIBLE_REMOTE_USER:-}"
ANSIBLE_PRIVATE_KEY_FILE="${ANSIBLE_PRIVATE_KEY_FILE:-}"
ANSIBLE_HOST_KEY_CHECKING="${ANSIBLE_HOST_KEY_CHECKING:-false}"
ANSIBLE_EE_DEBUG="${ANSIBLE_EE_DEBUG:-false}"

if [[ ! -f "${PLAYBOOK_PATH}" ]]; then
  printf 'Playbook not found: %s\n' "${PLAYBOOK_PATH}" >&2
  exit 1
fi

mkdir -p "${ANSIBLE_OUTPUT_DIR}/fact-cache"

if [[ -d "${ANSIBLE_INVENTORY}" ]]; then
  inventory_mount="${ANSIBLE_INVENTORY}"
  inventory_target="/inventory"
else
  inventory_mount="$(cd "$(dirname "${ANSIBLE_INVENTORY}")" && pwd)"
  inventory_target="/inventory/$(basename "${ANSIBLE_INVENTORY}")"
fi

runtime_args=(
  run --rm
  -v "${inventory_mount}:/inventory:ro"
  -v "${ANSIBLE_OUTPUT_DIR}:/output"
  -v "${PLAYBOOK_PATH}:/playbooks/ansible_fact_discovery.yml:ro"
  -e "ANSIBLE_INVENTORY=${inventory_target}"
  -e "ANSIBLE_OUTPUT_DIR=/output"
  -e "ANSIBLE_LIMIT=${ANSIBLE_LIMIT}"
  -e "ANSIBLE_REMOTE_USER=${ANSIBLE_REMOTE_USER}"
  -e "ANSIBLE_HOST_KEY_CHECKING=${ANSIBLE_HOST_KEY_CHECKING}"
)

if [[ -n "${ANSIBLE_PRIVATE_KEY_FILE}" ]]; then
  runtime_args+=(
    -v "${ANSIBLE_PRIVATE_KEY_FILE}:/ssh/id_rsa:ro"
    -e "ANSIBLE_PRIVATE_KEY_FILE=/ssh/id_rsa"
  )
else
  runtime_args+=(-e "ANSIBLE_PRIVATE_KEY_FILE=")
fi

container_script="$(cat <<'EOF'
set -euo pipefail

mkdir -p /output/fact-cache

cat >/tmp/ansible.cfg <<CFG
[defaults]
inventory = ${ANSIBLE_INVENTORY}
fact_caching = jsonfile
fact_caching_connection = /output/fact-cache
fact_caching_timeout = 86400
gathering = smart
host_key_checking = ${ANSIBLE_HOST_KEY_CHECKING}
retry_files_enabled = False
stdout_callback = yaml
interpreter_python = auto_silent
CFG

export ANSIBLE_CONFIG=/tmp/ansible.cfg

playbook_args=("/playbooks/ansible_fact_discovery.yml")
if [[ -n "${ANSIBLE_LIMIT}" ]]; then
  playbook_args+=("--limit" "${ANSIBLE_LIMIT}")
fi
if [[ -n "${ANSIBLE_REMOTE_USER}" ]]; then
  playbook_args+=("--user" "${ANSIBLE_REMOTE_USER}")
fi
if [[ -n "${ANSIBLE_PRIVATE_KEY_FILE}" ]]; then
  playbook_args+=("--private-key" "${ANSIBLE_PRIVATE_KEY_FILE}")
fi

ansible-playbook "${playbook_args[@]}"
EOF
)"

case "${ANSIBLE_EE_DEBUG}" in
  1|true|TRUE|yes|YES|on|ON)
    set -x
    ;;
esac

"${CONTAINER_RUNTIME}" "${runtime_args[@]}" "${ANSIBLE_EE_IMAGE}" /bin/bash -lc "${container_script}"

printf '\nAnsible fact cache written to %s/fact-cache\n' "${ANSIBLE_OUTPUT_DIR}"
