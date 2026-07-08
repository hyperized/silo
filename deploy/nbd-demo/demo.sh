#!/usr/bin/env bash
# End-to-end NBD demo: claim credentials, create a volume, attach it over NBD,
# mkfs+mount, write+read a file, then detach/re-attach to prove the data
# persists. Runs inside the privileged nbd-demo container on the silo network.
# SILO_DEMO_TOKEN and SILO_DEMO_FP are injected by `make nbd-demo`.
set -euo pipefail

BOOT=silo-a:7001
GRPC=silo-a:7000
NBD_HOST=silo-a
NBD_PORT=10809
VOL=/demo
DEV=/dev/nbd0
MNT=/mnt/demo

if [ -z "${SILO_DEMO_TOKEN:-}" ] || [ -z "${SILO_DEMO_FP:-}" ]; then
  echo "SILO_DEMO_TOKEN and SILO_DEMO_FP must be set (run via 'make nbd-demo')." >&2
  exit 1
fi

echo "==> Claiming cluster credentials from ${BOOT}"
siloctl auth init --token "$SILO_DEMO_TOKEN" --server "$BOOT" --server-fingerprint "$SILO_DEMO_FP"

echo "==> Creating a 256 MiB volume at ${VOL}"
# Flags must precede the path: Go's flag parser stops at the first positional.
siloctl volume create --size 256M --extent-size 64K --server "$GRPC" "$VOL"

echo "==> Loading the nbd kernel module"
if ! modprobe nbd nbds_max=1 2>/dev/null; then
  echo "" >&2
  echo "This host's kernel has no 'nbd' module, so the mkfs+mount step cannot run." >&2
  echo "The credential, volume-create, and NBD-serving steps above all succeeded —" >&2
  echo "only the kernel mount needs a Linux host with NBD (the macOS Docker VM does" >&2
  echo "not ship it). On a Linux host: 'sudo modprobe nbd' first, then re-run." >&2
  exit 1
fi

echo "==> Attaching ${VOL} as ${DEV} over NBD (${NBD_HOST}:${NBD_PORT})"
nbd-client "$NBD_HOST" "$NBD_PORT" "$DEV" -name "$VOL"

echo "==> mkfs.ext4 + mount"
mkfs.ext4 -q "$DEV"
mkdir -p "$MNT"
mount "$DEV" "$MNT"

echo "==> Writing a file"
echo "hello from a silo block volume" > "$MNT/hello.txt"
sync
cat "$MNT/hello.txt"

echo "==> Unmounting and detaching"
umount "$MNT"
nbd-client -d "$DEV"

echo "==> Re-attaching and remounting to prove persistence"
nbd-client "$NBD_HOST" "$NBD_PORT" "$DEV" -name "$VOL"
mount "$DEV" "$MNT"
echo "==> File survived the detach/re-attach round trip:"
cat "$MNT/hello.txt"
umount "$MNT"
nbd-client -d "$DEV"

echo "==> NBD demo OK"
