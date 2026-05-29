#!/usr/bin/env bash
# Boots a throwaway aarch64 Linux guest under QEMU and attaches a silo block
# volume as its virtio disk *over NBD* — making QEMU's own NBD client the
# independent verifier of silo's hand-rolled NBD server. The guest runs
# mkfs.ext4 + mount + write + read on the volume and prints a sentinel this
# script greps for.
#
# Unlike the container demo (deploy/nbd-demo/demo.sh, which needs a Linux host
# with the /dev/nbd kernel module), this path runs fully on macOS via the
# Hypervisor framework — no host NBD kernel module required.
#
# Driven by `make nbd-demo-vm`, which boots the cluster and passes the bootstrap
# token+fingerprint in via SILO_DEMO_TOKEN / SILO_DEMO_FP. The volume is created
# with a throwaway config dir so the operator's real siloctl credentials are
# never touched.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

BOOT_ADDR="${SILO_BOOT_ADDR:-127.0.0.1:7001}"
GRPC_ADDR="${SILO_GRPC_ADDR:-127.0.0.1:7900}"
NBD_HOST="${SILO_NBD_HOST:-127.0.0.1}"
NBD_PORT="${SILO_NBD_HOST_PORT:-10809}"
VOL="${SILO_DEMO_VOL:-/demo}"
SIZE="${SILO_DEMO_SIZE:-256M}"
EXTENT="${SILO_DEMO_EXTENT:-64K}"
NBD_URL="nbd:${NBD_HOST}:${NBD_PORT}:exportname=${VOL}"
OUT="$SCRIPT_DIR/out"

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

# --- preflight: every failure says how to fix it -----------------------------
for tool in qemu-system-aarch64 qemu-img qemu-io; do
	command -v "$tool" >/dev/null 2>&1 || die "QEMU tool '$tool' not found. Install QEMU first:
  brew install qemu                              (macOS)
  apt-get install qemu-system-arm qemu-utils     (Debian/Ubuntu)"
done
command -v docker >/dev/null 2>&1 || die "docker not found; it builds the guest kernel+initramfs. Start Docker Desktop or install docker."
command -v go >/dev/null 2>&1 || die "go not found; it builds siloctl. Install Go from https://go.dev/dl/."

[ -n "${SILO_DEMO_TOKEN:-}" ] || die "SILO_DEMO_TOKEN is empty. Run this via 'make nbd-demo-vm', which scrapes the bootstrap token from silo-a's logs."
[ -n "${SILO_DEMO_FP:-}" ] || die "SILO_DEMO_FP is empty. Run this via 'make nbd-demo-vm'."

# --- build siloctl, then claim creds + create the volume ---------------------
echo "==> Building siloctl"
(cd "$REPO_ROOT" && CGO_ENABLED=0 go build -o bin/siloctl ./cmd/siloctl)
SILOCTL="$REPO_ROOT/bin/siloctl"

CFG="$(mktemp -d)"
trap 'rm -rf "$CFG"' EXIT
echo "==> Claiming credentials into a throwaway config dir (your real siloctl config is untouched)"
"$SILOCTL" auth init --config-dir "$CFG" --token "$SILO_DEMO_TOKEN" --server "$BOOT_ADDR" --server-fingerprint "$SILO_DEMO_FP"

echo "==> Creating volume $VOL ($SIZE, extent $EXTENT) — a pre-existing volume is fine"
SILO_CA_CERT="$CFG/ca.crt" SILO_CLIENT_CERT="$CFG/client.crt" SILO_CLIENT_KEY="$CFG/client.key" \
	"$SILOCTL" volume create --server "$GRPC_ADDR" --size "$SIZE" --extent-size "$EXTENT" "$VOL" || true

# --- build the guest kernel + initramfs (cached after the first run) ---------
echo "==> Building the aarch64 guest image (kernel + initramfs)"
docker build --platform linux/arm64 \
	-f "$SCRIPT_DIR/Dockerfile" \
	--output "type=local,dest=$OUT" \
	"$REPO_ROOT"
{ [ -f "$OUT/vmlinuz" ] && [ -f "$OUT/initramfs.cpio.gz" ]; } ||
	die "guest artifacts missing under $OUT after the build."

# --- host-side NBD smoke via QEMU's own client -------------------------------
# qemu-img/qemu-io are an independent NBD client: if they can read the geometry
# and round-trip a block, the protocol handshake and READ/WRITE path are sound
# before we even boot a kernel.
echo "==> Reading export geometry (qemu-img)"
qemu-img info "$NBD_URL" >/dev/null ||
	die "could not read the NBD export at $NBD_URL.
Is the cluster up and the volume created? Try:  make down && make nbd-demo-vm"
echo "==> Block read/write integrity check (qemu-io)"
qemu-io -f raw -c "write -P 0x5a 0 4096" -c "read -P 0x5a 0 4096" "$NBD_URL" >/dev/null

# --- pick the best accelerator available -------------------------------------
if qemu-system-aarch64 -accel help 2>/dev/null | grep -qw hvf; then
	ACCEL=hvf CPU=host # macOS Hypervisor framework
elif qemu-system-aarch64 -accel help 2>/dev/null | grep -qw kvm; then
	ACCEL=kvm CPU=host # Linux KVM
else
	ACCEL=tcg CPU=max # pure emulation fallback (slow)
fi
echo "==> Booting guest (accel=$ACCEL) with $VOL attached as /dev/vda over NBD"

LOG="$(mktemp)"
qemu-system-aarch64 \
	-machine virt -accel "$ACCEL" -cpu "$CPU" -m 512 \
	-kernel "$OUT/vmlinuz" -initrd "$OUT/initramfs.cpio.gz" \
	-append "console=ttyAMA0 rdinit=/init panic=-1" \
	-drive "file=$NBD_URL,if=virtio,format=raw" \
	-display none -serial "file:$LOG" -monitor none -no-reboot >/dev/null 2>&1 &
QPID=$!

# The guest powers off on its own; this watchdog is the safety net for a hang.
for _ in $(seq 1 120); do
	grep -q "NBD-VM-OK\|NBD-VM-FAIL" "$LOG" 2>/dev/null && break
	kill -0 "$QPID" 2>/dev/null || break
	sleep 1
done
kill "$QPID" 2>/dev/null || true
wait "$QPID" 2>/dev/null || true

echo "----- guest report -----"
grep -E "NBD-VM" "$LOG" || true
echo "------------------------"

if grep -q "NBD-VM-OK" "$LOG"; then
	echo "==> QEMU NBD demo OK — silo served a real ext4 filesystem to QEMU's independent NBD client."
	rm -f "$LOG"
	exit 0
fi
printf 'Full guest serial log kept at: %s\n' "$LOG" >&2
die "guest did not report success — it could not mkfs/mount the NBD-backed disk. See the serial log above."
