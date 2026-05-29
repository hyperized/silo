# syntax=docker/dockerfile:1.7
#
# Builds a minimal aarch64 Linux guest (kernel + initramfs) for the QEMU NBD
# demo. The initramfs is the whole Alpine rootfs with deploy/nbd-demo/vm/init as
# /init; it carries busybox, e2fsprogs (mkfs.ext4), and the virtio_blk/ext4
# kernel modules. `docker build --output type=local` extracts the kernel
# (vmlinuz) and initramfs.cpio.gz to the host, where run-vm.sh boots them under
# QEMU and attaches a silo volume as the guest's virtio disk over NBD.
#
# Built for arm64 so the guest matches Apple Silicon's HVF acceleration:
#   docker build --platform linux/arm64 -f deploy/nbd-demo/vm/Dockerfile \
#     --output type=local,dest=deploy/nbd-demo/vm/out .

FROM --platform=linux/arm64 alpine:3.21 AS build

# linux-virt: a VM-tuned kernel + its modules. cpio/gzip: pack the initramfs.
# e2fsprogs: mkfs.ext4 in the guest. kmod: depmod so guest modprobe resolves
# module dependencies (busybox modprobe needs modules.dep).
RUN apk add --no-cache linux-virt e2fsprogs cpio kmod

COPY deploy/nbd-demo/vm/init /init
RUN chmod +x /init

# Stage the kernel and initramfs into /out, which is excluded from the
# initramfs itself. The kernel is direct-booted by QEMU (-kernel), so it must
# be a raw Image: decompress it when Alpine ships it gzip-compressed.
RUN set -eu; \
    mkdir /out; \
    for kver in /lib/modules/*; do depmod "$(basename "$kver")"; done; \
    if gzip -t /boot/vmlinuz-virt 2>/dev/null; then \
        zcat /boot/vmlinuz-virt >/out/vmlinuz; \
    else \
        cp /boot/vmlinuz-virt /out/vmlinuz; \
    fi; \
    cd /; \
    find . -xdev \
        -not -path './proc/*' \
        -not -path './sys/*' \
        -not -path './dev/*' \
        -not -path './boot/*' \
        -not -path './out' -not -path './out/*' \
        | cpio -o -H newc 2>/dev/null | gzip -9 >/out/initramfs.cpio.gz

# Empty final stage so `--output type=local` extracts only the two artifacts.
FROM scratch AS artifacts
COPY --from=build /out/vmlinuz /vmlinuz
COPY --from=build /out/initramfs.cpio.gz /initramfs.cpio.gz
