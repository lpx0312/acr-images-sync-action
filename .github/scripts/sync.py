#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sys
import time
import json
import shlex
from typing import List, Dict, Tuple

IMAGES_FILE = "images.txt"

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "8"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))
PER_IMAGE_TIMEOUT = int(os.getenv("PER_IMAGE_TIMEOUT", str(20 * 60)))
LOG_FILE = os.getenv("SYNC_LOG_FILE", "sync.log")

TARGET_REGISTRY = os.getenv("TARGET_REGISTRY")
TARGET_NAMESPACE = os.getenv("TARGET_NAMESPACE")
TARGET_USER = os.getenv("TARGET_USER")
TARGET_PASSWORD = os.getenv("TARGET_PASSWORD")
HUAWEI_REGISTRY = os.getenv("HUAWEI_REGISTRY")
HUAWEI_NAMESPACE = os.getenv("HUAWEI_NAMESPACE")
HUAWEI_REGISTRY_USER = os.getenv("HUAWEI_REGISTRY_USER")
HUAWEI_REGISTRY_PASSWORD = os.getenv("HUAWEI_REGISTRY_PASSWORD")
DOCKERHUB_USERNAME = os.getenv("DOCKERHUB_USERNAME")
DOCKERHUB_PASSWORD = os.getenv("DOCKERHUB_PASSWORD")
GHCR_USERNAME = os.getenv("GHCR_USERNAME")
GHCR_TOKEN = os.getenv("GHCR_TOKEN")

SUPPORTED_ARCH = [
    ("linux", "amd64"),
    ("linux", "arm64"),
]


def _needs_v2s2(registry: str) -> bool:
    # 华为云 SWR 基础版拒收顶层 OCI image index，推送时需强制转换为 Docker v2s2/manifest list；
    # 腾讯云 CCR 个人版同样按 v2s2 处理，保证 manifest list 兼容
    host = (registry or "").lower()
    return "myhuaweicloud.com" in host or "tencentyun.com" in host


# 推送目标：dispatch 传入的主目标 + 固定附加目标（华为云 SWR，来自仓库 vars/secrets）
PRIMARY_TARGET = {
    "registry": TARGET_REGISTRY,
    "namespace": TARGET_NAMESPACE,
    "user": TARGET_USER,
    "password": TARGET_PASSWORD,
}
PUSH_TARGETS = [PRIMARY_TARGET]
if all([HUAWEI_REGISTRY, HUAWEI_NAMESPACE, HUAWEI_REGISTRY_USER, HUAWEI_REGISTRY_PASSWORD]):
    if HUAWEI_REGISTRY != TARGET_REGISTRY:
        PUSH_TARGETS.append({
            "registry": HUAWEI_REGISTRY,
            "namespace": HUAWEI_NAMESPACE,
            "user": HUAWEI_REGISTRY_USER,
            "password": HUAWEI_REGISTRY_PASSWORD,
        })
    else:
        print("NOTE: extra target registry same as primary, skip duplicate", file=sys.stderr)

if not all([TARGET_REGISTRY, TARGET_NAMESPACE, TARGET_USER, TARGET_PASSWORD]):
    print("ERROR: missing target registry env", file=sys.stderr)
    sys.exit(1)

# ------------------ log ------------------
_log_fh = None

def _open_log():
    global _log_fh
    _log_fh = open(LOG_FILE, "a", encoding="utf-8")
    _log("=== START SYNC LOG ===")

def _close_log():
    global _log_fh
    if _log_fh:
        _log("=== END SYNC LOG ===")
        _log_fh.close()
        _log_fh = None

def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if _log_fh:
        _log_fh.write(line + "\n")
        _log_fh.flush()

# ------------------ run command ------------------

async def run_cmd(cmd: List[str], timeout: int = None):
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        outs, errs = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, outs.decode(errors="ignore"), errs.decode(errors="ignore")
    except asyncio.TimeoutError:
        if proc:
            try: proc.kill()
            except: pass
        return 124, "", f"TIMEOUT after {timeout}s"
    except Exception as e:
        return 125, "", str(e)

# ------------------ normalize image ------------------

def normalize_image_reference(image: str):
    image = image.strip()
    if "/" in image:
        first_part = image.split("/")[0]
        if "." in first_part or ":" in first_part or first_part == "localhost":
            source_ref = f"docker://{image}"
            clean_name = image.split("/", 1)[1]
            return source_ref, clean_name
    if image.count("/") == 0:
        source_ref = f"docker://docker.io/library/{image}"
        clean_name = image
    else:
        source_ref = f"docker://docker.io/{image}"
        clean_name = image
    return source_ref, clean_name

def dockerhub_inspect_copy_refs(source_ref: str) -> List[str]:
    """
    For images on docker.io only: try mirror.gcr.io first, then docker.io.
    mirror.gcr.io mirrors Docker Hub with the same path (library/... or user/...).
    """
    prefix = "docker://docker.io/"
    if not source_ref.startswith(prefix):
        return [source_ref]
    suffix = source_ref[len(prefix): ]
    mirror_ref = f"docker://mirror.gcr.io/{suffix}"
    return [mirror_ref, source_ref]

# ------------------ login ------------------

async def skopeo_login():
    _log(f"[LOGIN] primary target {TARGET_REGISTRY}")
    rc, out, err = await run_cmd([
        "skopeo", "login",
        "-u", TARGET_USER,
        "-p", TARGET_PASSWORD,
        TARGET_REGISTRY
    ], timeout=60)
    if rc != 0:
        _log(err)
        sys.exit(1)
    for extra in PUSH_TARGETS[1:]:
        _log(f"[LOGIN] extra target {extra['registry']}")
        rc, out, err = await run_cmd([
            "skopeo", "login",
            "-u", extra["user"],
            "-p", extra["password"],
            extra["registry"]
        ], timeout=60)
        if rc != 0:
            _log(f"[WARN] extra target login failed: {err}")
        else:
            _log(f"[LOGIN] extra target success")
    if DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD:
        _log("[LOGIN] dockerhub")
        rc, out, err = await run_cmd([
            "skopeo", "login",
            "-u", DOCKERHUB_USERNAME,
            "-p", DOCKERHUB_PASSWORD,
            "docker.io"
        ], timeout=60)
        if rc != 0:
            _log(f"[WARN] dockerhub login failed: {err}")
        else:
            _log("[LOGIN] dockerhub success")
    else:
        _log("[WARN] dockerhub credential not found, skip login")
    if GHCR_USERNAME and GHCR_TOKEN:
        _log("[LOGIN] ghcr.io")
        rc, out, err = await run_cmd([
            "skopeo", "login",
            "-u", GHCR_USERNAME,
            "-p", GHCR_TOKEN,
            "ghcr.io"
        ], timeout=60)
        if rc != 0:
            _log(f"[WARN] ghcr.io login failed: {err}")
        else:
            _log("[LOGIN] ghcr.io success")
    else:
        _log("[WARN] ghcr credential not found, skip login")

# ------------------ parse images ------------------

def parse_images_file(path: str):
    if not os.path.exists(path):
        _log(f"images file not found: {path}")
        sys.exit(1)
    lines = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    return lines

# ------------------ duplicate detect ------------------

def detect_duplicates(lines: List[str]):
    temp_map = {}
    duplicates = {}
    for image in lines:
        _, clean_name = normalize_image_reference(image)
        image_no_digest = clean_name.split("@")[0]
        parts = image_no_digest.split("/")
        image_name_tag = parts[-1]
        image_name = image_name_tag.split(":")[0]
        namespace = parts[-2] if len(parts) >= 2 else "library"
        if image_name in temp_map:
            if temp_map[image_name] != namespace:
                duplicates[image_name] = True
        else:
            temp_map[image_name] = namespace
    return duplicates

# ------------------ build target ------------------

def build_target(image: str, duplicates: Dict[str, bool], target: Dict[str, str]):
    _, clean_name = normalize_image_reference(image)
    image_no_digest = clean_name.split("@")[0]
    parts = image_no_digest.split("/")
    image_name_tag = parts[-1]
    image_name = image_name_tag.split(":")[0]
    prefix = ""
    if image_name in duplicates:
        if len(parts) >= 2:
            prefix = parts[-2] + "_"
    return f"{target['registry']}/{target['namespace']}/{prefix}{image_name_tag}"

# ------------------ inspect architectures ------------------

def _format_arch_list(arch_list: List[Tuple[str, str]]) -> str:
    return ", ".join(f"{os_name}/{arch}" for os_name, arch in arch_list) if arch_list else "(none)"

async def inspect_architectures(source_ref: str, index: int) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Inspect manifest; for docker.io images try mirror.gcr.io first, then docker.io.
    Returns (img_type, arch_list).
    """
    refs = dockerhub_inspect_copy_refs(source_ref)
    last_err = ""
    for ref in refs:
        rc, out, err = await run_cmd(["skopeo", "inspect", "--raw", ref], timeout=60)
        if rc == 0:
            data = json.loads(out)
            arch_list: List[Tuple[str, str]] = []
            if data.get("manifests"):
                img_type = "multi"
                for m in data["manifests"]:
                    plat = m.get("platform")
                    if plat and (plat.get("os"), plat.get("architecture")) in SUPPORTED_ARCH:
                        arch_list.append((plat.get("os"), plat.get("architecture")))
            else:
                img_type = "single"
                config = data.get("config")
                if config:
                    os_name = config.get("os", "linux")
                    arch = config.get("architecture", "amd64")
                    arch_list.append((os_name, arch))
                else:
                    arch_list.append(("linux", "amd64"))
            wl = _format_arch_list(list(SUPPORTED_ARCH))
            _log(
                f"[{index}] INSPECT ok via {ref} | type={img_type} "
                f"matched_platforms=[{_format_arch_list(arch_list)}] whitelist={wl}"
            )
            return img_type, arch_list
        last_err = err
        _log(f"[{index}] INSPECT fail via {ref}: {err.strip() or '(no stderr)'}")
    raise Exception(f"inspect failed (tried {len(refs)} ref(s)): {last_err}")

# ------------------ sync single arch ------------------

async def sync_single_arch(
    source_refs: List[str], target_ref: str, os_name: str, arch: str, index: int,
    force_v2s2: bool = False
):
    """
    For docker.io images source_refs is [mirror.gcr.io, docker.io]; try in order until one succeeds.
    force_v2s2: 目标为华为云 SWR 时强制转换为 Docker v2s2，避免 SWR 拒收 OCI manifest。
    """
    last_err = ""
    for ri, source_ref in enumerate(source_refs):
        _log(f"[{index}] COPY {os_name}/{arch} source={ri + 1}/{len(source_refs)} {source_ref}")
        cmd = [
            "skopeo", "copy",
            "--override-os", os_name,
            "--override-arch", arch,
            "--retry-times", "3",
        ]
        if force_v2s2:
            cmd += ["--format", "v2s2"]
        cmd += [source_ref, f"docker://{target_ref}"]
        _log(f"[{index}] SKOPEO_COPY: {shlex.join(cmd)}")
        rc, out, err = await run_cmd(cmd, timeout=PER_IMAGE_TIMEOUT)
        if rc == 0:
            return
        last_err = err
        if ri < len(source_refs) - 1:
            _log(f"[{index}] COPY fail, next source: {err.strip() or '(no stderr)'}")
    raise Exception(last_err)

# ------------------ manifest merge ------------------

async def manifest_merge(final_target: str, valid_platforms: List[str], index: int, user: str, password: str):
    _log(f"[{index}] CREATE manifest list")
    template = final_target + "-ARCH-tmp"
    cmd = [
        "manifest-tool",
        "--username", user,
        "--password", password,
        "push",
        "from-args",
        "--platforms", ",".join(valid_platforms),
        "--template", template,
        "--target", final_target
    ]
    rc, out, err = await run_cmd(cmd, timeout=300)
    if rc != 0:
        raise Exception(err)

# ------------------ delete temp images ------------------

async def delete_temp_image(target: str, index: int, user: str, password: str):
    _log(f"[{index}] DELETE TEMP {target}")
    rc, out, err = await run_cmd([
        "skopeo", "delete",
        "--creds", f"{user}:{password}",
        f"docker://{target}"
    ], timeout=120)
    if rc != 0:
        _log(f"[{index}] WARN delete failed: {err}")

# ------------------ sync task ------------------

async def sync_image_task(image: str, duplicates: Dict[str, bool], semaphore: asyncio.Semaphore, index: int):
    async with semaphore:
        start_ts = time.time()
        source_ref, _ = normalize_image_reference(image)
        copy_refs = dockerhub_inspect_copy_refs(source_ref)

        for attempt in range(1, RETRY_COUNT + 2):
            temp_targets = []
            try:
                _log(f"[{index}] START {image} attempt={attempt} targets={len(PUSH_TARGETS)}")

                # 架构探测只做一次，与推送目标无关
                img_type, arch_list = await inspect_architectures(source_ref, index)

                failed_targets = []
                for target in PUSH_TARGETS:
                    final_target = build_target(image, duplicates, target)
                    force_v2s2 = _needs_v2s2(target["registry"])
                    try:
                        if img_type == "single" or len(arch_list) < len(SUPPORTED_ARCH):
                            # 单架构或者缺失某些白名单架构
                            os_name, arch = arch_list[0]
                            await sync_single_arch(copy_refs, final_target, os_name, arch, index, force_v2s2)
                        else:
                            # 多架构
                            valid_platforms = []
                            for os_name, arch in SUPPORTED_ARCH:
                                if (os_name, arch) not in arch_list:
                                    continue
                                temp_target = f"{final_target}-{arch}-tmp"
                                await sync_single_arch(copy_refs, temp_target, os_name, arch, index, force_v2s2)
                                temp_targets.append(temp_target)
                                valid_platforms.append(f"{os_name}/{arch}")

                            if not valid_platforms:
                                raise Exception("no supported arch found")

                            # 多架构成功 → manifest merge
                            if len(valid_platforms) >= 2:
                                await manifest_merge(final_target, valid_platforms, index, target["user"], target["password"])

                            # 删除临时镜像
                            #for item in temp_targets_of_target:
                            #    await delete_temp_image(item, index, target["user"], target["password"])

                        _log(f"[{index}] TARGET OK ({time.time() - start_ts:.1f}s) -> {final_target}")
                    except Exception as te:
                        failed_targets.append((final_target, str(te)))
                        _log(f"[{index}] TARGET FAILED -> {final_target}: {str(te).strip()[:300]}")

                if failed_targets:
                    summary = "; ".join(f"{t}: {e.strip()[:150]}" for t, e in failed_targets)
                    raise Exception(f"{len(failed_targets)}/{len(PUSH_TARGETS)} targets failed: {summary}")

                elapsed = time.time() - start_ts
                _log(f"[{index}] SUCCESS ({elapsed:.1f}s) all {len(PUSH_TARGETS)} targets -> {build_target(image, duplicates, PRIMARY_TARGET)}")
                return 0, build_target(image, duplicates, PRIMARY_TARGET)

            except Exception as e:
                err_msg = str(e)
                _log(f"[{index}] FAILED attempt={attempt}: {err_msg[:500]}")
                for item in temp_targets:
                    for target in PUSH_TARGETS:
                        if item.startswith(f"{target['registry']}/"):
                            try:
                                await delete_temp_image(item, index, target["user"], target["password"])
                            except:
                                pass
                backoff = 300 if "toomanyrequests" in err_msg.lower() else min(30*(2**(attempt-1)),300)
                if attempt <= RETRY_COUNT:
                    _log(f"[{index}] retry after {backoff}s")
                    await asyncio.sleep(backoff)
                else:
                    return 1, build_target(image, duplicates, PRIMARY_TARGET)

# ------------------ main ------------------

async def main():
    _open_log()
    _log(f"CONFIG: MAX_CONCURRENT={MAX_CONCURRENT} RETRY_COUNT={RETRY_COUNT} PER_IMAGE_TIMEOUT={PER_IMAGE_TIMEOUT}")
    _log(f"TARGETS: {[(t['registry'], t['namespace']) for t in PUSH_TARGETS]}")
    await skopeo_login()
    lines = parse_images_file(IMAGES_FILE)
    duplicates = detect_duplicates(lines)
    _log(f"TOTAL IMAGES: {len(lines)}")
    if duplicates:
        _log(f"DUPLICATES: {list(duplicates.keys())}")
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [sync_image_task(img, duplicates, sem, i) for i, img in enumerate(lines, 1)]
    results = await asyncio.gather(*tasks)
    success = 0
    failed = []
    for rc, target in results:
        if rc == 0:
            success += 1
        else:
            failed.append(target)
    _log("===== SUMMARY =====")
    _log(f"SUCCESS: {success}")
    _log(f"FAILED : {len(failed)}")
    if failed:
        for item in failed:
            _log(f"FAILED IMAGE: {item}")
    _close_log()
    if failed:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
