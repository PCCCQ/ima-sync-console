#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ima 文件同步核心库

实现 ima OpenAPI 的文件上传流程:
  check_repeated_names(查重) -> create_media(取凭证) -> 腾讯云 COS PUT -> add_knowledge(入库)

支持的媒体类型见 FILE_TYPES(对应官方 MediaType 枚举):
  PDF / Word / PPT / Excel / Markdown / 图片 / TXT / Xmind / 录音 / HTML / EPUB

凭据获取: https://ima.qq.com/agent-interface
  Client ID -> IMA_CLIENT_ID
  API Key   -> IMA_API_KEY
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://ima.qq.com"
OPENAPI_PATH = "/openapi/wiki/v1"

MAX_AUDIO_SIZE = 200 * 1024 * 1024   # 200MB (官方: 音频/文档等)
MAX_TEXT_SIZE = 10 * 1024 * 1024     # 10MB (官方: Excel/TXT/Xmind/Markdown/HTML)
MAX_IMAGE_SIZE = 30 * 1024 * 1024    # 30MB (官方: 图片)
MAX_EPUB_SIZE = 50 * 1024 * 1024     # 50MB (官方: EPUB)
AUDIO_MAX_DURATION_HINT = "音频最长 2 小时"

# 扩展名 -> (media_type, content_type, 大小上限)
FILE_TYPES = {
    ".pdf":  (1,  "application/pdf", MAX_AUDIO_SIZE),
    ".doc":  (3,  "application/msword", MAX_AUDIO_SIZE),
    ".docx": (3,  "application/vnd.openxmlformats-officedocument.wordprocessingml.document", MAX_AUDIO_SIZE),
    ".ppt":  (4,  "application/vnd.ms-powerpoint", MAX_AUDIO_SIZE),
    ".pptx": (4,  "application/vnd.openxmlformats-officedocument.presentationml.presentation", MAX_AUDIO_SIZE),
    ".xls":  (5,  "application/vnd.ms-excel", MAX_TEXT_SIZE),
    ".xlsx": (5,  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", MAX_TEXT_SIZE),
    ".csv":  (5,  "text/csv", MAX_TEXT_SIZE),
    ".md":   (7,  "text/markdown", MAX_TEXT_SIZE),
    ".markdown": (7, "text/markdown", MAX_TEXT_SIZE),
    ".png":  (9,  "image/png", MAX_IMAGE_SIZE),
    ".jpg":  (9,  "image/jpeg", MAX_IMAGE_SIZE),
    ".jpeg": (9,  "image/jpeg", MAX_IMAGE_SIZE),
    ".webp": (9,  "image/webp", MAX_IMAGE_SIZE),
    ".txt":  (13, "text/plain", MAX_TEXT_SIZE),
    ".xmind": (14, "application/vnd.xmind.workbook", MAX_TEXT_SIZE),
    ".mp3":  (15, "audio/mpeg", MAX_AUDIO_SIZE),
    ".m4a":  (15, "audio/x-m4a", MAX_AUDIO_SIZE),
    ".wav":  (15, "audio/wav", MAX_AUDIO_SIZE),
    ".aac":  (15, "audio/aac", MAX_AUDIO_SIZE),
    ".html": (20, "text/html", MAX_TEXT_SIZE),
    ".htm":  (20, "text/html", MAX_TEXT_SIZE),
    ".epub": (21, "application/epub+zip", MAX_EPUB_SIZE),
}

TYPE_NAMES = {1: "PDF", 3: "Word", 4: "PPT", 5: "Excel", 7: "Markdown", 9: "图片",
              13: "TXT", 14: "Xmind", 15: "录音", 20: "HTML", 21: "EPUB"}

# 向后兼容旧调用方(录音模式)
AUDIO_EXT = {".mp3", ".m4a", ".wav", ".aac"}
MEDIA_TYPE_AUDIO = 15
AUDIO_CONTENT_TYPE = {k: FILE_TYPES[k][1] for k in AUDIO_EXT}


def log(msg):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), msg, flush=True)


def http_post_json(path, payload, client_id, api_key, timeout=60):
    url = BASE_URL + path
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "ima-openapi-clientid": client_id,
            "ima-openapi-apikey": api_key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            data = json.loads(e.read().decode("utf-8"))
        except Exception:
            raise RuntimeError("HTTP %s: %s" % (e.code, e.reason))
    if data.get("code") != 0:
        raise RuntimeError("API %s 失败: code=%s msg=%s" % (path, data.get("code"), data.get("msg")))
    return data.get("data") or {}


def cos_put_object(file_path, cred, content_type, timeout=600):
    """按腾讯云 COS XML API 签名，PUT 上传文件。"""
    secret_id = cred["secret_id"]
    secret_key = cred["secret_key"]
    token = cred["token"]
    bucket = cred["bucket_name"]
    region = cred["region"]
    cos_key = cred["cos_key"]
    start_time = int(cred.get("start_time") or time.time())
    expired_time = int(cred.get("expired_time") or (time.time() + 3600))

    hostname = "%s.cos.%s.myqcloud.com" % (bucket, region)
    pathname = "/" + cos_key

    with open(file_path, "rb") as f:
        content = f.read()
    content_length = str(len(content))

    sign_headers = {"host": hostname, "content-length": content_length}

    key_time = "%d;%d" % (start_time, expired_time)
    sign_key = hmac.new(secret_key.encode(), key_time.encode(), hashlib.sha1).hexdigest()

    header_list = sorted(sign_headers.keys())
    http_headers = "&".join(
        "%s=%s" % (k, urllib.parse.quote(sign_headers[k], safe="")) for k in header_list
    )
    http_string = "put\n%s\n\n%s\n" % (pathname, http_headers)
    string_to_sign = "sha1\n%s\n%s\n" % (key_time, hashlib.sha1(http_string.encode()).hexdigest())
    signature = hmac.new(sign_key.encode(), string_to_sign.encode(), hashlib.sha1).hexdigest()

    auth = "&".join(
        [
            "q-sign-algorithm=sha1",
            "q-ak=" + secret_id,
            "q-sign-time=" + key_time,
            "q-key-time=" + key_time,
            "q-header-list=" + ";".join(header_list),
            "q-url-param-list=",
            "q-signature=" + signature,
        ]
    )

    req = urllib.request.Request(
        "https://" + hostname + pathname,
        data=content,
        method="PUT",
        headers={
            "Host": hostname,
            "Content-Type": content_type,
            "Content-Length": content_length,
            "Authorization": auth,
            "x-cos-security-token": token,
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if not (200 <= resp.status < 300):
            raise RuntimeError("COS 上传失败: HTTP %s" % resp.status)


def detect_file_type(file_path):
    """返回 (media_type, content_type, max_size)，不支持的类型返回 None。"""
    ext = os.path.splitext(file_path)[1].lower()
    return FILE_TYPES.get(ext)


def scan_files(root, include_ext=None, skip_unsupported=True):
    """
    扫描目录下所有可上传文件。
    include_ext: 集合，只保留这些扩展名(小写含点)；None 表示不过滤(全部支持类型)。
    """
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if name.startswith("."):
                continue
            path = os.path.join(dirpath, name)
            info = detect_file_type(path)
            if info is None:
                if not skip_unsupported:
                    files.append(path)
                continue
            if include_ext and os.path.splitext(name)[1].lower() not in include_ext:
                continue
            files.append(path)
    files.sort()
    return files


def scan_audio_files(root):
    """兼容旧接口: 只扫音频。"""
    return scan_files(root, include_ext=AUDIO_EXT)


def load_state(state_file):
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log("状态文件读取失败，将重新记录: %s" % e)
    return {}


def save_state(state_file, state):
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    os.replace(tmp, state_file)


def file_signature(path):
    st = os.stat(path)
    return "%s|%s" % (st.st_size, int(st.st_mtime))


def file_hash(path, chunk_size=1024 * 1024):
    """文件内容 SHA-256，用于检测内容变化(修改后自动重传)。"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def check_repeated(items, kb_id, folder_id, client_id, api_key):
    """items: [{"name": 文件名, "media_type": 类型}]。返回重复文件名集合。
    folder_id 传 kb_id 或空串都表示根目录(payload 中省略)。"""
    if not items:
        return set()
    payload = {"params": items, "knowledge_base_id": kb_id}
    if folder_id and folder_id != kb_id:
        payload["folder_id"] = folder_id
    data = http_post_json(OPENAPI_PATH + "/check_repeated_names", payload, client_id, api_key)
    repeated = set()
    for r in data.get("results") or []:
        if r.get("is_repeated"):
            repeated.add(r.get("name"))
    return repeated


def list_subfolders(kb_id, folder_id, client_id, api_key):
    """列出某云端目录下的子文件夹: {名字: folder_id}。folder_id 传 kb_id 表示根目录。"""
    data = http_post_json(
        OPENAPI_PATH + "/get_knowledge_list",
        {"cursor": "", "limit": 50, "knowledge_base_id": kb_id,
         **({"folder_id": folder_id} if folder_id and folder_id != kb_id else {})},
        client_id, api_key)
    folders = {}
    for key in ("folder_list", "folder_info_list", "folders"):
        for f in data.get(key) or []:
            if f.get("name") and f.get("folder_id"):
                folders[f["name"]] = f["folder_id"]
    if not folders:
        # 兜底: 知识条目里 media_type=99 / media_id 前缀 folder_ 的是文件夹
        for item in data.get("knowledge_list") or []:
            mid = item.get("media_id") or ""
            if item.get("title") and (item.get("media_type") == 99 or mid.startswith("folder_")):
                folders[item["title"]] = mid
    return folders


def create_folder(kb_id, parent_folder_id, name, client_id, api_key):
    """在 parent 下创建子文件夹, 返回 folder_id。"""
    data = http_post_json(
        OPENAPI_PATH + "/create_folder",
        {"knowledge_base_id": kb_id, "parent_folder_id": parent_folder_id, "name": name},
        client_id, api_key)
    return data.get("media_id") or ""


def ensure_folder_path(kb_id, base_folder_id, rel_dir_parts, client_id, api_key, cache):
    """
    确保云端目录层级存在, 返回最终 folder_id。
    rel_dir_parts: 本地相对子目录逐级名称列表, 如 ['2023', '01']；空列表返回 base_folder_id。
    cache: dict, 进程内缓存 {('parent_id','名字'): folder_id}, 避免同一任务重复请求。

    注意: 根目录必须用 kb_id 标识, 但 add_knowledge 时根目录要省略 folder_id
    (传 kb_id 会被服务端当作不存在文件夹)。故 base_folder_id 为空时返回 kb_id,
    由 upload_one/check_repeated 在 payload 里对根目录特殊处理。
    """
    current = base_folder_id or kb_id
    for name in rel_dir_parts:
        key = (current, name)
        if key in cache:
            current = cache[key]
            continue
        existing = list_subfolders(kb_id, current, client_id, api_key)
        fid = existing.get(name)
        if not fid:
            fid = create_folder(kb_id, current, name, client_id, api_key)
            log("创建云端文件夹: %s (在 %s 下)" % (name, current))
        cache[key] = fid
        current = fid
    return current


def upload_one(file_path, kb_id, folder_id, client_id, api_key, title_prefix=""):
    """上传单个文件。返回 media_id。不支持的类型抛 ValueError。

    title_prefix: 云端标题前缀(如 '2023/01'), 体现来源子目录层级, 留空表示根目录文件。
    """
    file_name = os.path.basename(file_path)
    info = detect_file_type(file_path)
    if info is None:
        raise ValueError("不支持的文件类型: %s" % file_name)
    media_type, content_type, max_size = info
    size = os.path.getsize(file_path)
    if size > max_size:
        raise ValueError("文件超过大小上限(%dMB): %s" % (max_size // 1024 // 1024, file_name))

    ext = os.path.splitext(file_name)[1].lower().lstrip(".")
    base_title = os.path.splitext(file_name)[0]
    title = (title_prefix.rstrip("/") + "/" + base_title) if title_prefix else base_title
    # 云端显示名由 file_name 决定(add_knowledge 的 title 会被服务端忽略),
    # create_media/file_info 的 file_name 带相对路径才会在云端显示层级
    display_name = (title_prefix.rstrip("/") + "/" + file_name) if title_prefix else file_name

    data = http_post_json(
        OPENAPI_PATH + "/create_media",
        {
            "file_name": display_name,
            "file_size": size,
            "content_type": content_type,
            "knowledge_base_id": kb_id,
            "file_ext": ext,
        },
        client_id,
        api_key,
    )
    media_id = data["media_id"]
    cred = data["cos_credential"]

    cos_put_object(file_path, cred, content_type)

    payload = {
        "media_type": media_type,
        "media_id": media_id,
        "title": title,
        "knowledge_base_id": kb_id,
        "file_info": {
            "cos_key": cred["cos_key"],
            "file_size": size,
            "last_modify_time": int(os.path.getmtime(file_path)),
            "file_name": display_name,
        },
    }
    if folder_id and folder_id != kb_id:
        # 根目录(folder_id 缺省或等于 kb_id)必须省略, 否则服务端报"文件夹不存在"
        payload["folder_id"] = folder_id
    http_post_json(OPENAPI_PATH + "/add_knowledge", payload, client_id, api_key)
    return media_id


def check_repeated_names(names, kb_id, folder_id, client_id, api_key):
    """兼容旧接口: 传文件名列表(按音频处理)。"""
    return check_repeated(
        [{"name": n, "media_type": MEDIA_TYPE_AUDIO} for n in names],
        kb_id, folder_id, client_id, api_key)


def main():
    parser = argparse.ArgumentParser(description="同步本地文件夹到 ima 知识库")
    parser.add_argument("--dir", required=True, help="本地文件夹路径")
    parser.add_argument("--kb", required=True, help="ima 知识库 ID")
    parser.add_argument("--folder", default="", help="知识库内文件夹 ID（可选，默认根目录）")
    parser.add_argument("--ext", default="", help="只上传指定扩展名，逗号分隔，如 mp3,pdf（默认全部支持类型）")
    parser.add_argument("--client-id", default=os.environ.get("IMA_CLIENT_ID", ""))
    parser.add_argument("--api-key", default=os.environ.get("IMA_API_KEY", ""))
    parser.add_argument("--dry-run", action="store_true", help="只列出待上传文件，不实际上传")
    args = parser.parse_args()

    root = os.path.abspath(args.dir)
    if not os.path.isdir(root):
        log("目录不存在: %s" % root)
        return 2

    client_id = args.client_id.strip()
    api_key = args.api_key.strip()
    if not args.dry_run and (not client_id or not api_key):
        log("缺少凭据: 请通过 --client-id/--api-key 或环境变量 IMA_CLIENT_ID/IMA_API_KEY 提供")
        return 2

    include_ext = None
    if args.ext.strip():
        include_ext = {"." + e.strip().lower().lstrip(".") for e in args.ext.split(",") if e.strip()}

    state_file = os.path.join(root, ".ima_sync_state.json")
    state = load_state(state_file)

    candidates = scan_files(root, include_ext=include_ext)
    if not candidates:
        log("没有发现可上传的文件 (支持的类型: %s)" % ", ".join(sorted(TYPE_NAMES.values())))
        return 0

    todo = []
    for path in candidates:
        rel = os.path.relpath(path, root)
        sig = file_signature(path)
        if state.get(rel) == sig:
            continue
        todo.append(path)

    if not todo:
        log("没有新增或修改的文件，共扫描 %d 个" % len(candidates))
        return 0

    repeated = set()
    if not args.dry_run:
        try:
            items = []
            for p in todo:
                info = detect_file_type(p)
                if info:
                    items.append({"name": os.path.basename(p), "media_type": info[0]})
            repeated = check_repeated(items, args.kb, args.folder, client_id, api_key)
        except RuntimeError as e:
            log("检查重名失败(继续尝试上传): %s" % e)

    pending = [p for p in todo if os.path.basename(p) not in repeated]
    skipped = [p for p in todo if os.path.basename(p) in repeated]
    for p in skipped:
        rel = os.path.relpath(p, root)
        log("跳过(ima中已存在同名): %s" % rel)
        state[rel] = file_signature(p)

    log("待上传 %d 个文件" % len(pending))
    for p in pending:
        log("[dry-run] 将上传: %s" % os.path.relpath(p, root))
    if args.dry_run:
        log("dry-run 模式，不实际上传")
        return 0

    ok, fail = 0, 0
    for path in pending:
        rel = os.path.relpath(path, root)
        try:
            media_id = upload_one(path, args.kb, args.folder, client_id, api_key)
            state[rel] = file_signature(path)
            ok += 1
            log("上传成功: %s (media_id=%s)" % (rel, media_id))
        except Exception as e:
            fail += 1
            log("上传失败: %s -> %s" % (rel, e))
        save_state(state_file, state)
        time.sleep(1)

    log("完成: 成功 %d, 失败 %d" % (ok, fail))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
