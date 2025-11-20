#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, unquote, urlparse
from urllib.request import Request, urlopen
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


BASE_API = "https://chorig.org/wp-json/app/v1/categories/{category_id}/prayers/"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def sanitize_name(raw: str) -> str:
    if not raw:
        return "Untitled"
    # Replace filesystem-unfriendly characters
    bad_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    name = "".join('-' if c in bad_chars else c for c in raw)
    name = name.strip().strip(".")
    while "  " in name:
        name = name.replace("  ", " ")
    return name or "Untitled"


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
    if requests is not None:
        resp = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    # Fallback to urllib
    if params:
        qs = urlencode(params, doseq=True)
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{qs}"
    req = Request(url, headers=DEFAULT_HEADERS, method="GET")
    with urlopen(req, timeout=timeout) as r:
        data = r.read()
    return json.loads(data.decode("utf-8"))


def stream_download(url: str, dest_path: str, timeout: int = 60, chunk_size: int = 1 << 20) -> None:
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        return
    ensure_dir(os.path.dirname(dest_path))
    # Build robust headers, including a sane Referer for hosts that require it
    parsed = urlparse(url)
    headers = dict(DEFAULT_HEADERS)
    if parsed.scheme and parsed.netloc:
        headers.setdefault("Referer", f"{parsed.scheme}://{parsed.netloc}/")
    headers.setdefault("Accept", "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8")

    if requests is not None:
        with requests.get(
            url,
            stream=True,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        ) as r:
            r.raise_for_status()
            content_type = (r.headers.get("Content-Type") or "").lower()
            first_chunk: Optional[bytes] = None
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    if first_chunk is None:
                        first_chunk = chunk
                    f.write(chunk)
            # Validate PDF downloads when applicable
            _, ext = os.path.splitext(dest_path)
            if (ext.lower() == ".pdf" or "pdf" in content_type):
                if not first_chunk or (not first_chunk.startswith(b"%PDF-") and "pdf" not in content_type):
                    try:
                        os.remove(dest_path)
                    except Exception:
                        pass
                    raise RuntimeError(
                        f"Downloaded content is not a PDF (Content-Type: {content_type or 'unknown'})"
                    )
        return
    # urllib fallback
    req = Request(url, headers=headers, method="GET")
    with urlopen(req, timeout=timeout) as r:
        content_type = (r.headers.get("Content-Type") or "").lower()
        first_chunk: Optional[bytes] = None
        with open(dest_path, "wb") as f:
            while True:
                chunk = r.read(chunk_size)
                if not chunk:
                    break
                if first_chunk is None and chunk:
                    first_chunk = chunk
                f.write(chunk)
    # Validate PDF downloads when applicable (urllib path)
    _, ext = os.path.splitext(dest_path)
    if ext.lower() == ".pdf":
        if not first_chunk or not first_chunk.startswith(b"%PDF-"):
            try:
                os.remove(dest_path)
            except Exception:
                pass
            raise RuntimeError("Downloaded content is not a PDF")


def load_category_mapping(mapping_path: str) -> Dict[int, str]:
    with open(mapping_path, "r", encoding="utf-8") as f:
        raw = f.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # The file appears to be a Python dict literal (keys unquoted). Fall back safely.
        data = ast.literal_eval(raw)
    mapping: Dict[int, str] = {}
    for k, v in data.items():
        try:
            ik = int(k)
        except Exception:
            # Keep as-is if not an int-string; still usable if API accepts it
            ik = k  # type: ignore
        mapping[ik] = v
    return mapping


def iter_prayers(category_id: int) -> Iterable[Dict[str, Any]]:
    """
    Iterate prayers for a category using page number embedded in the URL path.
    Page numbering starts from 0 and continues until the API returns an empty
    'prayers' array.
    """
    seen_ids: Set[int] = set()
    base_url = BASE_API.format(category_id=category_id)
    total_count: Optional[int] = None

    page = 0
    while True:
        page_url = f"{base_url}{page}"
        try:
            data = http_get_json(page_url)
        except Exception:
            break
        total_count = data.get("totalCount", total_count)
        prayers = data.get("prayers") or []
        if not prayers:
            break
        for p in prayers:
            if not isinstance(p, dict):
                continue
            pid = p.get("id")
            if isinstance(pid, int):
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
            yield p
        if total_count is not None and len(seen_ids) >= int(total_count):
            break
        page += 1


def build_category_dirname(category_id: int, title: str) -> str:
    return f"{category_id} - {sanitize_name(title)}"


def build_prayer_dirname(prayer: Dict[str, Any]) -> str:
    pid = prayer.get("id", "unknown")
    name = sanitize_name(str(prayer.get("name") or "Untitled"))
    return f"{pid} - {name}"


def filename_from_url(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    raw = unquote(os.path.basename(parsed.path)) or fallback
    return sanitize_name(raw)


def unique_filename(directory: str, base_name: str) -> str:
    """
    Ensure filename uniqueness by appending numeric suffix if needed.
    """
    root, ext = os.path.splitext(base_name)
    candidate = base_name
    counter = 1
    while os.path.exists(os.path.join(directory, candidate)):
        candidate = f"{root} ({counter}){ext}"
        counter += 1
    return candidate


def save_metadata(prayer_dir: str, prayer: Dict[str, Any]) -> None:
    meta_path = os.path.join(prayer_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(prayer, f, ensure_ascii=False, indent=2)

def pdf_to_txt(file_path: Path, output_dir: Path, extracted_text: str):
    """Converts pdf file to a txt file"""
    output_file = output_dir / f"{file_path.stem}.txt"
    text = ""
    file_type = file_path.suffix[1:]

    if file_type == "pdf":
        text = extracted_text
        if text:
            with open(output_file, "w", encoding="utf-8") as file:
                file.write(text)
            return output_file


def read_pdf_file(pdf_file_path: Path) -> str:
    """Reads the content of a PDF file using pypdf."""
    text = ""
    try:
        with open(pdf_file_path, "rb") as pdf_file:
            pdf_reader = PdfReader(pdf_file)
            for page in pdf_reader.pages:
                text += page.extract_text() if page.extract_text() else ""
        return text
    except Exception as e:
        print(f"pdf file {pdf_file_path} is corrupted")
        return ""


def download_assets_for_prayer(prayer_dir: str, prayer: Dict[str, Any]) -> None:
    # Audio tracks
    tracks: List[Dict[str, Any]] = prayer.get("tracks") or []
    if tracks:
        audio_dir = os.path.join(prayer_dir, "audio")
        ensure_dir(audio_dir)
        for index, track in enumerate(tracks, start=1):
            url = (track or {}).get("url")
            if not url:
                continue
            preferred_name = (track or {}).get("name") or f"track_{index}"
            fname = filename_from_url(url, f"{preferred_name}.bin")
            # Prefix with index for stable ordering
            root, ext = os.path.splitext(fname)
            fname = f"{index:02d} - {root}{ext}"
            fname = unique_filename(audio_dir, fname)
            dest = os.path.join(audio_dir, fname)
            try:
                stream_download(url, dest)
            except Exception as e:
                print(f"[warn] Failed to download track: {url} -> {dest}: {e}")

    # Documents (PDFs)
    docs: List[Dict[str, Any]] = prayer.get("documents") or []
    if docs:
        docs_dir = os.path.join(prayer_dir, "documents")
        ensure_dir(docs_dir)
        for index, doc in enumerate(docs, start=1):
            url = (doc or {}).get("url")
            if not url:
                continue
            preferred_name = (doc or {}).get("name") or f"document_{index}"
            fname = filename_from_url(url, f"{preferred_name}.pdf")
            # Prefix with index for stable ordering
            root, ext = os.path.splitext(fname)
            if not ext:
                ext = ".pdf"
            fname = f"{index:02d} - {root}{ext}"
            fname = unique_filename(docs_dir, fname)
            dest = os.path.join(docs_dir, fname)
            try:
                stream_download(url, dest)
                extracted_text = read_pdf_file(dest)
                if extracted_text:
                    pdf_to_txt(dest, docs_dir, extracted_text)
            except Exception as e:
                print(f"[warn] Failed to download document: {url} -> {dest}: {e}")


def scrape_category(category_id: int, category_title: str, output_root: str) -> int:
    cat_dir = os.path.join(output_root, build_category_dirname(category_id, category_title))
    ensure_dir(cat_dir)
    count = 0
    for prayer in iter_prayers(category_id):
        pdir = os.path.join(cat_dir, build_prayer_dirname(prayer))
        ensure_dir(pdir)
        save_metadata(pdir, prayer)
        download_assets_for_prayer(pdir, prayer)
        count += 1
    return count


def main() -> None:
    project_root = os.path.dirname(os.path.abspath(__file__))
    mapping_path = os.path.join(project_root, "category_mapping.json")
    output_root = os.path.join(project_root, "downloads")
    ensure_dir(output_root)

    try:
        mapping = load_category_mapping(mapping_path)
    except Exception as e:
        print(f"[error] Failed to load category mapping from {mapping_path}: {e}", file=sys.stderr)
        sys.exit(1)

    total_prayers = 0
    for category_id, category_title in mapping.items():
        print(f"==> Category {category_id}: {category_title}")
        try:
            count = scrape_category(int(category_id), str(category_title), output_root)
            print(f"    Downloaded {count} prayers.")
            total_prayers += count
        except Exception as e:
            print(f"[error] Category {category_id} failed: {e}", file=sys.stderr)
            continue

    print(f"All done. Total prayers processed: {total_prayers}")


if __name__ == "__main__":
    main()

