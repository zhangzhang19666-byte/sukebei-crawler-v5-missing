#!/usr/bin/env python3
"""
V5 Missing-ID Crawler — GitHub Actions 版
从 missing_*.txt 文件中读取需要补爬的 ID，逐个爬取，记录进度。
适配 na-main 的 GitHub Actions 工作流模式（本地文件 + git 提交）。
"""
import asyncio
import json
import re
import random
import argparse
from pathlib import Path
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# --- Configuration ---
H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}
BASE_URL = "https://sukebei.nyaa.si/view/{}"

# ---------------------------------------------------------------------------
def parse_html(html, id_val):
    soup = BeautifulSoup(html, "html.parser")
    if soup.find("div", class_="alert-danger"):
        return None
    title_tag = soup.find("h3", class_="panel-title")
    if not title_tag:
        return None
    res = {"id": id_val, "title": title_tag.get_text(strip=True)}
    magnet_tag = soup.find("a", href=re.compile(r"^magnet:\?"))
    res["magnet"] = magnet_tag["href"] if magnet_tag else None
    if res["magnet"]:
        m = re.search(r"btih:([a-fA-F0-9]{40})", res["magnet"])
        res["info_hash"] = m.group(1).lower() if m else None
    ts_tag = soup.find(attrs={"data-timestamp": True})
    if ts_tag:
        try:
            res["uploaded_at"] = datetime.fromtimestamp(
                int(ts_tag["data-timestamp"]), tz=timezone.utc).isoformat()
        except Exception:
            pass
    def get_int(id_attr):
        tag = soup.find(id=id_attr)
        try:   return int(tag.get_text(strip=True)) if tag else 0
        except: return 0
    res["seeders"]  = get_int("seeders")
    res["leechers"] = get_int("leechers")
    for row in soup.select(".panel-body .row"):
        cols = row.find_all("div", recursive=False)
        if len(cols) < 2:
            continue
        key = cols[0].get_text(strip=True).rstrip(":")
        val = cols[1].get_text(strip=True)
        if   key == "Category":              res["category"]    = val
        elif key == "Submitter":             res["submitter"]   = val
        elif key in ("File size", "Size"):   res["size"]        = val
        elif key == "Completed":
            try:   res["completed"] = int(val)
            except: res["completed"] = 0
        elif key == "Information":
            a = cols[1].find("a")
            res["information"] = a["href"] if a else val
    d = soup.find(id="torrent-description")
    res["description"] = d.get_text(strip=True) if d else None
    return res

# ---------------------------------------------------------------------------
async def fetch_one(id_val, session, min_delay, max_delay):
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    try:
        resp = await session.get(BASE_URL.format(id_val), timeout=15)
        if resp.status_code == 404:
            return id_val, None, "404"
        if resp.status_code == 429:
            return id_val, None, "429"
        resp.raise_for_status()
        data = parse_html(resp.text, id_val)
        return id_val, data, "ok" if data else "parse_fail"
    except Exception as e:
        return id_val, None, str(e)[:50]

# ---------------------------------------------------------------------------
def load_progress(progress_file):
    try:
        if Path(progress_file).exists():
            with open(progress_file, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_progress(progress, progress_file):
    try:
        with open(progress_file, "w") as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        print(f" [!] Failed to save progress: {e}")

# ---------------------------------------------------------------------------
async def run_v5_missing_mode(missing_dir, workers, batch_size, batch_timeout,
                              min_delay, max_delay, proxy, progress_file):
    """
    V5 缺失ID模式：
    - 从 missing_*.txt 读取待爬ID
    - 用 progress_file 记录每个文件已处理到第几行
    - 每批处理 batch_size 个ID
    - 结果写入 JSONL 并返回文件名
    """
    # 1. 加载进度
    progress = load_progress(progress_file)
    
    # 2. 列出所有 missing_*.txt 文件
    txt_files = sorted(Path(missing_dir).glob("missing_*.txt"))
    if not txt_files:
        print("[*] V5: No missing_*.txt files found.")
        return None

    pending_ids = []
    file_contributions = {}

    # 3. 收集 ID 直到填满 batch_size
    for fpath in txt_files:
        if len(pending_ids) >= batch_size:
            break
        
        fname = fpath.name
        dispatched = progress.get(fname, 0)
        
        with open(fpath, "r") as f:
            lines = [l.strip() for l in f.readlines()]
        
        available = [int(l) for l in lines[dispatched:] if l.isdigit()]
        take = min(len(available), batch_size - len(pending_ids))
        if take > 0:
            pending_ids.extend(available[:take])
            file_contributions[fname] = take
    
    if not pending_ids:
        print("[*] V5: No pending IDs. All missing files consumed.")
        return None
    
    print(f"[*] V5: Taking {len(pending_ids)} IDs: {pending_ids[0]} ... {pending_ids[-1]}")
    
    # 4. 先存进度书签（防超时丢失）
    for fname, take in file_contributions.items():
        progress[fname] = progress.get(fname, 0) + take
    save_progress(progress, progress_file)
    print(f"[*] V5 Checkpoint saved: {progress}")
    
    # 5. 爬虫
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = f"nyaa_v5_missing_{ts}.jsonl"
    fh = open(output_file, "w", encoding="utf-8")
    
    processed_count = 0
    found_count = 0
    stop_flag = False
    q = asyncio.Queue()
    lock = asyncio.Lock()
    
    async def worker(session):
        nonlocal processed_count, found_count, stop_flag
        while True:
            if stop_flag:
                break
            id_val = await q.get()
            if id_val is None:
                q.task_done()
                break
            
            # 每个ID最多重试3次，超时/连接断开 → 标记timeout不阻塞
            data = None
            status_msg = "unknown"
            for attempt in range(3):
                _, data, status_msg = await fetch_one(id_val, session, min_delay, max_delay)
                if status_msg in ("ok", "404"):
                    break
                # 429 不重试（等下次再跑）
                if status_msg == "429":
                    break
                # 超时/连接断开 → 重试
                if ("timed out" in status_msg.lower()
                        or "connection closed" in status_msg.lower()
                        or "502" in status_msg
                        or "504" in status_msg):
                    if attempt < 2:
                        print(f" [v5] ID {id_val} attempt {attempt+1}: {status_msg[:40]} → retry")
                        await asyncio.sleep(2)
                    else:
                        status_msg = "timeout"
                else:
                    break  # 其他错误不重试
            
            async with lock:
                processed_count += 1
                if data:
                    found_count += 1
                    data["status"] = "ok"
                    fh.write(json.dumps(data, ensure_ascii=False) + "\n")
                else:
                    fh.write(json.dumps({"id": id_val, "status": status_msg}, ensure_ascii=False) + "\n")
                
                if processed_count % 50 == 0 or stop_flag:
                    fh.flush()
                    print(f" [v5][{processed_count}] id={id_val} found={found_count} status={status_msg}")
            q.task_done()
    
    session_kwargs = {"headers": H, "impersonate": "chrome110"}
    if proxy:
        session_kwargs["proxies"] = {"http": proxy, "https": proxy}
    
    try:
        async with AsyncSession(**session_kwargs) as session:
            workers_tasks = [asyncio.create_task(worker(session)) for _ in range(workers)]
            for i in pending_ids:
                q.put_nowait(i)
            for _ in range(workers):
                q.put_nowait(None)
            
            # 等待所有任务完成（直到队列为空）
            await q.join()
            
            await asyncio.gather(*workers_tasks, return_exceptions=True)
    finally:
        fh.close()
    
    print(f"[*] V5 Done. processed={processed_count} found={found_count} file={output_file}")
    return output_file

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--missing-dir",   type=str,   default=".")
    parser.add_argument("--workers",       type=int,   default=5)
    parser.add_argument("--batch-size",    type=int,   default=355)
    parser.add_argument("--batch-timeout", type=int,   default=540)
    parser.add_argument("--min-delay",     type=float, default=0.2)
    parser.add_argument("--max-delay",     type=float, default=0.6)
    parser.add_argument("--proxy",         type=str,   default=None)
    parser.add_argument("--progress-file", type=str,   default="v5_progress.json")
    args = parser.parse_args()
    
    asyncio.run(run_v5_missing_mode(
        args.missing_dir, args.workers, args.batch_size, args.batch_timeout,
        args.min_delay, args.max_delay, args.proxy, args.progress_file,
    ))
