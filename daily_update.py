#!/usr/bin/env python3
"""
每日更新：爬 nyaa.si 列表页 → 取最新ID → 对比本地 → 生成 missing → 跑爬虫
"""
import asyncio, json, re, random, argparse, os, sys
from pathlib import Path
from datetime import datetime, timezone
from curl_cffi.requests import AsyncSession

H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}

STATE_FILE = "crawler_state.json"

async def get_latest_id():
    """爬列表页第1行，取最新ID"""
    async with AsyncSession(headers=H, impersonate="chrome110") as s:
        resp = await s.get("https://sukebei.nyaa.si/", timeout=30)
        resp.raise_for_status()
        # 从 HTML 找第一个 /view/{id}
        m = re.search(r'/view/(\d+)', resp.text)
        if not m:
            print("[!] 无法从列表页解析ID")
            sys.exit(1)
        return int(m.group(1))

def load_state():
    try:
        if Path(STATE_FILE).exists():
            return json.load(open(STATE_FILE))
    except: pass
    return {"max_id": 0}

def save_state(max_id):
    json.dump({"max_id": max_id, "last_checked": datetime.now(timezone.utc).isoformat()},
              open(STATE_FILE, "w"), indent=2)

def main():
    # 1. 获取网站最新ID
    print("[*] 爬取 nyaa.si 列表页...")
    site_max = asyncio.run(get_latest_id())
    print(f"[*] 网站最新ID: {site_max}")
    
    # 2. 读取本地状态
    state = load_state()
    local_max = state.get("max_id", 0)
    print(f"[*] 本地最大ID: {local_max}")
    
    if site_max <= local_max:
        print("[*] 无新ID，跳过")
        save_state(site_max)
        return
    
    # 3. 生成缺失ID列表
    new_ids = list(range(local_max + 1, site_max + 1))
    print(f"[*] 新增 {len(new_ids)} 个ID: {local_max+1} ~ {site_max}")
    
    # 写入 missing 文件（支持多个文件，每批1100个）
    BATCH_SIZE = 1100
    for i in range(0, len(new_ids), BATCH_SIZE):
        chunk = new_ids[i:i+BATCH_SIZE]
        fname = f"missing_daily_{local_max+1}_{site_max}_part{i//BATCH_SIZE+1}.txt"
        with open(fname, "w") as f:
            for uid in chunk:
                f.write(str(uid) + "\n")
        print(f"  -> 生成 {fname} ({len(chunk)} 个ID)")
    
    # 4. 更新状态（先写，防丢失）
    save_state(site_max)
    print(f"[*] 状态已更新: max_id={site_max}")
    
    # 5. 输出 GITHUB_OUTPUT 供 workflow 使用
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"has_new=true\n")
            f.write(f"site_max={site_max}\n")
            f.write(f"new_count={len(new_ids)}\n")
    
    print(f"[*] 完成，可触发爬虫")

if __name__ == "__main__":
    main()
