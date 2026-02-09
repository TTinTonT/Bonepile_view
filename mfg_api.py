"""
MFG Log API - Python client for search_log_items & get_node_info.
Base URL and token read from env: MFG_API_BASE_URL, MFG_API_TOKEN (no code change when switching server).
"""

import os
import requests
from typing import Optional, List, Dict, Any

BASE_URL = os.environ.get("MFG_API_BASE_URL", "http://10.16.138.68:8000").rstrip("/")
TOKEN = os.environ.get("MFG_API_TOKEN", "06939a6ac0ed828115deba6a6bed85de77c715bb")

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Accept": "application/json",
}


def search_log_items(
    cur_page: int = 1,
    project: str = "",
    station: str = "",
    phase: str = "",
    precondition: str = "",
    label_data: str = "",
    result: str = "All",
    spid: str = "",
    machine: str = "",
    pn: str = "",
    from_date: str = "",
    to_date: str = "",
    sfc: str = "",
    cal_total: bool = True,
    is_trial: bool = False,
    sn: str = "",
) -> Dict[str, Any]:
    """Search log items với đầy đủ filter."""
    params = {
        "cur_page": cur_page,
        "project": project,
        "station": station,
        "phase": phase,
        "precondition": precondition,
        "label_data": label_data,
        "result": result,
        "spid": spid,
        "machine": machine,
        "pn": pn,
        "from_date": from_date,
        "to_date": to_date,
        "sfc": sfc,
        "cal_total": str(cal_total).lower(),
        "is_trial": str(is_trial).lower(),
        "sn": sn,
    }
    r = requests.get(f"{BASE_URL}/api/search_log_items/", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def search_by_date(
    from_date: str,
    to_date: str,
    result: str = "All",
    project: str = "",
    cur_page: int = 1,
    cal_total: bool = True,
) -> Dict[str, Any]:
    """Search log items theo khoảng ngày. Date format: YYYY-MM-DD."""
    return search_log_items(
        from_date=from_date,
        to_date=to_date,
        result=result,
        project=project,
        cur_page=cur_page,
        cal_total=cal_total,
    )


def search_recent(limit: int = 200) -> List[Dict[str, Any]]:
    """Lấy N log mới nhất. Mặc định 200 (4 trang x 50 item)."""
    items = []
    page = 1
    while len(items) < limit:
        resp = search_log_items(cur_page=page, cal_total=(page == 1))
        log_list = resp.get("log_list", [])
        if not log_list:
            break
        items.extend(log_list)
        if len(items) >= limit:
            return items[:limit]
        if page >= resp.get("total_pages", page):
            break
        page += 1
    return items


def search_by_sn(
    sn: str,
    cur_page: int = 1,
    cal_total: bool = False,
) -> Dict[str, Any]:
    """Search log items theo Serial Number."""
    return search_log_items(sn=sn, cur_page=cur_page, cal_total=cal_total)


def get_node_info(
    node_log_id: Optional[int] = None,
    execute_log_id: Optional[int] = None,
    all_detail: bool = False,
    load_tcs: bool = False,
) -> Dict[str, Any]:
    """
    Lấy chi tiết node info.
    Dùng node_log_id HOẶC execute_log_id (exe_log_id từ search_log_items).
    """
    if not node_log_id and not execute_log_id:
        raise ValueError("Cần node_log_id hoặc execute_log_id")
    params = {
        "node_log_id": node_log_id or "",
        "execute_log_id": execute_log_id or "",
        "all_detail": str(all_detail).lower(),
        "load_tcs": str(load_tcs).lower(),
    }
    r = requests.get(f"{BASE_URL}/api/get_node_info/", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def get_pn_sn_from_node(node_info: Dict[str, Any]) -> tuple:
    """Trích PN và SN từ response get_node_info."""
    uut = node_info.get("uut_info", [])
    pn = sn = None
    for item in uut:
        if item.get("scan_code") == "PN":
            pn = item.get("scan_value")
        elif item.get("scan_code") in ("SN", "SCAN_SYSTEM_SN"):
            sn = item.get("scan_value")
        if pn and sn:
            break
    return (pn, sn)


def get_sn_pn_bonepile_from_node(node_info: Dict[str, Any]) -> tuple:
    """
    Trích SN, Partnumber, và Bonepile từ response get_node_info.
    Returns (sn, partnumber, is_bonepile).
    - SN từ scan_code SCAN_SYSTEM_SN
    - Partnumber từ scan_code PN
    - is_bonepile = True chỉ khi PBR_NUMBER có giá trị, không rỗng, không phải "NA", và có chứa "PB"
    """
    uut = node_info.get("uut_info", []) or []
    sn = partnumber = pbr_value = None
    for item in uut:
        code = (item.get("scan_code") or "").strip()
        val = (item.get("scan_value") or "").strip()
        if code == "PN":
            partnumber = val or None
        elif code in ("SN", "SCAN_SYSTEM_SN"):
            sn = val or None
        elif code == "PBR_NUMBER":
            pbr_value = val or None
    pbr = (pbr_value or "").strip()
    is_bonepile = (
        bool(pbr)
        and pbr.upper() != "NA"
        and "PB" in pbr.upper()
    )
    return (sn, partnumber or "", is_bonepile)


if __name__ == "__main__":
    pass
