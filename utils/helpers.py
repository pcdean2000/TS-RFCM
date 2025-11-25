import numpy as np
import pandas as pd
import ipaddress
import os

def dropna(nparray):
    """
    遞迴地移除 numpy 陣列中的 NaN 值。
    """
    if isinstance(nparray[0], np.ndarray):
        return np.array([dropna(x) for x in nparray])
    else:
        return nparray[~np.isnan(nparray)]

def is_valid_ip(ip_str):
    """
    檢查字串是否為有效的 IP 位址 (IPv4 或 IPv6)。
    """
    if not isinstance(ip_str, str):
        return False
    if pd.isna(ip_str):
        return False
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False

def get_ip_network(ip_str, prefix_len):
    """
    根據 prefix_len (例如 24) 回傳子網段字串 (例如 '192.168.1.0/24')
    如果 ip 無效，回傳 None
    """
    try:
        if prefix_len == 32:
            return ip_str
        
        # 使用 strict=False 允許 host bits 不為 0
        net = ipaddress.ip_network(f"{ip_str}/{prefix_len}", strict=False)
        return str(net)
    except ValueError:
        return None

def ensure_dir_exists(filepath):
    """
    確保給定檔案的路徑目錄存在。
    """
    directory = os.path.dirname(filepath)
    if directory:
        os.makedirs(directory, exist_ok=True)