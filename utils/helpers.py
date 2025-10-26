import numpy as np
import pandas as pd
import ipaddress
import os

def dropna(nparray):
    """
    遞迴地移除 numpy 陣列中的 NaN 值。
    來自 3_5_clustering_kkmeans。
    """
    if isinstance(nparray[0], np.ndarray):
        return np.array([dropna(x) for x in nparray])
    else:
        return nparray[~np.isnan(nparray)]

def is_valid_ip(ip_str):
    """
    檢查字串是否為有效的 IP 位址 (IPv4 或 IPv6)。
    來自 3_8_statistics。
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

def ensure_dir_exists(filepath):
    """
    確保給定檔案的路徑目錄存在。
    """
    directory = os.path.dirname(filepath)
    if directory:
        os.makedirs(directory, exist_ok=True)