import os
from pathlib import Path
from datetime import datetime

# --- 基本路徑 ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
NETFLOW_DIR = DATA_DIR / "netflow"
ZEEK_DIR = DATA_DIR / "zeek"

# --- 輸出路徑 ---
OUTPUT_DIR = BASE_DIR / "output"
ZEEK_CSV_DIR = OUTPUT_DIR / "zeek_csv"

# 特徵路徑現在只是 Base，實際路徑會動態加上 mask
FEATURE_DIR_BASE = OUTPUT_DIR / "src_feature"
RESULTS_DIR = OUTPUT_DIR / "results"

# --- 時間序列參數 ---
INTERVAL = 30
TIMESERIES_MINUTES = 15
TIMESERIES_DIR_PREFIX = f"interval_{INTERVAL}_src_feature"

# 支援多個 Netmask 粒度 (EAC 核心需求) [cite: 252]
# 32=Host, 24=Subnet, 16/8=Org
EAC_MASKS = [32, 24, 16, 8] 

# 這些路徑變成 Base，程式中會根據 mask 動態生成子目錄
TIMESERIES_DIR_BASE = OUTPUT_DIR / "timeseries"
TIMESERIES_FEATURE_DIR_BASE = OUTPUT_DIR / "timeseries_feature"

# --- 預處理 (Preprocessing) ---
NETFLOW_SUMMARY_STRING = "Summary"
ZEEK_LOGS = {
    "conn": ZEEK_DIR / "conn.log",
    "analyzer": ZEEK_DIR / "analyzer.log",
    "dns": ZEEK_DIR / "dns.log",
}
ZEEK_CSVS = {
    "conn": ZEEK_CSV_DIR / "conn.csv",
    "analyzer": ZEEK_CSV_DIR / "analyzer.csv",
    "dns": ZEEK_CSV_DIR / "dns.csv",
    "filtered_conn": ZEEK_CSV_DIR / "filtered_conn.csv",
}

# --- 重組 (Reformatting) ---
FEATURES = [
    "packets", "bytes", "flows", 
    "bytes/packets", "flows/(bytes/packets)", 
    "nDstIP", "nSrcPort", "nDstPort"
]
FEATURES_RENAMED = [f.replace('/', '_') for f in FEATURES]

# --- 分群 (Clustering) ---
CLUSTERING_MODELS = ["kkmeans", "ksom", "rfcm"]
N_CLUSTERS = 20 # 這是 Elbow 的中心值 k
RANDOM_STATE = 10
N_JOBS = 2
MAX_ITER = 10
EPSILON = 1e-3

MODEL_PARAMS = {
    "kkmeans": {
        "n_clusters": N_CLUSTERS,
        "verbose": True,
        "random_state": RANDOM_STATE,
        "n_jobs": N_JOBS,
        "max_iter": MAX_ITER,
        "tol": EPSILON,
        "kernel_params": {"sigma": 1}
    },
    "ksom": {
        "x": 10,
        "y": 10,
        "sigma": 0.3,
        "learning_rate": 0.1,
        "random_seed": RANDOM_STATE,
        "n_iter": 50000
    },
    "rfcm": {
        "n_clusters": N_CLUSTERS,
        "max_iter": MAX_ITER,
        "random_state": RANDOM_STATE,
        "n_jobs": N_JOBS,
        "epsilon": EPSILON
    }
}

# 模型結果儲存路徑 (RFCM 改為目錄，因為會有大量中間檔案)
MODEL_OUTPUT_PATHS = {
    "kkmeans": OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/tslearn_kmeans.npy",
    "ksom": OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/ksom_label.npy",
    "rfcm_eac_root": TIMESERIES_FEATURE_DIR_BASE / "rfcm_eac_results"
}

# --- 評估 (Evaluation) ---
GROUND_TRUTH_LOGS = {
    "notice": ZEEK_DIR / "notice.log",
    "conn": ZEEK_DIR / "conn.log",
    "weird": ZEEK_DIR / "weird.log"
}
MIN_CONN_THRESHOLD = 100
MIN_WEIRD_THRESHOLD = 50
ZEEK_ANOMALY_STATES = ['S0', 'REJ']
ZEEK_SRC_IP_COLS = {
    "notice": "src",
    "conn": "id.orig_h",
    "weird": "id.orig_h"
}

ROC_PLOT_PATH = RESULTS_DIR / f"roc_interval_{INTERVAL}_src.png"