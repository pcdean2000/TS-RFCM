import os
from pathlib import Path
from datetime import datetime

# --- 基本路徑 ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"  # 假設所有原始資料都在 data/
NETFLOW_DIR = DATA_DIR / "netflow"
ZEEK_DIR = DATA_DIR / "zeek"

# --- 輸出路徑 (由腳本自動建立) ---
OUTPUT_DIR = BASE_DIR / "output"
ZEEK_CSV_DIR = OUTPUT_DIR / "zeek_csv"
FEATURE_DIR = OUTPUT_DIR / "src_feature"
RESULTS_DIR = OUTPUT_DIR / "results" # 存放 CSV 和圖片

# --- 時間序列參數 ---
INTERVAL = 30
TIMESERIES_MINUTES = 15
TIMESERIES_DIR_PREFIX = f"interval_{INTERVAL}_src_feature"
TIMESERIES_DIR = OUTPUT_DIR / TIMESERIES_DIR_PREFIX
TIMESERIES_FEATURE_DIR = OUTPUT_DIR / f"timeseries_feature/{TIMESERIES_DIR_PREFIX}"

# --- 預處理 (Preprocessing) ---
NETFLOW_SUMMARY_STRING = "Summary"
ZEEK_LOGS = {
    "conn": ZEEK_DIR / "conn.log",
    "analyzer": ZEEK_DIR / "analyzer.log",
    "dns": ZEEK_DIR / "dns.log",
    # "weird": ZEEK_DIR / "weird.log",
}
ZEEK_CSVS = {
    "conn": ZEEK_CSV_DIR / "conn.csv",
    "analyzer": ZEEK_CSV_DIR / "analyzer.csv",
    "dns": ZEEK_CSV_DIR / "dns.csv",
    # "weird": ZEEK_CSV_DIR / "weird.csv",
    "filtered_conn": ZEEK_CSV_DIR / "filtered_conn.csv",
}

# --- 重組 (Reformatting) ---
FEATURES = [
    "packets", "bytes", "flows", 
    "bytes/packets", "flows/(bytes/packets)", 
    "nDstIP", "nSrcPort", "nDstPort"
]
# 將斜線替換為底線，用於 RFCM 的檔案命名
FEATURES_RENAMED = [f.replace('/', '_') for f in FEATURES]

PYTS_DATASET_FILE = OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/pyts_dataset.npy"
TIMESERIES_DICT_FILE = OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/timeseries_dictionary.pickle"
SAMPLE_FILE = OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/pyts_dataset_sample.txt"
FEATURE_FILE = OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/pyts_dataset_feature.txt"

# --- 分群 (Clustering) ---
# 要執行的模型策略
CLUSTERING_MODELS = ["kkmeans", "ksom", "rfcm"]
N_CLUSTERS = 20
RANDOM_STATE = 10
N_JOBS = 2  # 共用
MAX_ITER = 10
EPSILON = 1e-3

# 各模型的特定參數
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
# 模型結果儲存路徑
MODEL_OUTPUT_PATHS = {
    "kkmeans": OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/tslearn_kmeans.npy",
    "ksom": OUTPUT_DIR / f"timeseries/{TIMESERIES_DIR_PREFIX}/ksom_label.npy",
    "rfcm": TIMESERIES_FEATURE_DIR, # RFCM 比較特殊
    "rfcm_merged": TIMESERIES_FEATURE_DIR / "rfcm_results.csv",
    "rfcm_sorted": TIMESERIES_FEATURE_DIR / "sorted_rfcm_results.csv",
    "rfcm_types": TIMESERIES_FEATURE_DIR / "types.json"
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