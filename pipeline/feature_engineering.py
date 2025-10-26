import numpy as np
import pandas as pd
import os
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists

class FeatureEngineeringStage(BaseStage):
    """
    執行特徵工程：
    1. 讀取過濾後的 Netflow (目前硬編碼為 config.PROCESSING_NETFLOW_FILE)
    2. 按來源 IP 分組
    3. 將流量特徵分攤到每一秒
    4. 計算新特徵
    5. 儲存每個 IP 的 Parquet 檔案
    """
    
    def __init__(self, config):
        self.config = config
        print("Initializing Feature Engineering Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Feature Engineering Stage...")
        
        # 原始腳本只處理一個特定檔案，我們遵循該邏輯
        # 理想情況下，這應該處理所有 `_filtered.csv` 檔案
        netflow_file = str(self.config.PROCESSING_NETFLOW_FILE).replace(".csv", "_filtered.csv")
        
        if not os.path.exists(netflow_file):
            print(f"  Error: Filtered netflow file not found: {netflow_file}")
            print("  Please ensure preprocessing ran correctly.")
            return context

        print(f"  Processing file: {netflow_file}")
        netflow_filtered = pd.read_csv(netflow_file)
        netflow_filtered[["ts", "te"]] = netflow_filtered[["ts", "te"]].astype(dtype='datetime64[ns]')

        grouped = netflow_filtered.groupby('sa')

        for key, df in grouped:
            print(f"  Processing IP: {key}", " " * 50, end="\r")
            if ":" in key:  # 忽略 IPv6
                continue
            
            targetFile = self.config.FEATURE_DIR / f"{key}.parquet"
            if os.path.isfile(targetFile):
                continue
            
            temp_data = self._process_ip_group(df)
            temp_df = pd.DataFrame(temp_data)
            
            if temp_df.empty:
                continue
                
            feature_df = self._calculate_features(temp_df)
            
            ensure_dir_exists(targetFile)
            feature_df.to_parquet(targetFile, index=False)
            
        print("\nFeature Engineering Stage Complete.")
        context['feature_engineering_complete'] = True
        return context

    def _process_ip_group(self, df: pd.DataFrame) -> dict:
        """ 處理單一 IP 的所有 netflow 記錄 """
        temp = {
            "ts": [], "sa": [], "ipkt": [], "ibyt": [],
            "opkt": [], "obyt": [], "flows": [],
            "nda": [], "nsp": [], "ndp": [],
        }
        for row in df.itertuples():
            try:
                # 確保 td 是有效的數值
                duration = int(float(row.td)) + 1
            except ValueError:
                continue # 跳過無效 td 的行
                
            for t in [row.ts + pd.Timedelta(seconds=s) for s in range(duration)]:
                temp["ts"].append(t)
                temp["sa"].append(row.sa)
                temp["ipkt"].append(row.ipkt / duration)
                temp["ibyt"].append(row.ibyt / duration)
                temp["opkt"].append(row.opkt / duration)
                temp["obyt"].append(row.obyt / duration)
                temp["flows"].append(1)
                temp["nda"].append(1)
                temp["nsp"].append(1)
                temp["ndp"].append(1)
        return temp

    def _calculate_features(self, temp_df: pd.DataFrame) -> pd.DataFrame:
        """ 從分攤的數據計算最終特徵 """
        feature = pd.DataFrame()
        feature["timeStart"] = temp_df["ts"]
        feature["srcIP"] = temp_df["sa"]
        feature["packets"] = temp_df["ipkt"] + temp_df["opkt"]
        feature["bytes"] = temp_df["ibyt"] + temp_df["obyt"]
        
        # 處理除以零的情況
        feature["bytes/packets"] = (feature["bytes"] / feature["packets"]).fillna(0)
        feature["flows"] = temp_df["flows"]
        
        feature["flows/(bytes/packets)"] = (feature["flows"] / feature["bytes/packets"])
        # 處理 inf (當 bytes/packets 為 0 時)
        feature["flows/(bytes/packets)"] = feature["flows/(bytes/packets)"].replace([np.inf, -np.inf], 0).fillna(0)

        feature["nDstIP"] = temp_df["nda"]
        feature["nSrcPort"] = temp_df["nsp"]
        feature["nDstPort"] = temp_df["ndp"]
        return feature