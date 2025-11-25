import numpy as np
import pandas as pd
import os
import glob
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists, get_ip_network

class FeatureEngineeringStage(BaseStage):
    """
    支援 EAC 多粒度特徵工程：
    對每個設定的 Netmask (32, 24, 16, 8) 執行特徵聚合。
    """
    
    def __init__(self, config):
        self.config = config
        print("Initializing Feature Engineering Stage (EAC Enabled)...")

    def execute(self, context: dict) -> dict:
        print("Executing Feature Engineering Stage...")
        
        file_list = glob.glob(str(self.config.NETFLOW_DIR / '*_filtered.csv'))
        if not file_list:
            print(f"  Error: No '*_filtered.csv' files found.")
            return context

        # 1. 讀取並合併資料
        all_netflow_dfs = []
        for netflow_file in file_list:
            try:
                df = pd.read_csv(netflow_file, low_memory=False)
                all_netflow_dfs.append(df)
            except Exception as e:
                print(f"    Warning: Could not read {netflow_file}: {e}")
        
        if not all_netflow_dfs:
            return context

        netflow_combined = pd.concat(all_netflow_dfs, ignore_index=True)
        netflow_combined[["ts", "te"]] = netflow_combined[["ts", "te"]].astype(dtype='datetime64[ns]')

        global_min_ts = netflow_combined["ts"].min()
        context['timeseries_start'] = global_min_ts
        print(f"  Global minimum timestamp: {global_min_ts}")

        # 2. 針對每個 Mask 執行聚合
        for mask in self.config.EAC_MASKS:
            print(f"\n  --- Processing Netmask /{mask} ---")
            
            # 定義該 mask 的輸出目錄
            mask_feature_dir = self.config.FEATURE_DIR_BASE / f"mask_{mask}"
            
            # 計算 Group Key (單一 IP 或 Subnet)
            if mask == 32:
                netflow_combined['group_key'] = netflow_combined['sa']
            else:
                # 使用 helper 轉換
                netflow_combined['group_key'] = netflow_combined['sa'].apply(lambda x: get_ip_network(x, mask))

            # 移除無效 IP (轉換失敗的)
            valid_df = netflow_combined.dropna(subset=['group_key'])
            
            grouped = valid_df.groupby('group_key')
            count = 0
            
            for key, df in grouped:
                if ":" in str(key): # 暫時忽略 IPv6
                    continue
                
                # 檔名要轉義斜線，例如 192.168.1.0/24 -> 192.168.1.0_24.parquet
                safe_key = str(key).replace('/', '_')
                targetFile = mask_feature_dir / f"{safe_key}.parquet"
                
                if os.path.isfile(targetFile):
                    continue
                
                temp_data = self._process_ip_group(df)
                temp_df = pd.DataFrame(temp_data)
                
                if temp_df.empty:
                    continue
                    
                feature_df = self._calculate_features(temp_df)
                
                ensure_dir_exists(targetFile)
                feature_df.to_parquet(targetFile, index=False)
                count += 1
            
            print(f"  Finished Mask /{mask}: Generated {count} feature files.")

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
                duration = int(float(row.td)) + 1
            except (ValueError, TypeError):
                continue
                
            for t in [row.ts + pd.Timedelta(seconds=s) for s in range(duration)]:
                temp["ts"].append(t)
                temp["sa"].append(row.group_key) # 使用 group_key
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
        feature["srcIP"] = temp_df["sa"] # 這裡實際上存的是 IP 或 Subnet
        feature["packets"] = temp_df["ipkt"] + temp_df["opkt"]
        feature["bytes"] = temp_df["ibyt"] + temp_df["obyt"]
        feature["bytes/packets"] = (feature["bytes"] / feature["packets"]).fillna(0)
        feature["flows"] = temp_df["flows"]
        feature["flows/(bytes/packets)"] = (feature["flows"] / feature["bytes/packets"]).replace([np.inf, -np.inf], 0).fillna(0)
        feature["nDstIP"] = temp_df["nda"]
        feature["nSrcPort"] = temp_df["nsp"]
        feature["nDstPort"] = temp_df["ndp"]
        return feature