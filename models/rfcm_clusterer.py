import numpy as np
import pandas as pd
import json
import os
from glob import glob
from itertools import combinations
from .rfcm import RFCM
from .base_clusterer import BaseClusterer
from utils.helpers import dropna, ensure_dir_exists

class RFCMClusterer(BaseClusterer):
    """ 
    RFCM 策略實現。
    這個策略比較複雜，它需要：
    1. 在多個 2-feature 組合上分別運行
    2. 合併 (3_6) 結果
    3. 排序 (3_7) 結果
    """
    
    def __init__(self, **params):
        super().__init__(**params)
        self.model = RFCM(**params)
        self.feature_pair_paths = []

    def load_data(self, config):
        print("  RFCM: Finding 2-feature combination datasets...")
        
        # 根據 config.FEATURES_RENAMED 生成預期的目錄名稱
        renamed_features = config.FEATURES_RENAMED
        for f1, f2 in combinations(renamed_features, 2):
            pair_dir = config.TIMESERIES_FEATURE_DIR / f"{f1}-{f2}"
            if os.path.isdir(pair_dir):
                self.feature_pair_paths.append(pair_dir)
        
        print(f"    Found {len(self.feature_pair_paths)} feature-pair directories.")
        if not self.feature_pair_paths:
            raise FileNotFoundError("No feature-pair datasets found for RFCM.")

    def fit_predict(self):
        print("  RFCM: Fitting models on feature pairs...")
        # RFCM 沒有 'labels' 屬性，我們這裡不儲存 labels
        
        for pair_dir in self.feature_pair_paths:
            print(f"\tProcessing {pair_dir.name}", " " * 20, end="\r")
            
            target_file = pair_dir / "rfcm_label.npy"
            if os.path.exists(target_file):
                continue
                
            try:
                pyts_dataset = np.load(pair_dir / "pyts_dataset.npy")
                pyts_dataset = dropna(pyts_dataset)
                
                model = RFCM(**self.params) # 每次都建立新模型
                model.fit(pyts_dataset)
                y_pred = model.labels_
                
                np.save(target_file, y_pred)
            except FileNotFoundError:
                print(f"\n\tWarning: pyts_dataset.npy not found in {pair_dir.name}, skipping.")
            except Exception as e:
                print(f"\n\tError processing {pair_dir.name}: {e}")
        print("\n  RFCM: Model fitting complete.")

    def save_results(self, config):
        # fit_predict 已經在迴圈中儲存了結果
        print("  RFCM: Individual results saved during fit_predict.")
        pass

    def post_process(self, config):
        print("  RFCM: Post-processing (merging and sorting)...")
        self._merge_results(config)
        self._sort_labels(config)
        print("  RFCM: Post-processing complete.")

    def _merge_results(self, config):
        """ 來自 3_6_merge_rfcm_results """
        print("    Merging RFCM results...")
        target_csv = config.MODEL_OUTPUT_PATHS["rfcm_merged"]
        types_json = config.MODEL_OUTPUT_PATHS["rfcm_types"]
        
        # 讀取 IP 範例列表
        try:
            with open(config.SAMPLE_FILE) as f:
                ip_list = [line.strip() for line in f.readlines()]
        except FileNotFoundError:
            print(f"    Error: Cannot merge RFCM results. Sample file not found: {config.SAMPLE_FILE}")
            return

        results = []
        for pair_dir in self.feature_pair_paths:
            label_file = pair_dir / "rfcm_label.npy"
            try:
                label = np.load(label_file)
                df = pd.DataFrame({"ip": ip_list, pair_dir.name: label})
                df.set_index("ip", inplace=True)
                results.append(df)
            except FileNotFoundError:
                continue
            
        if not results:
            print("    Warning: No RFCM label files found to merge.")
            return

        result_df = pd.concat(results, axis=1)
        result_df.to_csv(target_csv)
        
        # 儲存 types.json
        res = result_df.dtypes.to_frame('dtypes').reset_index()
        d = res.set_index('index')['dtypes'].astype(str).to_dict()
        with open(types_json, 'w') as f:
            json.dump(d, f)
        print(f"    Merged results saved to {target_csv}")

    def _sort_labels(self, config):
        """ 來自 3_7_sort_labels """
        print("    Sorting RFCM labels...")
        source_csv = config.MODEL_OUTPUT_PATHS["rfcm_merged"]
        target_csv = config.MODEL_OUTPUT_PATHS["rfcm_sorted"]
        types_json = config.MODEL_OUTPUT_PATHS["rfcm_types"]
        
        if not os.path.exists(source_csv) or not os.path.exists(types_json):
            print(f"    Warning: Cannot sort. Missing {source_csv} or {types_json}")
            return
            
        with open(types_json) as f:
            types = json.load(f)
        
        df = pd.read_csv(source_csv, index_col=0, dtype=types)
        
        for col in df.columns:
            # 根據標籤出現次數重新排序
            replace_map = {key: value for value, key in enumerate(df[col].value_counts().index)}
            df[col].replace(replace_map, inplace=True)
        
        df.to_csv(target_csv)
        print(f"    Sorted results saved to {target_csv}")