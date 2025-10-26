import warnings
warnings.filterwarnings('ignore')

import os
import pickle
import numpy as np
import pandas as pd
from glob import glob
from itertools import combinations
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists

class DataReformattingStage(BaseStage):
    """
    執行資料重組：
    1. 將所有 IP 的時間序列彙總為 pyts_dataset.npy
    2. 儲存輔助檔案 (pickle, sample.txt, feature.txt)
    3. 建立並儲存特徵的兩兩組合 (為 RFCM 準備)
    """

    def __init__(self, config):
        self.config = config
        print("Initializing Data Reformatting Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Data Reformatting Stage...")
        
        # 載入或建立主時間序列字典
        if os.path.exists(self.config.TIMESERIES_DICT_FILE):
            print(f"  Loading existing timeseries dictionary from {self.config.TIMESERIES_DICT_FILE}")
            with open(self.config.TIMESERIES_DICT_FILE, 'rb') as f:
                timeseries = pickle.load(f)
        else:
            print("  Building timeseries dictionary from parquet files...")
            timeseries = self._build_timeseries_dict()
            
            if not timeseries:
                print("  Error: Timeseries dictionary is empty. Cannot proceed.")
                return context

            self._save_main_dataset(timeseries)

        # 建立 2-feature 組合
        print("  Creating 2-feature combination datasets...")
        self._create_feature_combinations(timeseries)
        
        print("Data Reformatting Stage Complete.")
        context['reformatting_complete'] = True
        return context

    def _build_timeseries_dict(self) -> dict:
        """ 從 Parquet 檔案建立 {feature: {ip: [values]}} 字典 """
        timeseries = {}
        source_files = glob(str(self.config.TIMESERIES_DIR / '*.parquet'))
        if not source_files:
            print(f"  Warning: No timeseries parquet files found in {self.config.TIMESERIES_DIR}")
            return {}

        for feature in self.config.FEATURES:
            print(f"\tFeature: {feature}")
            result = {}
            for filename in source_files:
                print(f"\t\tFile: {filename}", " " * 5, end="\r")
                try:
                    value = pd.read_parquet(filename, columns=[feature])[feature].to_list()
                    column = os.path.splitext(os.path.basename(filename))[0]
                    result[column] = value
                except Exception as e:
                    print(f"\n\t\tError reading {filename} for feature {feature}: {e}")
            print()
            timeseries[feature] = result
        return timeseries

    def _save_main_dataset(self, timeseries: dict):
        """ 儲存主要的 pyts 數據集和輔助檔案 """
        print(f"  Saving main dataset to {self.config.PYTS_DATASET_FILE}")
        ensure_dir_exists(self.config.PYTS_DATASET_FILE)
        
        pyts_dataset = np.array(np.array(pd.DataFrame.from_dict(timeseries)).tolist())
        np.save(self.config.PYTS_DATASET_FILE, pyts_dataset)

        with open(self.config.TIMESERIES_DICT_FILE, 'wb') as f:
            pickle.dump(timeseries, f)

        with open(self.config.SAMPLE_FILE, "w") as f:
            f.write('\n'.join(list(timeseries[self.config.FEATURES[0]].keys())))
            
        with open(self.config.FEATURE_FILE, "w") as f:
            f.write('\n'.join(self.config.FEATURES))

    def _create_feature_combinations(self, timeseries: dict):
        """ 建立並儲存 2-feature 組合 (for RFCM) """
        targetDirname = self.config.TIMESERIES_FEATURE_DIR

        for feature_pair in combinations(self.config.FEATURES, 2):
            print(f"\tFeature combination: {feature_pair}")
            
            # 使用 config.FEATURES_RENAMED 中的名稱來建立目錄
            f1_renamed = feature_pair[0].replace('/', '_')
            f2_renamed = feature_pair[1].replace('/', '_')
            dirname = targetDirname / f"{f1_renamed}-{f2_renamed}"
            
            targetFilename = dirname / "pyts_dataset.npy"
            featureFilename = dirname / "pyts_dataset_feature.txt"

            if os.path.exists(featureFilename):
                continue
            
            partial_timeseries = {key: value for key, value in timeseries.items() if key in feature_pair}
            
            ensure_dir_exists(targetFilename)
            pyts_dataset = np.array(np.array(pd.DataFrame.from_dict(partial_timeseries)).tolist())
            np.save(targetFilename, pyts_dataset)
            
            with open(featureFilename, "w") as f:
                f.write('\n'.join(feature_pair))