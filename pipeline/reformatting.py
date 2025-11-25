import warnings
warnings.filterwarnings('ignore')
import os
import pickle
import numpy as np
import pandas as pd
from glob import glob
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists

class DataReformattingStage(BaseStage):
    """
    針對每個 Mask 生成 dataset。
    注意：EAC 的核心 RFCM 通常跑在全特徵上，但為了相容舊邏輯，
    我們主要生成全特徵的 pyts_dataset.npy 給 RFCM 讀取。
    """

    def __init__(self, config):
        self.config = config
        print("Initializing Data Reformatting Stage (EAC)...")

    def execute(self, context: dict) -> dict:
        print("Executing Data Reformatting Stage...")
        
        ts_dir_prefix = self.config.TIMESERIES_DIR_PREFIX
        base_ts_dir = self.config.TIMESERIES_DIR_BASE / ts_dir_prefix
        
        # 遍歷每個 Mask
        for mask in self.config.EAC_MASKS:
            mask_ts_dir = base_ts_dir / f"mask_{mask}"
            
            # 輸出目錄：output/timeseries_feature/.../mask_XX/
            target_base_dir = self.config.TIMESERIES_FEATURE_DIR_BASE / ts_dir_prefix / f"mask_{mask}"
            
            pyts_file = target_base_dir / "pyts_dataset.npy"
            dict_file = target_base_dir / "timeseries_dictionary.pickle"
            sample_file = target_base_dir / "sample.txt"
            
            if os.path.exists(pyts_file):
                print(f"  Mask /{mask}: Dataset already exists.")
                continue

            print(f"  Mask /{mask}: Building dataset...")
            timeseries = self._build_timeseries_dict(mask_ts_dir)
            
            if not timeseries:
                print(f"    Warning: No data for mask /{mask}")
                continue

            self._save_dataset(timeseries, pyts_file, dict_file, sample_file)

        context['reformatting_complete'] = True
        return context

    def _build_timeseries_dict(self, source_dir) -> dict:
        timeseries = {}
        source_files = glob(str(source_dir / '*.parquet'))
        
        if not source_files:
            return {}

        # 讀取所有 Feature
        for feature in self.config.FEATURES:
            result = {}
            for filename in source_files:
                try:
                    # key 是檔名 (IP 或 Subnet)
                    key = os.path.splitext(os.path.basename(filename))[0]
                    # 還原 safe_key (把底線換回斜線，如果需要的話，但這裡保持 safe_key 比較好處理檔名)
                    
                    value = pd.read_parquet(filename, columns=[feature])[feature].to_list()
                    result[key] = value
                except Exception:
                    pass
            timeseries[feature] = result
        return timeseries

    def _save_dataset(self, timeseries: dict, pyts_path, dict_path, sample_path):
        ensure_dir_exists(pyts_path)
        
        # 轉換為 (Samples, Features, TimeSteps)
        # 注意：需確保所有 sample 順序一致，這由 DataFrame.from_dict 處理
        try:
            # 使用第一個 feature 的 keys 作為基準
            ref_feature = self.config.FEATURES[0]
            sorted_keys = sorted(list(timeseries[ref_feature].keys()))
            
            # 構建 3D array
            # shape: (n_samples, n_features, n_timesteps)
            data_list = []
            for key in sorted_keys:
                sample_features = []
                for feat in self.config.FEATURES:
                    sample_features.append(timeseries[feat][key])
                data_list.append(sample_features)
            
            pyts_dataset = np.array(data_list)
            
            np.save(pyts_path, pyts_dataset)

            with open(dict_path, 'wb') as f:
                pickle.dump(timeseries, f)

            with open(sample_path, "w") as f:
                f.write('\n'.join(sorted_keys))
                
            print(f"    Saved shape: {pyts_dataset.shape}")
            
        except Exception as e:
            print(f"    Error saving dataset: {e}")