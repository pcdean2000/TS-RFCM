import numpy as np
import pandas as pd # 確保 Evaluation 讀取時需要 sample 列表
from tslearn.utils import from_pyts_dataset
from tslearn.clustering import KernelKMeans
from .base_clusterer import BaseClusterer
from utils.helpers import dropna, ensure_dir_exists

class KernelKMeansClusterer(BaseClusterer):
    """ Kernel K-Means 策略實現 """
    
    def __init__(self, **params):
        super().__init__(**params)
        self.model = KernelKMeans(**params)
        self.sample_list = [] # 用於最後存檔時對齊 IP

    def load_data(self, config):
        print("  KKMeans: Loading data (Host Level /32)...")
        
        # 使用新的動態路徑方法
        try:
            dataset_path = self.get_dataset_path(config, mask=32)
            sample_path = self.get_sample_path(config, mask=32)
            
            pyts_dataset = np.load(dataset_path)
            pyts_dataset = dropna(pyts_dataset)
            
            with open(sample_path) as f:
                self.sample_list = [line.strip() for line in f.readlines()]

            print(f"\tPyts dataset shape: {pyts_dataset.shape}")
            self.data = from_pyts_dataset(pyts_dataset)
            print(f"\tTslearn dataset shape: {self.data.shape}")
            
        except FileNotFoundError as e:
            print(f"  Error loading KKMeans data: {e}")
            raise

    def fit_predict(self):
        print("  KKMeans: Fitting model...")
        self.labels = self.model.fit_predict(self.data)

    def save_results(self, config):
        print("  KKMeans: Saving results...")
        target_file = config.MODEL_OUTPUT_PATHS["kkmeans"]
        ensure_dir_exists(target_file)
        
        # 儲存 .npy 標籤
        np.save(target_file, self.labels)
        print(f"\tSaved labels to {target_file}")

    def post_process(self, config):
        # Kernel K-Means 在原始腳本中沒有合併/排序步驟
        # 評估階段 (EvaluationStage) 會處理標籤重排序
        print("  KKMeans: No post-processing required.")
        pass