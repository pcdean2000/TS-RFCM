import numpy as np
from tslearn.utils import from_pyts_dataset
from tslearn.clustering import KernelKMeans
from .base_clusterer import BaseClusterer
from utils.helpers import dropna, ensure_dir_exists

class KernelKMeansClusterer(BaseClusterer):
    """ Kernel K-Means 策略實現 """
    
    def __init__(self, **params):
        super().__init__(**params)
        self.model = KernelKMeans(**params)

    def load_data(self, config):
        print("  KKMeans: Loading data...")
        pyts_dataset = np.load(config.PYTS_DATASET_FILE)
        pyts_dataset = dropna(pyts_dataset) # 使用 utils.helpers
        print(f"\tPyts dataset shape: {pyts_dataset.shape}")
        self.data = from_pyts_dataset(pyts_dataset)
        print(f"\tTslearn dataset shape: {self.data.shape}")

    def fit_predict(self):
        print("  KKMeans: Fitting model...")
        self.labels = self.model.fit_predict(self.data)

    def save_results(self, config):
        print("  KKMeans: Saving results...")
        target_file = config.MODEL_OUTPUT_PATHS["kkmeans"]
        ensure_dir_exists(target_file)
        np.save(target_file, self.labels)
        print(f"\tTslearn kmeans shape: {self.labels.shape}")

    def post_process(self, config):
        # Kernel K-Means 在原始腳本中沒有合併/排序步驟
        # 評估階段 (EvaluationStage) 會處理標籤重排序
        print("  KKMeans: No post-processing required.")
        pass