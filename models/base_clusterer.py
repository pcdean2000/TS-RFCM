from abc import ABC, abstractmethod
import os
from pathlib import Path

class BaseClusterer(ABC):
    """
    分群策略的抽象基礎類別 (Strategy Pattern)。
    """
    
    def __init__(self, **params):
        """
        :param params: 一個字典，包含模型的特定超參數。
        """
        self.model = None
        self.params = params
        self.data = None
        self.labels = None

    @abstractmethod
    def load_data(self, config):
        """
        從 config 中定義的路徑載入此模型所需的特定資料。
        """
        pass

    @abstractmethod
    def fit_predict(self):
        """
        在載入的資料上擬合模型並預測標籤。
        """
        pass
        
    @abstractmethod
    def save_results(self, config):
        """
        將預測的標籤儲存到 config 中定義的路徑。
        """
        pass
        
    @abstractmethod
    def post_process(self, config):
        """
        執行任何模型特定的後處理，例如合併 (RFCM) 或排序。
        """
        pass

    def get_dataset_path(self, config, mask=32) -> Path:
        """
        統一獲取特徵資料路徑的方法。
        未來的模型可以直接呼叫此方法，不用硬編碼路徑。
        
        :param config: 設定檔物件
        :param mask: 資料粒度 (預設為 32，即 Host Level)
        :return: 指向 pyts_dataset.npy 的 Path 物件
        """
        ts_dir_prefix = config.TIMESERIES_DIR_PREFIX
        # 路徑結構: output/timeseries_feature/{prefix}/mask_{mask}/pyts_dataset.npy
        base_dir = config.TIMESERIES_FEATURE_DIR_BASE / ts_dir_prefix / f"mask_{mask}"
        target_file = base_dir / "pyts_dataset.npy"
        
        if not target_file.exists():
            raise FileNotFoundError(
                f"Dataset not found for mask /{mask} at {target_file}. "
                "Please check if ReformattingStage generated it correctly."
            )
            
        return target_file

    def get_sample_path(self, config, mask=32) -> Path:
        """ 獲取對應的 IP 列表檔案 """
        ts_dir_prefix = config.TIMESERIES_DIR_PREFIX
        base_dir = config.TIMESERIES_FEATURE_DIR_BASE / ts_dir_prefix / f"mask_{mask}"
        return base_dir / "sample.txt"