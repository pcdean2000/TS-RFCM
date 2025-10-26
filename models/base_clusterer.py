from abc import ABC, abstractmethod

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
        (可選) 執行任何模型特定的後處理，例如合併 (RFCM) 或排序。
        """
        pass