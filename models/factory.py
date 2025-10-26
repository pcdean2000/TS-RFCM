from .kkmeans_clusterer import KernelKMeansClusterer
from .ksom_clusterer import KernelSOMClusterer
from .rfcm_clusterer import RFCMClusterer

class ClustererFactory:
    """
    工廠模式，用於根據名稱實例化分群策略。
    """
    
    _creators = {
        "kkmeans": KernelKMeansClusterer,
        "ksom": KernelSOMClusterer,
        "rfcm": RFCMClusterer
    }

    def create_clusterer(self, model_name: str, params: dict) -> "BaseClusterer":
        """
        建立一個分群器實例。
        
        :param model_name: 要建立的模型名稱 (例如 "kkmeans")
        :param params: 傳遞給模型 __init__ 的參數字典
        :return: BaseClusterer 的一個實例
        """
        if model_name not in self._creators:
            raise ValueError(f"Unknown model strategy: {model_name}")
            
        return self._creators[model_name](**params)