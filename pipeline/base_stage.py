from abc import ABC, abstractmethod

class BaseStage(ABC):
    """
    管線中單一階段的抽象基礎類別。
    """
    
    @abstractmethod
    def execute(self, context: dict) -> dict:
        """
        執行此階段的邏輯。
        
        :param context: 一個字典，包含從先前階段傳遞的狀態或資料路徑。
        :return: 更新後的 context 字典，傳遞給下一階段。
        """
        pass