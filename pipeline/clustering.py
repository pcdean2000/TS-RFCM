from .base_stage import BaseStage
from models.factory import ClustererFactory

class ClusteringStage(BaseStage):
    """
    執行分群階段：
    1. 從 config 讀取要運行的模型列表
    2. 使用 Factory 建立每個模型策略
    3. 依次執行每個策略 (load_data, fit_predict, save_results, post_process)
    """
    
    def __init__(self, config):
        self.config = config
        self.factory = ClustererFactory()
        self.models_to_run = config.CLUSTERING_MODELS
        print("Initializing Clustering Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Clustering Stage...")
        
        for model_name in self.models_to_run:
            print(f"\n--- Running Clustering Strategy: {model_name.upper()} ---")
            
            try:
                params = self.config.MODEL_PARAMS.get(model_name, {})
                clusterer = self.factory.create_clusterer(model_name, params)
                
                clusterer.load_data(self.config)
                clusterer.fit_predict()
                clusterer.save_results(self.config)
                clusterer.post_process(self.config)
                
                print(f"--- Finished {model_name.upper()} ---")
                
            except Exception as e:
                print(f"\n!!! Error running {model_name} strategy: {e}")
                # 選擇繼續執行下一個模型
                continue

        print("\nClustering Stage Complete.")
        context['clustering_complete'] = True
        return context