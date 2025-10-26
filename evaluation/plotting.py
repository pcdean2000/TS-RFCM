import matplotlib.pyplot as plt
from utils.helpers import ensure_dir_exists

class ROCPlotter:
    """
    繪製並儲存 ROC 曲線圖。
    """
    
    def plot(self, all_model_metrics: dict, save_path: str):
        """
        繪製 ROC 曲線。
        
        :param all_model_metrics: 一個字典，
               { "model_name": {"fpr": [], "tpr": [], "auc": 0.0, "name": "..."} }
        :param save_path: 儲存圖片的路徑
        """
        print(f"\nPlotting ROC curve to {save_path}...")
        
        plt.figure(figsize=(10, 8))
        
        for model_name, metrics in all_model_metrics.items():
            if not metrics: # 如果模型評估失敗
                continue
                
            label = f'{metrics["name"]} (AUC: {metrics["auc"]:.3f})'
            plt.step(metrics["fpr"], metrics["tpr"], label=label)

        plt.plot([0, 1], [0, 1], 'k--') # 對角線
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('ROC Curve Comparison')
        plt.legend()
        
        try:
            ensure_dir_exists(save_path)
            plt.savefig(save_path)
            print("ROC curve saved.")
        except Exception as e:
            print(f"Error saving ROC plot: {e}")
        
        # plt.show() # 在腳本中通常不 show()