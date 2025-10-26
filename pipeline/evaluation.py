import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import json
from .base_stage import BaseStage
from evaluation.ground_truth import GroundTruthGenerator
from evaluation.metrics import MetricsCalculator
from evaluation.plotting import ROCPlotter

class EvaluationStage(BaseStage):
    """
    執行評估階段：
    1. 生成 Ground Truth IP 列表
    2. 載入三種模型的結果
    3. 計算每種模型的 ROC/AUC
    4. 繪製並儲存 ROC 比較圖
    """
    
    def __init__(self, config):
        self.config = config
        self.gt_generator = GroundTruthGenerator()
        self.metrics_calc = MetricsCalculator()
        self.plotter = ROCPlotter()
        self.ip_samples = []
        self.ground_truth_set = set()
        print("Initializing Evaluation Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Evaluation Stage...")
        
        # 1. 生成 Ground Truth
        ground_truth_list = self.gt_generator.generate(self.config)
        self.ground_truth_set = set(ground_truth_list)
        
        # 2. 載入 IP 範例 (所有模型共用)
        self.ip_samples = self._load_ip_samples()
        if not self.ip_samples:
            print("  Error: Cannot load IP sample list. Evaluation aborted.")
            return context
            
        all_metrics = {}

        # 3. 評估每個模型
        for model_name in self.config.CLUSTERING_MODELS:
            try:
                if model_name == "rfcm":
                    df_rfcm = self._load_rfcm_results()
                    metrics = self.metrics_calc.evaluate_rfcm(df_rfcm, self.ground_truth_set)
                    all_metrics[model_name] = metrics
                    
                elif model_name == "kkmeans":
                    df_kkmeans = self._load_kmeans_results()
                    metrics = self.metrics_calc.evaluate_cluster_model(df_kkmeans, self.ground_truth_set, "Kernel K-Means")
                    all_metrics[model_name] = metrics

                elif model_name == "ksom":
                    df_som = self._load_som_results()
                    metrics = self.metrics_calc.evaluate_cluster_model(df_som, self.ground_truth_set, "Kernel SOM")
                    all_metrics[model_name] = metrics
            except Exception as e:
                print(f"  Error evaluating model {model_name}: {e}")
                all_metrics[model_name] = None # 標記為失敗

        # 4. 繪製 ROC
        self.plotter.plot(all_metrics, self.config.ROC_PLOT_PATH)

        print("Evaluation Stage Complete.")
        context['evaluation_complete'] = True
        return context

    def _load_ip_samples(self) -> list:
        """ 載入 IP 列表 (來自 sample.txt) """
        try:
            with open(self.config.SAMPLE_FILE) as f:
                return f.read().splitlines()
        except FileNotFoundError:
            print(f"  Error: IP sample file not found at {self.config.SAMPLE_FILE}")
            return []

    def _load_rfcm_results(self) -> pd.DataFrame:
        """ 載入 RFCM 的 `sorted_rfcm_results.csv` """
        print("  Loading RFCM results...")
        csv_path = self.config.MODEL_OUTPUT_PATHS["rfcm_sorted"]
        types_path = self.config.MODEL_OUTPUT_PATHS["rfcm_types"]
        
        with open(types_path) as f:
            types = json.load(f)
            
        df = pd.read_csv(csv_path, index_col=0, dtype=types)
        
        # 原始腳本中，只選擇了 2-feature 組合
        renamed_features = self.config.FEATURES_RENAMED
        valid_cols = ["-".join(pair) for pair in combinations(renamed_features, 2)]
        
        # 過濾掉 'avg' (如果存在) 和其他無效欄位
        df = df[[col for col in valid_cols if col in df.columns]]
        return df

    def _load_kmeans_results(self) -> pd.DataFrame:
        """ 載入 KKMeans 的 .npy 標籤 """
        print("  Loading KKMeans results...")
        labels = np.load(self.config.MODEL_OUTPUT_PATHS["kkmeans"])
        return pd.DataFrame({'ip': self.ip_samples, 'label': labels})

    def _load_som_results(self) -> pd.DataFrame:
        """ 載入 KSOM 的 .npy 標籤 """
        print("  Loading KSOM results...")
        labels = np.load(self.config.MODEL_OUTPUT_PATHS["ksom"])
        # 原始腳本中，som_df 使用的是 kmeans_label，這看起來是一個 bug。
        # 我們修正它，使用 som_label
        # som_label = np.load(f'timeseries/interval_{INTERVAL}_src_feature/ksom_label.npy')
        # som_df = pd.DataFrame({'ip': ip, 'label': kmeans_label}) <-- Bug
        # 應該是:
        # som_df = pd.DataFrame({'ip': ip, 'label': som_label})
        return pd.DataFrame({'ip': self.ip_samples, 'label': labels})