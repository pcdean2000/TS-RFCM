import numpy as np
import pandas as pd
from sklearn.metrics import auc

class MetricsCalculator:
    """
    計算評估指標，包括 ROC/AUC。
    """

    def _normalize_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根據標籤 (label) 的出現次數重新排序，分數越高越可疑。
        """
        replace_map = {key: value for value, key in enumerate(df["label"].value_counts().index)}
        df["label"] = df["label"].replace(replace_map)
        df.sort_values(by="label", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_roc_points(self, df: pd.DataFrame, score_column: str, ground_truth_set: set):
        """
        計算 ROC 曲線的點。
        """
        
        ips_in_df = set(df['ip'])
        anomalies_in_df = ips_in_df.intersection(ground_truth_set)
        
        P = len(anomalies_in_df)
        N = len(df) - len(anomalies_in_df)
        
        if P == 0:
            print("Warning: No positive samples (ground truth) found in dataset. AUC will be 0.")
            return [0, 1], [0, 1], 0.0

        # 獲取所有獨特的閾值
        thresholds = df[df.ip.isin(anomalies_in_df)][score_column].unique().tolist()
        thresholds = sorted(list(set(thresholds)), reverse=True)

        TPRs = []
        FPRs = []

        for theta in thresholds:
            # 預測為正 (分數 > 閾值)
            # 原始腳本的 > 和 <= 邏輯有點反，我們這裡統一
            # 分數越高 = 越可疑 (label 0 是最可疑的)
            # 我們的 normalize 讓 label 越大越不可疑，所以用 <
            # 為了統一起見，讓分數越高越可疑
            # 讓我們重新正規化：
            # replace_map = {key: i for i, key in enumerate(df["label"].value_counts().index)}
            # df['score'] = df['label'].map(replace_map)
            # 這樣 'score' 0 是最常見 (最不可疑)，分數越高越稀有 (越可疑)
            
            # 原始碼是 ascending=False，所以 label 0 是最可疑的。
            # 閾值： [19, 18, 17...]
            # theta = 19 -> 預測 label > 19 (沒有)
            # theta = 0 -> 預測 label > 0 (1-19)
            # 這看起來是 'label' (分數) 越 *小* 越可疑
            # 我們將 'score' 反轉，使其越高越可疑
            
            max_label = df[score_column].max()
            df['score'] = max_label - df[score_column]
            theta = max_label - theta # 反轉閾值

            TP = len(df[df['score'] >= theta][df.ip.isin(anomalies_in_df)])
            FP = len(df[df['score'] >= theta][~df.ip.isin(anomalies_in_df)])
            
            TPRs.append(TP / P)
            FPRs.append(FP / N)

        # 加上 (0,0) 和 (1,1) 點
        TPRs = [1] + TPRs + [0]
        FPRs = [1] + FPRs + [0]

        roc_auc = auc(FPRs, TPRs)
        return FPRs, TPRs, roc_auc

    def evaluate_rfcm(self, df_rfcm: pd.DataFrame, ground_truth_set: set) -> dict:
        """ 評估 RFCM (使用 'avg' 分數) """
        print("  Calculating metrics for RFCM...")
        
        # 原始腳本使用 'avg'，且是 ascending=False，所以 avg 越高越可疑
        df_rfcm["avg"] = df_rfcm.mean(axis=1)
        df_rfcm.sort_values(by="avg", ascending=False, inplace=True)
        df_rfcm.reset_index(inplace=True)
        
        fpr, tpr, roc_auc = self._get_roc_points(df_rfcm, "avg", ground_truth_set)
        print(f"    RFCM AUC: {roc_auc:.4f}")
        return {"fpr": fpr, "tpr": tpr, "auc": roc_auc, "name": "Proposed Method (RFCM)"}


    def evaluate_cluster_model(self, df_model: pd.DataFrame, ground_truth_set: set, name: str) -> dict:
        """ 評估 KKMeans 或 KSOM (使用 'label' 分數) """
        print(f"  Calculating metrics for {name}...")
        
        # 正規化標籤，使 0 成為最可疑 (最稀有)
        df_normalized = self._normalize_labels(df_model)
        
        # 'label' 越小越可疑
        fpr, tpr, roc_auc = self._get_roc_points(df_normalized, "label", ground_truth_set)
        print(f"    {name} AUC: {roc_auc:.4f}")
        return {"fpr": fpr, "tpr": tpr, "auc": roc_auc, "name": name}