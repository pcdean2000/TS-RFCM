import numpy as np
import pandas as pd
import json
import os
import ipaddress
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from .rfcm import RFCM
from .base_clusterer import BaseClusterer
from utils.helpers import dropna, ensure_dir_exists

class RFCMClusterer(BaseClusterer):
    """ 
    實作 Evidence Accumulation Clustering (EAC)。
    
    流程：
    1. 針對 4 種 Netmask (32, 24, 16, 8) 載入數據。
    2. 對每種 Mask，使用 [k-1, k, k+1] 三種群集數執行 RFCM。
    3. 總共產生 12 組分群結果 (Ensemble Members)。
    4. 將所有結果映射回 Host Level (/32)。
    5. 建立 Co-association Matrix 並執行 Hierarchical Clustering。
    """
    
    def __init__(self, **params):
        super().__init__(**params)
        # 這裡的 params 主要包含 n_clusters (作為 k 的基準)
        self.base_k = params.get('n_clusters', 10)
        self.rfcm_params = {k:v for k,v in params.items() if k != 'n_clusters'}
        
        self.ensemble_results = [] # 儲存每次跑的結果: (mask, k, label_dict)
        self.host_list = [] # 所有的 /32 IP 列表 (基準)

    def load_data(self, config):
        print("  EAC-RFCM: Preparing data paths for all masks...")
        self.config = config
        
        # 1. 載入 Host Level (/32) 的 sample list 作為基準
        ts_dir_prefix = config.TIMESERIES_DIR_PREFIX
        base_dir = config.TIMESERIES_FEATURE_DIR_BASE / ts_dir_prefix
        
        host_sample_file = base_dir / "mask_32" / "sample.txt"
        if not os.path.exists(host_sample_file):
            raise FileNotFoundError("Host level (mask 32) sample file not found. Run reformatting first.")
            
        with open(host_sample_file) as f:
            self.host_list = [line.strip() for line in f.readlines()]
            
        print(f"    Base Host List: {len(self.host_list)} IPs")

    def fit_predict(self):
        print("  EAC-RFCM: Starting Ensemble Loop (4 masks * 3 k)...")
        
        ts_dir_prefix = self.config.TIMESERIES_DIR_PREFIX
        base_dir = self.config.TIMESERIES_FEATURE_DIR_BASE / ts_dir_prefix
        
        # 1. 遍歷所有 Masks
        for mask in self.config.EAC_MASKS:
            # 載入該 Mask 的數據
            mask_dir = base_dir / f"mask_{mask}"
            pyts_path = mask_dir / "pyts_dataset.npy"
            sample_path = mask_dir / "sample.txt"
            
            if not os.path.exists(pyts_path):
                print(f"    Warning: Data for mask /{mask} not found, skipping.")
                continue
                
            # 載入該 Mask 的數據
            data = np.load(pyts_path)
            data = dropna(data) # 移除 NaN
            
            with open(sample_path) as f:
                samples = [line.strip() for line in f.readlines()]
            
            # 2. 遍歷 3 種 k 值 [k-1, k, k+1]
            k_values = [self.base_k - 1, self.base_k, self.base_k + 1]
            
            for k in k_values:
                if k < 2: continue
                
                print(f"\tRunning RFCM (Mask=/{mask}, K={k})...", end="\r")
                
                # 建立並訓練模型
                model = RFCM(n_clusters=k, **self.rfcm_params)
                try:
                    model.fit(data)
                    labels = model.labels_
                    
                    # 獲取距離 (用於計算 Outlier Score)
                    dists = model.distances_
                    
                    # 建立映射字典
                    label_dict = dict(zip(samples, labels))
                    dist_dict = dict(zip(samples, dists))
                    
                    self.ensemble_results.append({
                        "mask": mask,
                        "k": k,
                        "labels": label_dict,
                        "distances": dist_dict # 儲存距離
                    })
                except Exception as e:
                    print(f"\n\tError in RFCM run (Mask={mask}, K={k}): {e}")
                    import traceback
                    traceback.print_exc()
                    
        print(f"\n  EAC-RFCM: Ensemble loop complete. Generated {len(self.ensemble_results)} partitions.")

    def save_results(self, config):
        # 這裡我們暫存 Ensemble 的中間結果，以防萬一
        target_dir = config.MODEL_OUTPUT_PATHS["rfcm_eac_root"]
        ensure_dir_exists(target_dir / "dummy")
        
        # 儲存每次 run 的詳細結果
        for i, res in enumerate(self.ensemble_results):
            fname = target_dir / f"run_{i}_mask{res['mask']}_k{res['k']}.json"
            # converting numpy types to python types for json
            serializable_labels = {k: int(v) for k,v in res['labels'].items()}
            with open(fname, 'w') as f:
                json.dump(serializable_labels, f)

    def post_process(self, config):
        print("  EAC-RFCM: Post-processing (Evidence Accumulation)...")
        
        n_samples = len(self.host_list)
        co_matrix = np.zeros((n_samples, n_samples))
        
        # 初始化 Outlier Vector D(o)
        outlier_vector = np.zeros(n_samples)
        
        n_partitions = len(self.ensemble_results)
        
        if n_partitions == 0:
            print("    Error: No partitions generated.")
            return

        # 1. 建立 Co-association Matrix 
        # 矩陣很大，這步會比較慢，可以考慮用 Numba 加速，但這裡先用純 Python 邏輯確保正確性
        print("    Building Co-association Matrix and Accumulating Outlier Scores...")
        
        # 預先將每個 Partition 的結果映射回 Host List
        # mapped_partitions[run_idx][host_idx] = label
        mapped_partitions = np.zeros((n_partitions, n_samples), dtype=int)
        
        for run_idx, res in enumerate(self.ensemble_results):
            mask = res['mask']
            label_dict = res['labels']
            dist_dict = res['distances']
            
            for host_idx, host_ip in enumerate(self.host_list):
                # 找出這個 Host 在該 Mask 下屬於哪個 Key (IP or Subnet)
                if mask == 32:
                    key = host_ip
                else:
                    # 這裡必須與 FeatureEngineering 的邏輯一致 (使用 strict=False)
                    try:
                        net = ipaddress.ip_network(f"{host_ip}/{mask}", strict=False)
                        key = str(net).replace('/', '_') # 記得 feature_engineering 存檔名時有用 replace
                    except ValueError:
                        key = None
                
                # 查表得到 Label
                if key in label_dict:
                    mapped_partitions[run_idx][host_idx] = label_dict[key]
                    
                    # 累加 Outlier Score D(o)
                    # 如果一個 Subnet 離中心很遠，其內的所有 Host 都會繼承這個距離
                    outlier_vector[host_idx] += dist_dict[key]
                else:
                    mapped_partitions[run_idx][host_idx] = -1 # Missing data handling
        
        # 計算共現
        # Vectorized implementation for speed
        for i in range(n_samples):
            # 取出第 i 個 host 在所有 runs 的 labels
            vec_i = mapped_partitions[:, i]
            # 與所有其他 hosts 比較
            # matches: (n_partitions, n_samples) boolean matrix
            matches = (mapped_partitions == vec_i[:, None])
            # 排除掉 label 為 -1 (missing) 的情況 (如果需要更嚴謹的處理)
            valid = (vec_i[:, None] != -1) & (mapped_partitions != -1)
            
            # 加總 True 的次數
            co_matrix[i] = np.sum(matches & valid, axis=0)
            
        # 正規化 (變成 0~1 的相似度)
        co_matrix /= n_partitions
        
        # 2. Hierarchical Clustering
        print("    Running Hierarchical Clustering on Co-association Matrix...")
        # 轉為距離矩陣
        dist_matrix = 1.0 - co_matrix
        np.fill_diagonal(dist_matrix, 0) # 確保對角線為 0
        
        # 轉為 condensed distance matrix (required by linkage)
        condensed_dist = squareform(dist_matrix)
        
        # 使用 Average Linkage (UPGMA)
        Z = linkage(condensed_dist, method='average')
        
        # 取得最終分群結果
        final_labels = fcluster(Z, t=self.base_k, criterion='maxclust')
        
        # 3. 儲存最終結果 (相容 Evaluation 的格式)
        self.final_labels_ = final_labels
        self._save_final_csv(config, final_labels, outlier_vector)

    def _save_final_csv(self, config, labels, outlier_scores):
        # 現在我們使用的 D(o) 作為 'avg' 分數
        
        df = pd.DataFrame({
            'ip': self.host_list, 
            'label': labels,
            'avg': outlier_scores # 論文中的 D(o)
        })
        
        # 排序：D(o) 越大代表越異常
        df.sort_values('avg', ascending=False, inplace=True)
        
        target_csv = config.MODEL_OUTPUT_PATHS["rfcm_sorted"] # 覆蓋舊路徑以讓 eval 讀取
        # 為了防止 Evaluation 讀取 types.json 報錯，我們也造一個 dummy types
        types_json = config.MODEL_OUTPUT_PATHS["rfcm_types"]
        
        df.to_csv(target_csv)
        
        with open(types_json, 'w') as f:
            json.dump({'ip': 'object', 'label': 'int64', 'avg': 'float64'}, f)
            
        print(f"    Final EAC results saved to {target_csv}")
        print(f"    (Note: 'avg' column now represents the accumulated Outlier Score D(o))")