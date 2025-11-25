import numpy as np
from minisom import MiniSom
from .base_clusterer import BaseClusterer
from utils.helpers import dropna, ensure_dir_exists

class KernelSOMClusterer(BaseClusterer):
    """ Kernel SOM (MiniSom) 策略實現 """
    
    def __init__(self, **params):
        # MiniSom 的參數名稱與我們的 config 不同，需要映射
        self.n_iter = params.pop('n_iter', 50000)
        som_x = params.pop('x', 10)
        som_y = params.pop('y', 10)
        
        super().__init__(**params)
        self.som_shape = (som_x, som_y)
        # MiniSom 不是在 __init__ 時傳入所有參數
        
    def load_data(self, config):
        print("  KSOM: Loading data (Host Level /32)...")
        
        # 使用新的動態路徑方法
        try:
            dataset_path = self.get_dataset_path(config, mask=32)
            # KSOM 一般不需要 sample list，除非存檔需要，這裡僅載入數據
            
            pyts_dataset = np.load(dataset_path)
            pyts_dataset = dropna(pyts_dataset)
            print(f"\tPyts dataset shape: {pyts_dataset.shape}")
            
            # KSOM (MiniSom) 需要 2D 數據 (n_samples, n_features)
            self.data = pyts_dataset.reshape(len(pyts_dataset), -1)
            print(f"\tX shape: {self.data.shape}")
            
        except FileNotFoundError as e:
            print(f"  Error loading KSOM data: {e}")
            raise

    def fit_predict(self):
        print("  KSOM: Fitting model...")
        input_len = self.data.shape[1]

        # 在這裡實例化 MiniSom
        self.model = MiniSom(
            self.som_shape[0], 
            self.som_shape[1], 
            input_len, 
            sigma=self.params.get('sigma', 0.3),
            learning_rate=self.params.get('learning_rate', 0.1),
            random_seed=self.params.get('random_seed', 10)
        )
        
        self.model.random_weights_init(self.data)
        self.model.train(self.data, self.n_iter, verbose=True)
        
        # MiniSom 沒有 fit_predict，需要手動計算 winner
        self.labels = np.array([self.model.winner(x)[1] for x in self.data])

    def save_results(self, config):
        print("  KSOM: Saving results...")
        target_file = config.MODEL_OUTPUT_PATHS["ksom"]
        ensure_dir_exists(target_file)
        np.save(target_file, self.labels)
        print(f"\tSaved labels to {target_file}")

    def post_process(self, config):
        # 評估階段 (EvaluationStage) 會處理標籤重排序
        print("  KSOM: No post-processing required.")
        pass