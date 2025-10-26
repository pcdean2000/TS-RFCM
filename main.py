import config
from pipeline.base_stage import BaseStage
from pipeline.preprocessing import PreprocessingStage
from pipeline.feature_engineering import FeatureEngineeringStage
from pipeline.timeseries import TimeSeriesGenerationStage
from pipeline.reformatting import DataReformattingStage
from pipeline.clustering import ClusteringStage
from pipeline.evaluation import EvaluationStage
import time

class AnalysisFacade:
    """
    Facade Pattern 實作。
    """
    
    def __init__(self, config):
        self.config = config
        self.pipeline: list[BaseStage] = []
        self._register_stages()
        self.context = {} # 用於在階段之間傳遞資訊

    def _register_stages(self):
        """
        按照正確的順序註冊所有管線階段。
        """
        self.pipeline.append(PreprocessingStage(self.config))
        self.pipeline.append(FeatureEngineeringStage(self.config))
        self.pipeline.append(TimeSeriesGenerationStage(self.config))
        self.pipeline.append(DataReformattingStage(self.config))
        self.pipeline.append(ClusteringStage(self.config))
        self.pipeline.append(EvaluationStage(self.config))
        
        print(f"Analysis pipeline registered with {len(self.pipeline)} stages.")

    def run_analysis(self):
        """
        依序執行管線中的所有階段。
        """
        print("========== ANALYSIS PIPELINE STARTING ==========")
        start_time = time.time()
        
        for stage in self.pipeline:
            stage_name = stage.__class__.__name__
            print(f"\n======= EXECUTING STAGE: {stage_name} =======")
            try:
                stage_start = time.time()
                self.context = stage.execute(self.context)
                stage_end = time.time()
                print(f"======= STAGE {stage_name} COMPLETED IN {stage_end - stage_start:.2f}s =======")
            except Exception as e:
                print(f"\n!!!!!! CRITICAL ERROR IN STAGE: {stage_name} !!!!!!")
                print(f"Error: {e}")
                print("Pipeline execution halted.")
                import traceback
                traceback.print_exc()
                return # 發生錯誤時停止管線

        end_time = time.time()
        print(f"\n========== ANALYSIS PIPELINE FINISHED ==========")
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    # 1. 建立 Facade
    facade = AnalysisFacade(config)
    
    # 2. 執行完整分析
    facade.run_analysis()