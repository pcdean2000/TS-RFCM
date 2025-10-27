import pandas as pd
from datetime import timedelta
from glob import glob
import os
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists

class TimeSeriesGenerationStage(BaseStage):
    """
    執行時間序列生成：
    1. 讀取每個 IP 的 Parquet 特徵檔案
    2. 從 context 獲取動態的 start_time
    3. 根據 config.INTERVAL 和 config.TIMESERIES_MINUTES 進行時間分桶和彙總
    4. 儲存每個 IP 的時間序列 Parquet 檔案
    """
    
    def __init__(self, config):
        self.config = config
        self.interval = timedelta(seconds=self.config.INTERVAL)
        print("Initializing Time Series Generation Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Time Series Generation Stage...")

        # 從 context 獲取動態 start_time
        start_time = context.get('timeseries_start')
        if start_time is None:
            raise ValueError("TimeSeriesGenerationStage: 'timeseries_start' not found in context. Did FeatureEngineeringStage fail?")
        
        # 確保 start_time 是 pandas Timestamp
        self.start_time = pd.to_datetime(start_time)
        # [MODIFIED] 使用 TIMESERIES_MINUTES 計算 end_time
        self.end_time = self.start_time + timedelta(minutes=self.config.TIMESERIES_MINUTES)
        
        print(f"  Time window set: {self.start_time} to {self.end_time}")

        source_files = glob(str(self.config.FEATURE_DIR / '*.parquet'))
        if not source_files:
            print(f"  Warning: No feature files found in {self.config.FEATURE_DIR}.")
            return context
            
        for filename in source_files:
            print(f"  Processing {filename}", " " * 50, end="\r")
            
            targetFile = str(filename).replace(
                str(self.config.FEATURE_DIR), 
                str(self.config.TIMESERIES_DIR)
            )
            
            if os.path.exists(targetFile):
                continue
                
            try:
                df = pd.read_parquet(filename)
                if df.empty:
                    continue
                df[["timeStart"]] = df[["timeStart"]].astype(dtype='datetime64[ns]')
                
                # _aggregate_to_interval 會使用 self.start_time 和 self.end_time
                result_df = self._aggregate_to_interval(df)
                
                ensure_dir_exists(targetFile)
                result_df.to_parquet(targetFile, index=False)
            except Exception as e:
                print(f"\n    Error processing {filename}: {e}")

        print("\nTime Series Generation Stage Complete.")
        context['timeseries_generation_complete'] = True
        return context

    def _aggregate_to_interval(self, df: pd.DataFrame) -> pd.DataFrame:
        """ 將單一 IP 的秒級數據彙總到時間間隔 """
        result = []
        current_time = self.start_time
        src_ip = df["srcIP"].unique().tolist()[0]

        while current_time <= self.end_time:
            window_end = current_time + self.interval
            filtered = df[
                (df["timeStart"] >= current_time) & 
                (df["timeStart"] < window_end)
            ]
            
            sum_packets = filtered["packets"].sum()
            sum_bytes = filtered["bytes"].sum()
            sum_flows = filtered["flows"].sum()
            
            if sum_packets != 0 and sum_bytes != 0:
                bytes_per_packet = sum_bytes / sum_packets
                flows_per_byte_packet = sum_flows / bytes_per_packet
            else:
                bytes_per_packet = 0
                flows_per_byte_packet = 0

            result.append({
                "timeStart": current_time,
                "srcIP": src_ip,
                "packets": sum_packets,
                "bytes": sum_bytes,
                "flows": sum_flows,
                "bytes/packets": bytes_per_packet,
                "flows/(bytes/packets)": flows_per_byte_packet,
                "nDstIP": filtered["nDstIP"].sum(),
                "nSrcPort": filtered["nSrcPort"].sum(),
                "nDstPort": filtered["nDstPort"].sum(),
            })
            current_time += self.interval
            
        return pd.DataFrame(result)