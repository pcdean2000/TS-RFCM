import pandas as pd
from datetime import timedelta
from glob import glob
import os
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists

class TimeSeriesGenerationStage(BaseStage):
    """
    針對每個 Mask 的特徵檔案生成時間序列。
    """
    
    def __init__(self, config):
        self.config = config
        self.interval = timedelta(seconds=self.config.INTERVAL)
        print("Initializing Time Series Generation Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Time Series Generation Stage...")

        start_time = context.get('timeseries_start')
        if start_time is None:
            raise ValueError("Context missing 'timeseries_start'")
        
        self.start_time = pd.to_datetime(start_time)
        self.end_time = self.start_time + timedelta(minutes=self.config.TIMESERIES_MINUTES)
        
        # 遍歷所有 Mask 目錄
        for mask in self.config.EAC_MASKS:
            src_dir = self.config.FEATURE_DIR_BASE / f"mask_{mask}"
            
            # 目標目錄: output/timeseries/interval_30_src_feature/mask_32/
            target_dir_prefix = self.config.TIMESERIES_DIR_PREFIX # e.g. interval_30_src_feature
            target_dir = self.config.TIMESERIES_DIR_BASE / target_dir_prefix / f"mask_{mask}"
            
            source_files = glob(str(src_dir / '*.parquet'))
            print(f"  Mask /{mask}: Processing {len(source_files)} files -> {target_dir}")
            
            for filename in source_files:
                targetFile = target_dir / os.path.basename(filename)
                
                if os.path.exists(targetFile):
                    continue
                    
                try:
                    df = pd.read_parquet(filename)
                    if df.empty: continue
                    df[["timeStart"]] = df[["timeStart"]].astype(dtype='datetime64[ns]')
                    
                    result_df = self._aggregate_to_interval(df)
                    
                    ensure_dir_exists(targetFile)
                    result_df.to_parquet(targetFile, index=False)
                except Exception as e:
                    print(f"    Error processing {filename}: {e}")

        context['timeseries_generation_complete'] = True
        return context

    def _aggregate_to_interval(self, df: pd.DataFrame) -> pd.DataFrame:
        """ 將單一 IP 的秒級數據彙總到時間間隔 """
        result = []
        current_time = self.start_time
        src_ip = df["srcIP"].unique().tolist()[0]

        while current_time <= self.end_time:
            window_end = current_time + self.interval
            filtered = df[(df["timeStart"] >= current_time) & (df["timeStart"] < window_end)]
            
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