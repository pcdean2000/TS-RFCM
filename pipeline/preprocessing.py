import pandas as pd
from glob import glob
import os
from .base_stage import BaseStage
from utils.helpers import ensure_dir_exists
from parsezeeklogs import ParseZeekLogs

class PreprocessingStage(BaseStage):
    """
    執行所有資料預處理任務：
    1. 清理 Netflow CSV
    2. 轉換 Zeek logs
    3. 過濾 conn.csv
    4. 過濾 Netflow
    """
    
    def __init__(self, config):
        self.config = config
        print("Initializing Preprocessing Stage...")

    def execute(self, context: dict) -> dict:
        print("Executing Preprocessing Stage...")
        
        self._clean_netflow_summaries()
        self._convert_zeek_logs()
        self._filter_conn_log()
        self._filter_netflow_logs()
        
        print("Preprocessing Stage Complete.")
        context['preprocessing_complete'] = True
        return context

    def _clean_netflow_summaries(self):
        """ 1. 移除 Netflow CSV 檔案末尾的 'Summary' 行 """
        print("  Cleaning Netflow 'Summary' rows...")
        for file in glob(str(self.config.NETFLOW_DIR / '*.csv')):
            try:
                netflow = pd.read_csv(file)
                summary_index = netflow[netflow["ts"] == self.config.NETFLOW_SUMMARY_STRING].index
                if not summary_index.empty:
                    first_summary_index = summary_index.values[0]
                    netflow.drop(netflow.tail(len(netflow) - first_summary_index).index, inplace=True)
                    netflow.to_csv(file, index=False)
            except Exception as e:
                print(f"    Warning: Could not process {file}: {e}")

    def _convert_zeek_logs(self):
        """ 2. 使用 ParseZeekLogs 將 Zeek .log 轉換為 .csv """
        print("  Converting Zeek logs to CSV...")
        ensure_dir_exists(self.config.ZEEK_CSVS['conn'])
        
        for name, log_file in self.config.ZEEK_LOGS.items():
            out_csv = self.config.ZEEK_CSVS[name]
            if os.path.exists(out_csv):
                print(f"    {out_csv} already exists, skipping conversion.")
                continue
            
            if not os.path.exists(log_file):
                print(f"    Warning: Log file {log_file} not found, skipping.")
                continue

            print(f"    Processing {log_file} -> {out_csv}")
            try:
                with open(out_csv, "w") as outfile:
                    zeekLogs = ParseZeekLogs(str(log_file), output_format="csv")
                    outfile.write(zeekLogs.get_fields() + "\n")
                    for log_record in zeekLogs:
                        if log_record is not None:
                            outfile.write(log_record + "\n")
            except Exception as e:
                print(f"    Error converting {log_file}: {e}")

    def _filter_conn_log(self):
        """ 3. 根據 analyzer 和 dns logs 過濾 conn.csv """
        print("  Filtering conn.csv...")
        try:
            conn = pd.read_csv(self.config.ZEEK_CSVS['conn'], low_memory=False)
            analyzer = pd.read_csv(self.config.ZEEK_CSVS['analyzer'])
            dns = pd.read_csv(self.config.ZEEK_CSVS['dns'])
            # weird = pd.read_csv(self.config.ZEEK_CSVS['weird']) # 根據原始碼，weird 被註解掉了

            conn_filtered = conn[~conn.uid.isin(analyzer.uid)]
            conn_filtered = conn_filtered[~conn_filtered.uid.isin(dns.uid)]
            # conn_filtered = conn_filtered[~conn_filtered.uid.isin(weird.uid)]

            conn_filtered.to_csv(self.config.ZEEK_CSVS['filtered_conn'], index=False)
            print(f"    Saved filtered conn log to {self.config.ZEEK_CSVS['filtered_conn']}")
        except FileNotFoundError as e:
            print(f"    Error: Missing Zeek CSV file. Did conversion fail? {e}")
        except Exception as e:
            print(f"    Error filtering conn.csv: {e}")

    def _filter_netflow_logs(self):
        """ 4. 根據 analyzer 和 dns IP/Port 過濾 Netflow 檔案 """
        print("  Filtering Netflow files...")
        try:
            analyzer = pd.read_csv(self.config.ZEEK_CSVS['analyzer'])
            dns = pd.read_csv(self.config.ZEEK_CSVS['dns'])

            analyzer_dns = pd.merge(
                analyzer[["id.orig_h", "id.resp_h", "id.orig_p", "id.resp_p"]],
                dns[["id.orig_h", "id.resp_h", "id.orig_p", "id.resp_p"]],
                how="outer"
            )
            analyzer_dns.columns = ["sa", "da", "sp", "dp"]

            for file_path in glob(str(self.config.NETFLOW_DIR / '*.csv')):
                if "_filtered.csv" in file_path:
                    continue
                
                print(f"    Filtering {file_path}...")
                netflow = pd.read_csv(file_path, low_memory=False)
                # 確保欄位類型一致以進行合併
                for col in ["sa", "da", "sp", "dp"]:
                    if col in netflow.columns:
                         netflow[col] = netflow[col].astype(str)
                    if col in analyzer_dns.columns:
                        analyzer_dns[col] = analyzer_dns[col].astype(str)

                netflow_filtered = pd.merge(netflow, analyzer_dns, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
                
                filtered_file_path = file_path.replace(".csv", "_filtered.csv")
                netflow_filtered.to_csv(filtered_file_path, index=False)
                print(f"    Saved filtered netflow to {filtered_file_path}")
        except FileNotFoundError as e:
            print(f"    Error: Missing Zeek CSV file. Did conversion fail? {e}")
        except Exception as e:
            print(f"    Error filtering netflow logs: {e}")