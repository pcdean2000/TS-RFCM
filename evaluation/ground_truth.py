import pandas as pd
import ipaddress
import time
from zat.log_to_dataframe import LogToDataFrame
from utils.helpers import is_valid_ip

class GroundTruthGenerator:
    """
    從 Zeek logs (notice, conn, weird) 生成 Ground Truth 可疑 IP 列表。
    """
    
    def __init__(self):
        self.log_to_df = LogToDataFrame()
        self.anomalous_ip_set = set()

    def generate(self, config) -> list:
        """
        執行 Ground Truth 生成。
        
        :param config: 包含日誌路徑和閾值的 config 物件
        :return: 一個排序後的唯一可疑 IP 列表
        """
        print("Generating Ground Truth list...")
        
        self._process_notice_log(config)
        self._process_conn_log(config)
        self._process_weird_log(config)
        
        return self._sort_ips()

    def _process_log(self, log_path, src_ip_col, analysis_func=None):
        """ 輔助函式：讀取日誌並應用分析 """
        print(f"\n  Processing {log_path}...")
        start_time = time.time()
        added_count = 0
        try:
            df = self.log_to_df.create_dataframe(str(log_path))
            read_time = time.time() - start_time
            print(f"    Read complete ({read_time:.2f}s). Analyzing...")
            
            if src_ip_col not in df.columns:
                print(f"    Warning: '{src_ip_col}' column not found in {log_path}")
                return

            if analysis_func:
                valid_ips = analysis_func(df)
            else:
                # 預設行為 (for notice.log)
                valid_ips = df[src_ip_col][df[src_ip_col].apply(is_valid_ip)].unique()

            for ip in valid_ips:
                self.anomalous_ip_set.add(ip)
            added_count = len(valid_ips)
            print(f"    Added {added_count} unique valid IPs from {log_path}.")

        except FileNotFoundError:
            print(f"    Error: File not found '{log_path}'")
        except MemoryError:
            print(f"    Error: MemoryError processing {log_path}.")
        except Exception as e:
            print(f"    Error processing {log_path}: {e}")
        
        print(f"    Finished {log_path} (Total: {time.time() - start_time:.2f}s).")

    def _process_notice_log(self, config):
        log_path = config.GROUND_TRUTH_LOGS["notice"]
        src_col = config.ZEEK_SRC_IP_COLS["notice"]
        self._process_log(log_path, src_col)

    def _process_conn_log(self, config):
        log_path = config.GROUND_TRUTH_LOGS["conn"]
        src_col = config.ZEEK_SRC_IP_COLS["conn"]
        threshold = config.MIN_CONN_THRESHOLD
        states = config.ZEEK_ANOMALY_STATES
        
        def conn_analysis(df):
            if 'conn_state' not in df.columns:
                print("    Warning: 'conn_state' column not found in conn.log.")
                return []
            
            df_filtered = df[df['conn_state'].isin(states)]
            ip_counts = df_filtered[src_col].value_counts()
            threshold_ips = ip_counts[ip_counts >= threshold].index.tolist()
            print(f"    Found {len(threshold_ips)} IPs with S0/REJ count >= {threshold}")
            return [ip for ip in threshold_ips if is_valid_ip(ip)]
            
        self._process_log(log_path, src_col, conn_analysis)

    def _process_weird_log(self, config):
        log_path = config.GROUND_TRUTH_LOGS["weird"]
        src_col = config.ZEEK_SRC_IP_COLS["weird"]
        threshold = config.MIN_WEIRD_THRESHOLD

        def weird_analysis(df):
            ip_counts = df[src_col].value_counts()
            threshold_ips = ip_counts[ip_counts >= threshold].index.tolist()
            print(f"    Found {len(threshold_ips)} IPs with count >= {threshold}")
            return [ip for ip in threshold_ips if is_valid_ip(ip)]

        self._process_log(log_path, src_col, weird_analysis)

    def _sort_ips(self) -> list:
        """ 分類 (v4/v6) 並排序 IP 位址 """
        print("\nSorting final IP list...")
        ipv4_list, ipv6_list, invalid_ips = [], [], []

        for ip_str in self.anomalous_ip_set:
            try:
                ip_obj = ipaddress.ip_address(ip_str)
                (ipv4_list if ip_obj.version == 4 else ipv6_list).append(ip_obj)
            except ValueError:
                invalid_ips.append(ip_str)

        sorted_ipv4 = sorted(ipv4_list)
        sorted_ipv6 = sorted(ipv6_list)
        anomalous_ip_list = [str(ip) for ip in sorted_ipv4] + [str(ip) for ip in sorted_ipv6]
        
        print(f"--- Ground Truth Summary ---")
        print(f"Total unique anomalous IPs: {len(anomalous_ip_list)}")
        if invalid_ips:
            print(f"Found {len(invalid_ips)} unparseable IPs (excluded).")
        
        return anomalous_ip_list