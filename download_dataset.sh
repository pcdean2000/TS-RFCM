#!/bin/bash

# 當任何指令失敗時立即退出，並將未設定的變數視為錯誤
set -euo pipefail

# --- 常數定義 ---
readonly PROG_NAME=$(basename "$0")
readonly PCAP_BASE_URL="http://mawi.wide.ad.jp/mawi/samplepoint-F"
readonly NFPROBE_PORT="2055"

# --- 函式定義 ---

# 顯示用法並退出
usage() {
	echo "用法: $PROG_NAME <YYYYMMDD>"
	echo "描述: 下載並處理指定日期的 MAWI 流量樣本。"
	exit 1
}

# 記錄訊息
log() {
	echo "==> $1" >&2
}

# 檢查必要的指令是否存在
check_dependencies() {
	local missing=0
	for cmd in wget gunzip pmacctd nfdump zeek; do
		if ! command -v "$cmd" &>/dev/null; then
			echo "錯誤: 缺少必要的指令: $cmd" >&2
			missing=1
		fi
	done
	if [ "$missing" -eq 1 ]; then
		exit 1
	fi
}

# 下載並解壓縮 pcap 檔案
# $1: 日期 (YYYYMMDD)
# $2: pcap 目錄
# 返回: pcap 檔案的路徑
download_and_extract() {
	local date="$1"
	local pcap_dir="$2"
	local year="${date::4}"
	local basename="${date}1400"
	local pcap_file="${basename}.pcap"
	local dump_file="${basename}.dump"
	
	# 檢查是否已經存在解壓縮後的檔案
	if [ -f "$pcap_file" ]; then
		log "找到現有的 pcap 檔案: ${pcap_file}，跳過下載和解壓縮步驟"
		echo "$pcap_file"
		return 0
	elif [ -f "$dump_file" ]; then
		log "找到現有的 dump 檔案: ${dump_file}，跳過下載和解壓縮步驟"
		echo "$dump_file"
		return 0
	elif [ -f "${pcap_dir}/${pcap_file}" ]; then
		log "找到已歸檔的 pcap 檔案: ${pcap_dir}/${pcap_file}，跳過下載和解壓縮步驟"
		echo "${pcap_dir}/${pcap_file}"
		return 0
	elif [ -f "${pcap_dir}/${dump_file}" ]; then
		log "找到已歸檔的 dump 檔案: ${pcap_dir}/${dump_file}，跳過下載和解壓縮步驟"
		echo "${pcap_dir}/${dump_file}"
		return 0
	fi
	
	local pcap_url="${PCAP_BASE_URL}/${year}/${basename}.pcap.gz"
	local dump_url="${PCAP_BASE_URL}/${year}/${basename}.dump.gz"
	local download_url=""
	local gz_filename=""

	log "正在檢查可用的檔案..."
	if wget -q --spider "$pcap_url"; then
		download_url="$pcap_url"
		gz_filename="${basename}.pcap.gz"
	elif wget -q --spider "$dump_url"; then
		download_url="$dump_url"
		gz_filename="${basename}.dump.gz"
	else
		echo "錯誤：在以下位置均找不到檔案：" >&2
		echo "- $pcap_url" >&2
		echo "- $dump_url" >&2
		exit 1
	fi

	log "正在從 ${download_url} 下載..."
	wget -q --show-progress "$download_url"

	log "正在解壓縮 ${gz_filename}..."
	gunzip -f "$gz_filename"

	# 回傳解壓縮後的檔案名稱
	echo "${gz_filename%.gz}"
}

# 產生 pmacct 設定檔
# $1: pcap 檔案路徑
# 返回: 設定檔的路徑
generate_pmacct_config() {
	local pcap_file="$1"
	local config_file
	config_file=$(mktemp) # 建立安全的臨時檔案

	log "正在產生 pmacct 設定檔..."
	cat >"$config_file" <<EOF
daemonize: false
pcap_savefile: ${pcap_file}
savefile_wait: true
aggregate: src_host, dst_host, src_port, dst_port, proto, tos
plugins: nfprobe
nfprobe_receiver: localhost:${NFPROBE_PORT}
nfprobe_version: 9
nfprobe_timeouts: tcp=30:maxlife=60
EOF
	echo "$config_file"
}

# 執行 pmacctd
# $1: 設定檔路徑
# $2: nfdump 資料目錄
run_pmacct() {
	local config_file="$1"
	local nfdump_dir="$2"
	
	# 檢查 nfdump 目錄是否已有資料
	if [ "$(find "$nfdump_dir" -name 'nfcapd.*' -print -quit 2>/dev/null)" ]; then
		log "找到現有的 nfdump 資料，正在清除以便重新產生..."
        # 使用與 cleanup 函式相同的 sudo 邏輯
		if sudo -n true 2>/dev/null; then
			sudo rm -f "${nfdump_dir}"/nfcapd.*
		else
			echo "需要 sudo 權限來清空 ${nfdump_dir}。請輸入密碼："
			sudo rm -f "${nfdump_dir}"/nfcapd.*
		fi
		log "舊資料清除完成。"
	fi
	
	log "正在執行 pmacctd 來處理 pcap..."
	pmacctd -f "$config_file"
}

# --- MODIFIED: 移除內部的檔案存在檢查 ---
# 執行 nfdump
# $1: nfdump 資料目錄
# $2: 輸出 CSV 檔案路徑
run_nfdump() {
	local nfdump_dir="$1"
	local output_csv="$2"
	
	log "正在將 netflow 資料轉換為 ${output_csv}..."
	nfdump -R "$nfdump_dir" -o extended -o csv >"$output_csv"
}
# --- END MODIFICATION ---

# 執行 Zeek
# $1: pcap 檔案路徑 (e.g., data/pcap/file.pcap)
# $2: Zeek 輸出目錄 (e.g., data/zeek)
run_zeek() {
	local pcap_file="$1"
	local zeek_dir="$2"
	
	# 檢查 Zeek 輸出目錄是否已有日誌檔案
	if [ -d "$zeek_dir" ] && [ "$(find "$zeek_dir" -name '*.log' -print -quit 2>/dev/null)" ]; then
		log "找到現有的 Zeek 日誌檔案於 ${zeek_dir}/，跳過 Zeek 處理步驟"
		return 0
	fi
	
	log "正在使用 Zeek 處理 pcap 檔案..."
	mkdir -p "$zeek_dir"
	(
		# 設定 ZEEKPATH 環境變數
		export ZEEKPATH="/opt/zeek/share/zeek/site/packages:.:/opt/zeek/share/zeek:/opt/zeek/share/zeek/policy:/opt/zeek/share/zeek/site:/opt/zeek/share/zeek/builtin-plugins:/opt/zeek/share/zeek/builtin-plugins/Zeek_AF_Packet"
		
		cd "$zeek_dir"
		
		# 動態計算相對路徑
		local pcap_dir_basename=$(basename "$(dirname "$pcap_file")") # "pcap"
		local pcap_filename=$(basename "$pcap_file")             # "file.pcap"
		local relative_pcap_path="../${pcap_dir_basename}/${pcap_filename}" # "../pcap/file.pcap"

		log "執行: zeek -r ${relative_pcap_path} policy/frameworks/intel/seen ja3 hassh anomalous-dns /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds"
		
		# 執行使用者指定的 Zeek 指令
		zeek -r "${relative_pcap_path}" \
			 policy/frameworks/intel/seen \
			 ja3 \
			 hassh \
			 anomalous-dns \
			 /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds
	)
	log "Zeek 處理完成。日誌檔案位於 ${zeek_dir}/ 目錄中。"
}

# 清理檔案
# $1: pmacct 設定檔
# $2: nfdump 資料目錄
cleanup() {
	local config_file="$1"
	local nfdump_dir="$2"
	log "正在清理..."
	rm -f "$config_file"
	log "正在清空 nfdump 快取目錄..."
	# 使用 sudo 前先提示，或檢查是否需要
    # 這裡保留，作為最後的清理步驟
	if [ "$(find "$nfdump_dir" -mindepth 1 -print -quit)" ]; then
		if sudo -n true 2>/dev/null; then
			sudo rm -f "${nfdump_dir}"/*
		else
			echo "需要 sudo 權限來清空 ${nfdump_dir}。請輸入密碼："
			sudo rm -f "${nfdump_dir}"/*
		fi
	fi
}

# --- 主函式 ---
main() {
	check_dependencies

	if [ -z "${1:-}" ]; then
		usage
	fi

	local date="$1"
	
	local pcap_dir="data/pcap"
	local netflow_dir="data/netflow"
	local zeek_dir="data/zeek"
	
	local nfdump_dir="/var/cache/nfdump/"
	local output_csv="${netflow_dir}/${date}.csv"

	# 建立輸出目錄
	mkdir -p "$pcap_dir" "$netflow_dir" "$zeek_dir"

	# 執行流程
	local pcap_file
	pcap_file=$(download_and_extract "$date" "$pcap_dir")
	
	# 確定 pcap 檔案的實際位置
	local actual_pcap_path
	if [[ "$pcap_file" == "$pcap_dir"/* ]]; then
		# 檔案已經在 pcap_dir 中
		actual_pcap_path="$pcap_file"
	else
		# 檔案還在當前目錄
		actual_pcap_path="$pcap_file"
	fi

	local config_file
	config_file=$(generate_pmacct_config "$actual_pcap_path")

	# --- MODIFIED: 檢查最終 CSV 檔案是否存在 ---
	if [ -f "$output_csv" ] && [ -s "$output_csv" ]; then
		log "找到現有的 netflow CSV 檔案: ${output_csv}，跳過 pmacctd 和 nfdump 處理步驟"
	else
		log "Netflow CSV 檔案不存在，開始執行 pmacctd 和 nfdump..."
		run_pmacct "$config_file" "$nfdump_dir"
		run_nfdump "$nfdump_dir" "$output_csv"
	fi
	# --- END MODIFICATION ---

	# 將 pcap 歸檔（如果還沒歸檔）
	if [[ "$actual_pcap_path" != "$pcap_dir"/* ]]; then
		log "正在將 ${pcap_file} 移動到 ${pcap_dir}/ 目錄..."
		mv "$pcap_file" "$pcap_dir/"
		actual_pcap_path="${pcap_dir}/${pcap_file}"
	else
		log "pcap 檔案已在 ${pcap_dir}/ 目錄中"
	fi

	run_zeek "$actual_pcap_path" "$zeek_dir"

	cleanup "$config_file" "$nfdump_dir"

	log "處理完成。輸出檔案為 ${output_csv}"
}

# --- 腳本進入點 ---
main "$@"