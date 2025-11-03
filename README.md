# TS-RFCM: A Robust Unsupervised Pipeline for Network Anomaly Detection using Time Series Clustering

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

This repository contains the Python implementation of TS-RFCM, a novel, unsupervised network anomaly detection pipeline. The methodology is based on the research presented in the academic paper: "Network Anomaly Detection Based on NetFlow Time Series Data in a Healthcare Network Environment".

TS-RFCM is not a single algorithm but a complete, multi-stage analytical pipeline designed to identify sophisticated, zero-day threats in complex network environments. It operates without any reliance on pre-labeled training data, making it ideal for dynamic and evolving threat landscapes. The core technical pillars of this project are:

1. A unique adaptation of Revised Fuzzy C-Means (RFCM) specifically for time-series data, where the internal distance metric is replaced by Dynamic Time Warping.

2. The use of Dynamic Time Warping (DTW) as a robust similarity measure that is resilient to the temporal shifts and distortions common in real-world network traffic.

3. The stabilization of clustering results through an ensemble method known as Evidence Accumulation Clustering (EAC), which produces a highly reliable consensus from multiple analytical perspectives.

This project formally defines and implements the `TS-RFCM` model, crystallizing the paper's core methodological innovation into a practical, usable tool. It provides the reference implementation for applying the noise-resistant RFCM algorithm natively to temporal data by integrating the time-flexible DTW metric.

## The TS-RFCM Methodology

The TS-RFCM pipeline is a multi-stage process designed to robustly handle the complexity of real-world network data.

### Stage 1: Feature Extraction

Raw network traffic (ideally in NetFlow format) is segmented into discrete time windows. For each network node (IP address), an 8-dimensional feature vector is calculated within each window. Stacking these vectors chronologically creates a multivariate time series that serves as a behavioral fingerprint for each node.

### Stage 2: Similarity Measurement with DTW

To compare the behavioral fingerprints of different nodes, **Dynamic Time Warping (DTW)** is used. Unlike simple metrics like Euclidean distance, DTW finds the optimal non-linear alignment between two time series, making it resilient to the phase shifts and delays common in network traffic.

### Stage 3: Robust Clustering with TS-RFCM

Nodes are grouped using a modified **Revised Fuzzy C-Means (RFCM)** algorithm. RFCM is a fuzzy clustering method that is inherently resistant to noise and cluster size imbalance. The core innovation of this project is replacing the standard Euclidean distance within RFCM with the DTW metric, allowing the algorithm to cluster time-series data directly.

### Stage 4: Consensus via Evidence Accumulation Clustering (EAC)

To ensure stable and reliable results, **Evidence Accumulation Clustering (EAC)** is used. This ensemble technique runs the TS-RFCM clustering process multiple times (e.g., on data aggregated at different IP netmask granularities). It then builds a co-association matrix that records how frequently each pair of nodes was grouped together. A final hierarchical clustering is performed on this matrix to obtain a robust consensus, with anomalies identified as small, distinct clusters.

## Getting Started

### Prerequisites

- Python 3.8+
- NumPy
- SciPy
- scikit-learn
- dtw-python

### Installation

Follow these steps to set up the project environment.

#### Step 1: Install System Dependencies

Ensure you have `python3-pip`, `nfdump`, `pmacctd`, and `zeek` (e.g., 6.0) installed.

Here is an example using `apt` (Debian/Ubuntu):

```bash
# Install system packages
sudo apt update
sudo apt install python3-pip nfdump pmacctd zeek-6.0

# Ensure pip is up-to-date
python3 -m pip install --upgrade pip
```

*Note: Depending on your OS, the installation for zeek may differ. Please consult its official documentation.*

#### Step 2: Install and Fix Zeek (zkg) Packages

Update the zkg package index:

```bash
sudo /opt/zeek/bin/zkg refresh
```

Install `ja3` and `anomalous-dns` (via index): (We use `sudo -E` to preserve the PATH environment variable, ensuring `zkg` can find the `zeek` executable during tests)

```bash
sudo -E /opt/zeek/bin/zkg install salesforce/ja3
sudo -E /opt/zeek/bin/zkg install jbaggs/anomalous-dns
```

Install `hassh` (via URL): (This will clone the package into a directory named `hassh.git`)

```bash
sudo -E /opt/zeek/bin/zkg install [https://github.com/salesforce/hassh.git](https://github.com/salesforce/hassh.git)
```

(Critical Fix) Rename the `hassh` package directory: For the `hassh` call in the `zeek -r` command to succeed, we must rename the `hassh.git` directory created by `zkg` to `hassh`.

```bash
# Check if 'hassh.git' exists, if so, rename it to 'hassh'
if [ -d /opt/zeek/share/zeek/site/packages/hassh.git ]; then
    sudo mv /opt/zeek/share/zeek/site/packages/hassh.git /opt/zeek/share/zeek/site/packages/hassh
    echo "Success: Renamed 'hassh.git' to 'hassh'."
else
    echo "Note: 'hassh' directory already exists or 'hassh.git' not found. No rename needed."
fi
```

#### Step 3: Install and Fix Zeek-Intelligence-Feeds (Manual)

Manually `git clone` the threat intelligence feeds:

```bash
sudo git clone [https://github.com/CriticalPathSecurity/Zeek-Intelligence-Feeds.git](https://github.com/CriticalPathSecurity/Zeek-Intelligence-Feeds.git) /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds
```

(Critical Fix 1) Fix incorrect path in `main.zeek`: This script hardcodes the `/usr/local/zeek` path. We use `sed` to fix it to the correct `/opt/zeek` path used by the `apt` installation.

```bash
sudo sed -i 's|/usr/local/zeek/share/zeek/site/Zeek-Intelligence-Feeds/|/opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds/|g' /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds/main.zeek
```

(Critical Fix 2) Fix enum error in `cps-collected-iocs.intel`: This file contains an unrecognized `indicator_type` (Intel::PHISHING). We replace it with the standard `Intel::ADDR`.

```bash
sudo sed -i 's|\tIntel::PHISHING\t|\tIntel::ADDR\t|g' /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds/cps-collected-iocs.intel
```

(Critical Fix 3) Remove broken file references in `main.zeek`: `ellio.intel` (contains HTML/CSS) and `lockbit.intel` (filename error or missing) cause errors. We comment them out directly in `main.zeek`.

```bash
# Comment out ellio.intel
sudo sed -i 's|.*ellio.intel.*|#&|' /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds/main.zeek
# Comment out all lockbit-related lines
sudo sed -i 's|.*lockbit.*|#&|' /opt/zeek/share/zeek/site/Zeek-Intelligence-Feeds/main.zeek
```

#### Step 4: Install uv

We will use `uv` for virtual environment and package management. Install it via `pip`:

```bash
pip install uv
```

#### Step 5: Clone the Repository

```bash
git clone git@github.com:pcdean2000/TS-RFCM.git
cd TS-RFCM
```

#### Step 6: Set Up Virtual Environment and Install Packages

Use `uv` to create a virtual environment named `.venv` and install the required Python packages from `requirements.txt`.

```bash
# Create the virtual environment
uv venv .venv

# Activate the virtual environment (Linux/macOS)
source .venv/bin/activate

# Install Python packages using uv
uv pip install -r requirements.txt
```

### Usage

#### Step 1: Download the Dataset

Run the script from the project root to download the dataset. Replace `<YYYYMMDD>` with your desired date.

```bash
./download_dataset.sh <YYYYMMDD>
```

#### Step 2: Run the Analysis

Execute the main analysis script:

```bash
python main.py
```

## Validation and Performance

The performance of this methodology was validated using the MAWILab dataset, a publicly available source of real-world backbone network traffic that includes professionally labeled anomalies. The accuracy was calculated using a standard confusion matrix approach.

The results demonstrate consistently high accuracy across a wide variety of modern attack techniques, showcasing the model's versatility and robustness.

## License

This project is licensed under the MIT License - see the(LICENSE) file for details.