# DX Cluster Analyzer

A powerful tool for analyzing DX cluster spots by frequency, mode, and band. This utility connects to a DX cluster or scrapes the dx-cluster.de website, collects spot data, and provides detailed analysis to help you understand popular frequencies and activity patterns.

## Key Features

- **Multiple Data Sources**:
  - Connect to DX clusters via Telnet (with automatic fallback to backup clusters)
  - Scrape data from dx-cluster.de website (updates every 10 seconds)
- **Mode Filtering**: Only collects SSB and CW spots (filters out digital modes)
- **Callsign Storage**: Save your callsign once and use it for future runs (optional in web mode)
- **Size Management**: Stops when reaching 500GB or 2 weeks, whichever comes first
- **Performance Optimization**: 
  - Buffered writes for improved I/O performance (raw data buffered until 10 spots)
  - Batched analysis file updates (every 1000 spots)
  - Spot caching system to prevent duplicate entries
- **Connection Reliability**:
  - Automatic backup cluster rotation if the primary fails
  - Tries different cluster after 10 consecutive disconnections
  - Exponential backoff for network reconnections
- **Real-time Display**: Shows spots in the console as they arrive
- **Multiple Output Files**:
  - Raw spots data (timestamp, frequency, callsign, etc.)
  - Frequency counts (how often each frequency appears)
  - Summary statistics by band and mode

## Installation

No external dependencies required! The script only uses Python standard library modules.

1. Clone or download this repository
2. Ensure you have Python 3.6+ installed
3. Ready to run!

## Usage

### Basic Usage (Traditional Cluster Connection)

```bash
python dx_cluster_analyzer.py --callsign YourCallsign
```
Your callsign will be saved for future use.

### Web Scraping Mode (No Callsign Required)

```bash
python dx_cluster_analyzer.py --web
```
Uses the dx-cluster.de website as a data source instead of connecting to a cluster directly.

### Advanced Options

```bash
python dx_cluster_analyzer.py --callsign YourCallsign --config custom_bands.csv --output data_folder --cluster your.cluster.net:7300 --maxsize 100 --noskimmer
```

### Command-Line Options

- `--callsign`, `-c`: Your amateur radio callsign (will be saved for future use, optional when using --web)
- `--config`: Path to band configuration file (default: band_config.csv)
- `--output`, `-o`: Output directory for data files (default: dx_data)
- `--cluster`: DX cluster host:port (default: cluster.dxwatch.com:8000)
- `--maxsize`: Maximum size in GB for data collection (default: 500.0)
- `--noskimmer`: Disable the SET/SKIMMER command (use if it causes connection issues)
- `--web`: Use dx-cluster.de website as data source instead of direct cluster connection

## Configuration

Create your band configuration file (band_config.csv) with this format:
```
Band,Mode,StartFreq,EndFreq,Region,Notes
160m,CW,1810,1838,1,
160m,SSB,1838,2000,1,
...
```

## Important Notes

- The script connects to a DX cluster (default: cluster.dxwatch.com:8000) or dx-cluster.de website
- Mode is determined from both frequency ranges and comment text
- Fixed filenames are used to prevent filling the output directory
- Comprehensive logging tracks progress
- Your callsign is stored in ~/.dx_cluster_analyzer/config.json

## Output Files

All files are stored in the output directory (default: dx_data) with fixed filenames:

- **raw_spots.csv**: All raw spot data (written every 10 spots)
- **frequency_counts.csv**: Analysis of frequency popularity (written every 1000 spots)
- **summary.csv**: Summary statistics by band and mode (written every 1000 spots)

All files are also written when the program exits to ensure no data is lost.

## Backup Clusters

The program automatically tries these backup clusters if the primary cluster fails:

1. dxc.w1nr.net:8000
2. dxc.ve7cc.net:23
3. dxspots.com:8000
4. cluster-eu-is.com:7300
5. arcluster.net:7373


