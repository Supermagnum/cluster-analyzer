# cluster-analyzer

Key Features:

CSV Configuration Support: Reads your band configuration file with Band, Mode, StartFreq, EndFreq, Region, Notes format
Mode Filtering: Only collects SSB and CW spots (filters out digital modes)
Size Management: Stops when reaching 500GB or 2 weeks, whichever comes first
Multiple Output Files:

Raw spots data (timestamp, frequency, callsign, etc.)
Frequency counts (how often each frequency appears)
Summary statistics by band and mode



Usage:

Create your configuration file (band_config.csv) with your frequency ranges
Install required Python packages (all standard library - no extra installs needed)
Modify the script:

Change self.callsign = "ANALYZER" to your actual callsign
Optionally change self.cluster_host to your preferred DX cluster


Run the script: python dx_analyzer.py

Important Notes:

The script connects to a DX cluster (default: hb9dxc.net:8000)
You may need to adjust the cluster host/port for your preferred cluster
The script automatically determines mode from frequency ranges and comments
It creates timestamped files to avoid overwriting previous runs
Includes comprehensive logging to track progress


