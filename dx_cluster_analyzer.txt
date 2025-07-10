#!/usr/bin/env python3
"""
DX Cluster Frequency Analyzer
Pulls DX cluster data and analyzes frequency popularity for SSB and CW modes.
"""

import csv
import os
import time
import socket
import threading
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
import re
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dx_analyzer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DXClusterAnalyzer:
    def __init__(self, config_file: str = "band_config.csv", 
                 output_dir: str = "dx_data", 
                 max_size_gb: float = 500.0):
        self.config_file = config_file
        self.output_dir = output_dir
        self.max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        self.running = False
        self.start_time = None
        self.band_configs = []
        self.raw_data_file = None
        self.processed_data_file = None
        self.summary_file = None
        
        # DX Cluster connection settings
        self.cluster_host = "dxcluster.hb9dxc.net"  # Example cluster
        self.cluster_port = 8000
        self.socket = None
        self.callsign = "ANALYZER"  # Change this to your callsign
        
        # Data storage
        self.frequency_counts = defaultdict(lambda: defaultdict(int))
        self.total_spots = 0
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
    def load_band_config(self) -> bool:
        """Load band configuration from CSV file"""
        try:
            with open(self.config_file, 'r') as f:
                reader = csv.DictReader(f)
                self.band_configs = list(reader)
                
            logger.info(f"Loaded {len(self.band_configs)} band configurations")
            for config in self.band_configs:
                logger.info(f"Band: {config['Band']}, Mode: {config['Mode']}, "
                           f"Range: {config['StartFreq']}-{config['EndFreq']} kHz, "
                           f"Region: {config['Region']}")
            return True
            
        except FileNotFoundError:
            logger.error(f"Configuration file {self.config_file} not found")
            return False
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return False
    
    def setup_output_files(self):
        """Setup output CSV files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Raw data file
        self.raw_data_file = os.path.join(self.output_dir, f"raw_spots_{timestamp}.csv")
        with open(self.raw_data_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Frequency', 'Callsign', 'Spotter', 'Mode', 'Band', 'Region'])
        
        # Processed data file
        self.processed_data_file = os.path.join(self.output_dir, f"frequency_counts_{timestamp}.csv")
        
        # Summary file
        self.summary_file = os.path.join(self.output_dir, f"summary_{timestamp}.csv")
        
        logger.info(f"Output files created: {self.raw_data_file}")
    
    def connect_to_cluster(self) -> bool:
        """Connect to DX cluster"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(30)
            self.socket.connect((self.cluster_host, self.cluster_port))
            
            # Send callsign
            self.socket.send(f"{self.callsign}\n".encode())
            
            # Read initial messages
            for _ in range(10):  # Read first few messages
                try:
                    data = self.socket.recv(1024).decode('utf-8', errors='ignore')
                    if data:
                        logger.info(f"Cluster: {data.strip()}")
                except:
                    break
            
            logger.info(f"Connected to DX cluster: {self.cluster_host}:{self.cluster_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to cluster: {e}")
            return False
    
    def parse_dx_spot(self, line: str) -> Tuple[str, str, str, str, float]:
        """Parse DX spot line and extract relevant information"""
        # DX spot format: DX de CALL: freq DX_CALL comment time
        # Example: DX de ON4KST: 14205.0 JA1ABC CQ                1200Z
        
        spot_pattern = r'DX de (\w+):\s+(\d+\.?\d*)\s+(\w+)\s+(.+?)\s+(\d{4}Z)'
        match = re.match(spot_pattern, line.strip())
        
        if match:
            spotter = match.group(1)
            frequency = float(match.group(2))
            dx_call = match.group(3)
            comment = match.group(4).strip()
            time_str = match.group(5)
            
            return spotter, dx_call, comment, time_str, frequency
        
        return None, None, None, None, 0.0
    
    def determine_mode_and_band(self, frequency: float, comment: str) -> Tuple[str, str, str]:
        """Determine mode and band based on frequency and comment"""
        mode = "UNKNOWN"
        band = "UNKNOWN"
        region = "UNKNOWN"
        
        # Check comment for mode indicators
        comment_upper = comment.upper()
        if any(cw_indicator in comment_upper for cw_indicator in ['CW', 'QRS', 'K1A', 'K2A']):
            mode = "CW"
        elif any(ssb_indicator in comment_upper for ssb_indicator in ['SSB', 'LSB', 'USB', 'CQ', 'PHONE']):
            mode = "SSB"
        elif any(ssb_indicator in comment_upper for ssb_indicator in ['FT8', 'FT4', 'PSK', 'RTTY']):
            mode = "DIGITAL"  # We'll filter this out
        else:
            # Try to determine mode by frequency
            for config in self.band_configs:
                start_freq = float(config['StartFreq'])
                end_freq = float(config['EndFreq'])
                if start_freq <= frequency <= end_freq:
                    mode = config['Mode']
                    band = config['Band']
                    region = config['Region']
                    break
        
        # If we couldn't determine from config, try frequency-based band determination
        if band == "UNKNOWN":
            if 1800 <= frequency <= 2000:
                band = "160m"
            elif 3500 <= frequency <= 4000:
                band = "80m"
            elif 5330 <= frequency <= 5406:
                band = "60m"
            elif 7000 <= frequency <= 7300:
                band = "40m"
            elif 10100 <= frequency <= 10150:
                band = "30m"
            elif 14000 <= frequency <= 14350:
                band = "20m"
            elif 18068 <= frequency <= 18168:
                band = "17m"
            elif 21000 <= frequency <= 21450:
                band = "15m"
            elif 24890 <= frequency <= 24990:
                band = "12m"
            elif 28000 <= frequency <= 29700:
                band = "10m"
        
        return mode, band, region
    
    def should_include_spot(self, frequency: float, mode: str) -> bool:
        """Check if spot should be included based on our criteria"""
        if mode not in ['CW', 'SSB']:
            return False
        
        for config in self.band_configs:
            if config['Mode'] == mode:
                start_freq = float(config['StartFreq'])
                end_freq = float(config['EndFreq'])
                if start_freq <= frequency <= end_freq:
                    return True
        
        return False
    
    def get_directory_size(self) -> int:
        """Get total size of output directory in bytes"""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.output_dir):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
        return total_size
    
    def save_frequency_counts(self):
        """Save frequency counts to CSV file"""
        with open(self.processed_data_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Frequency', 'Mode', 'Band', 'Count', 'Percentage'])
            
            for freq, modes in self.frequency_counts.items():
                for mode, count in modes.items():
                    percentage = (count / self.total_spots) * 100 if self.total_spots > 0 else 0
                    # Determine band for this frequency
                    mode_name, band, region = self.determine_mode_and_band(freq, "")
                    writer.writerow([freq, mode, band, count, f"{percentage:.2f}%"])
    
    def generate_summary(self):
        """Generate summary statistics"""
        summary_data = defaultdict(lambda: defaultdict(int))
        
        for freq, modes in self.frequency_counts.items():
            mode_name, band, region = self.determine_mode_and_band(freq, "")
            for mode, count in modes.items():
                summary_data[band][mode] += count
        
        with open(self.summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Band', 'Mode', 'Total_Spots', 'Percentage'])
            
            for band, modes in summary_data.items():
                for mode, count in modes.items():
                    percentage = (count / self.total_spots) * 100 if self.total_spots > 0 else 0
                    writer.writerow([band, mode, count, f"{percentage:.2f}%"])
    
    def process_cluster_data(self):
        """Main loop to process cluster data"""
        logger.info("Starting data collection...")
        
        while self.running:
            try:
                # Check time limit (2 weeks)
                if datetime.now() - self.start_time > timedelta(weeks=2):
                    logger.info("Time limit reached (2 weeks)")
                    break
                
                # Check size limit
                if self.get_directory_size() > self.max_size_bytes:
                    logger.info(f"Size limit reached ({self.max_size_bytes / (1024**3):.1f} GB)")
                    break
                
                # Read data from cluster
                data = self.socket.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    logger.warning("No data received, reconnecting...")
                    self.connect_to_cluster()
                    continue
                
                lines = data.strip().split('\n')
                
                for line in lines:
                    if line.startswith('DX de '):
                        spotter, dx_call, comment, time_str, frequency = self.parse_dx_spot(line)
                        
                        if frequency > 0:
                            mode, band, region = self.determine_mode_and_band(frequency, comment)
                            
                            if self.should_include_spot(frequency, mode):
                                # Save raw data
                                timestamp = datetime.now().isoformat()
                                with open(self.raw_data_file, 'a', newline='') as f:
                                    writer = csv.writer(f)
                                    writer.writerow([timestamp, frequency, dx_call, spotter, mode, band, region])
                                
                                # Update counts
                                self.frequency_counts[frequency][mode] += 1
                                self.total_spots += 1
                                
                                if self.total_spots % 100 == 0:
                                    logger.info(f"Processed {self.total_spots} spots")
                
                # Save processed data every 1000 spots
                if self.total_spots % 1000 == 0 and self.total_spots > 0:
                    self.save_frequency_counts()
                    self.generate_summary()
                
                time.sleep(0.1)  # Small delay to prevent overwhelming
                
            except socket.timeout:
                logger.warning("Socket timeout, continuing...")
                continue
            except Exception as e:
                logger.error(f"Error processing data: {e}")
                time.sleep(5)
    
    def run(self):
        """Main run method"""
        logger.info("Starting DX Cluster Analyzer")
        
        if not self.load_band_config():
            return False
        
        self.setup_output_files()
        
        if not self.connect_to_cluster():
            return False
        
        self.running = True
        self.start_time = datetime.now()
        
        try:
            self.process_cluster_data()
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.running = False
            if self.socket:
                self.socket.close()
            
            # Final save
            self.save_frequency_counts()
            self.generate_summary()
            
            logger.info(f"Collection complete. Total spots: {self.total_spots}")
            logger.info(f"Files saved in: {self.output_dir}")
        
        return True

def main():
    """Main function"""
    analyzer = DXClusterAnalyzer()
    analyzer.run()

if __name__ == "__main__":
    main()
