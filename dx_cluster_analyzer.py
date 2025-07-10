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
import json
import argparse
import select
import urllib.request
import urllib.error
import html.parser
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any
import re
import sys
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dx_cluster_analyzer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BaseDXClusterHTMLParser(html.parser.HTMLParser):
    """Base HTML parser for extracting DX spots from websites"""
    
    def __init__(self):
        super().__init__()
        self.spots = []
        self.debug_output = ""  # For debugging purposes
        
    def extract_spots_from_html(self, html):
        """Extract spots from HTML content"""
        self.feed(html)
        return self.spots

class GenericDXClusterParser(BaseDXClusterHTMLParser):
    """Generic parser that attempts to find spots in any table structure"""
    
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.current_row_data = []
        self.column_data = ""
        self.all_table_data = []
        
    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
        elif self.in_table and tag == 'tr':
            self.in_row = True
            self.current_row_data = []
        elif self.in_row and (tag == 'td' or tag == 'th'):
            self.column_data = ""
    
    def handle_endtag(self, tag):
        if tag == 'table':
            self.in_table = False
            self.process_table_data()
        elif tag == 'tr':
            self.in_row = False
            if self.current_row_data:
                self.all_table_data.append(self.current_row_data)
        elif self.in_row and (tag == 'td' or tag == 'th'):
            self.current_row_data.append(self.column_data.strip())
    
    def handle_data(self, data):
        if self.in_row:
            self.column_data += data
            self.debug_output += data + "\n"
    
    def process_table_data(self):
        """Process collected table data to find spots"""
        if not self.all_table_data:
            return
        
        # Look for frequency patterns in the data
        for row in self.all_table_data:
            if not row:
                continue
                
            # Try to extract frequency and callsign from the row
            freq = None
            callsign = None
            comment = ""
            spotter = "Unknown"
            
            # Join row data for regex search
            row_text = " ".join(row)
            
            # First attempt to extract more realistic frequency formats
            # Look for common ham band frequencies like 14.195, 7.074, 3.525, etc.
            freq_match = re.search(r'(\d{1,2}\.\d{1,3})', row_text)
            if freq_match:
                try:
                    freq_text = freq_match.group(1)
                    # Convert to kHz if needed (e.g., 14.195 -> 14195.0)
                    if 1 <= float(freq_text) <= 30:  # Likely in MHz
                        freq = float(freq_text) * 1000
                    else:
                        freq = float(freq_text)
                except ValueError:
                    freq = None
            
            # If no frequency found, try looking for more general patterns
            if not freq:
                # Look for common ham band frequencies without decimal like 14195, 7074, etc.
                freq_match = re.search(r'(\d{4,5})', row_text)
                if freq_match:
                    try:
                        freq_text = freq_match.group(1)
                        freq_val = float(freq_text)
                        # Check if this is likely a frequency in kHz
                        if 1800 <= freq_val <= 29700:  # Common HF range
                            freq = freq_val
                    except ValueError:
                        freq = None
            
            # If still no frequency, try other formats
            if not freq:
                # Look for MHz format with decimal
                freq_match = re.search(r'(\d{1,2}\.\d{1,3})\s*(?:MHz|Mhz)', row_text, re.IGNORECASE)
                if freq_match:
                    try:
                        freq_text = freq_match.group(1)
                        freq = float(freq_text) * 1000  # Convert MHz to kHz
                    except ValueError:
                        freq = None
            
            # Look for callsign pattern - more specific to avoid matching frequencies
            call_match = re.search(r'([A-Z0-9]{1,3}(?:/)?[A-Z0-9]{1,2}[0-9][A-Z0-9]{1,3}(?:/[A-Z0-9]+)?)', row_text, re.IGNORECASE)
            if call_match:
                potential_callsign = call_match.group(1).upper()
                
                # Skip callsigns that are likely frequencies like 7074, 144180, etc.
                if re.match(r'^[0-9]{3,6}$', potential_callsign):
                    continue
                    
                # Skip numeric-only callsigns
                if re.match(r'^[0-9]+$', potential_callsign):
                    continue
                    
                # Valid callsign
                callsign = potential_callsign
            
            # Extract whatever might be the comment
            if len(row) > 2:
                comment = row[-1]  # Assume last column might be comment
            
            # If we found both frequency and callsign, create a spot
            if freq and callsign:
                # Check if this is a suspiciously rounded frequency (exact multiple of 1000)
                is_rounded = freq % 1000 == 0 and freq >= 1000
                
                # Map frequency to standard ham bands, but keep specific frequency
                if 1800 <= freq <= 2000:  # 160m
                    band = "160m"
                elif 3500 <= freq <= 4000:  # 80m
                    band = "80m"
                elif 5350 <= freq <= 5370:  # 60m
                    band = "60m"
                elif 7000 <= freq <= 7300:  # 40m
                    band = "40m"
                elif 10100 <= freq <= 10150:  # 30m
                    band = "30m"
                elif 14000 <= freq <= 14350:  # 20m
                    band = "20m"
                elif 18068 <= freq <= 18168:  # 17m
                    band = "17m"
                elif 21000 <= freq <= 21450:  # 15m
                    band = "15m"
                elif 24890 <= freq <= 24990:  # 12m
                    band = "12m"
                elif 28000 <= freq <= 29700:  # 10m
                    band = "10m"
                elif 50000 <= freq <= 54000:  # 6m
                    band = "6m"
                elif 144000 <= freq <= 148000:  # 2m
                    band = "2m"
                else:
                    band = "UNKNOWN"
                
                # If this is a suspiciously rounded frequency, try to look for more specific 
                # frequency in the row text
                if is_rounded:
                    # Look for more specific frequencies in the row text
                    more_specific = re.findall(r'(\d+\.\d+)', row_text)
                    for specific_freq_str in more_specific:
                        try:
                            specific_freq = float(specific_freq_str)
                            # If it's in MHz format and in the same band as our rounded freq
                            if 1 <= specific_freq <= 30:
                                specific_freq_khz = specific_freq * 1000
                                # Check if it's in the same band
                                if int(specific_freq_khz / 1000) == int(freq / 1000):
                                    # Use the more specific frequency
                                    freq = specific_freq_khz
                                    is_rounded = False
                                    break
                            # If it's already in kHz format and reasonably close to our freq
                            elif abs(specific_freq - freq) < 1000 and specific_freq % 1000 != 0:
                                freq = specific_freq
                                is_rounded = False
                                break
                        except ValueError:
                            continue
                
                # Use the best frequency we found
                actual_freq = freq
                
                # Create the spot with the correct frequency and band info
                spot = {
                    'dx_call': callsign,
                    'frequency': actual_freq,
                    'comment': comment,
                    'spotter': spotter,
                    'band': band,  # Include band info
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'time': datetime.now().strftime('%H:%M')
                }
                self.spots.append(spot)
        
        # Clear the table data for the next table
        self.all_table_data = []

class DXWatchParser(BaseDXClusterHTMLParser):
    """Specialized parser for DXWatch.com"""
    
    def __init__(self):
        super().__init__()
        self.in_spots_div = False
        self.in_spot_div = False
        self.current_spot = {}
        self.current_tag = None
        self.current_data = ""
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        # Look for the main spots container
        if tag == 'div' and 'id' in attrs_dict and attrs_dict['id'] == 'spots':
            self.in_spots_div = True
        
        # Look for individual spot divs
        elif self.in_spots_div and tag == 'div' and 'class' in attrs_dict and 'spot' in attrs_dict['class']:
            self.in_spot_div = True
            self.current_spot = {}
        
        # Track specific elements within a spot
        if self.in_spot_div:
            self.current_tag = tag
            
            # Look for frequency in a specific span
            if tag == 'span' and 'class' in attrs_dict and 'freq' in attrs_dict['class']:
                self.current_data = ""
            
            # Look for callsign in a specific element
            elif tag == 'a' and 'class' in attrs_dict and 'call' in attrs_dict['class']:
                self.current_data = ""
    
    def handle_endtag(self, tag):
        if tag == 'div' and self.in_spots_div and not self.in_spot_div:
            self.in_spots_div = False
        
        elif tag == 'div' and self.in_spot_div:
            self.in_spot_div = False
            # Add the spot if we have enough data
            if 'dx_call' in self.current_spot and 'frequency' in self.current_spot:
                self.spots.append(self.current_spot.copy())
        
        # Process data based on the tag that's ending
        if self.in_spot_div and tag == self.current_tag:
            if tag == 'span' and 'freq' in self.current_data:
                try:
                    # Extract the frequency
                    freq_match = re.search(r'(\d+\.?\d*)', self.current_data)
                    if freq_match:
                        self.current_spot['frequency'] = float(freq_match.group(1))
                except ValueError:
                    pass
            
            elif tag == 'a' and self.current_data.strip():
                self.current_spot['dx_call'] = self.current_data.strip()
            
            self.current_tag = None
    
    def handle_data(self, data):
        if self.in_spot_div and self.current_tag:
            self.current_data += data
            self.debug_output += f"{self.current_tag}: {data}\n"

class HamQTHParser(BaseDXClusterHTMLParser):
    """Specialized parser for HamQTH.com"""
    
    def __init__(self):
        super().__init__()
        self.in_dx_table = False
        self.in_row = False
        self.current_column = 0
        self.current_spot = {}
    
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        # Look for the DX cluster table
        if tag == 'table' and 'id' in attrs_dict and attrs_dict['id'] == 'dxc-table':
            self.in_dx_table = True
        
        # Track rows in the table
        elif self.in_dx_table and tag == 'tr':
            self.in_row = True
            self.current_column = 0
            self.current_spot = {}
        
        # Count columns
        elif self.in_row and tag == 'td':
            self.current_column += 1
    
    def handle_endtag(self, tag):
        if tag == 'table' and self.in_dx_table:
            self.in_dx_table = False
        
        elif tag == 'tr' and self.in_row:
            self.in_row = False
            # Add the spot if it has the minimum required data
            if 'dx_call' in self.current_spot and 'frequency' in self.current_spot:
                self.spots.append(self.current_spot.copy())
    
    def handle_data(self, data):
        if not self.in_row:
            return
        
        data = data.strip()
        if not data:
            return
            
        # Extract information based on column position
        if self.current_column == 1:  # Frequency
            try:
                self.current_spot['frequency'] = float(data)
            except ValueError:
                pass
        elif self.current_column == 2:  # DX Call
            self.current_spot['dx_call'] = data
        elif self.current_column == 3:  # Comment
            self.current_spot['comment'] = data
        elif self.current_column == 4:  # Time
            self.current_spot['time'] = data
        elif self.current_column == 5:  # Spotter
            self.current_spot['spotter'] = data

class DXClusterAnalyzer:
    def __init__(self, config_file: str = "band_config.csv", 
                 output_dir: str = "dx_data", 
                 max_size_gb: float = 500.0,
                 callsign: str = None,
                 use_web_source: bool = False):
        self.config_file = config_file
        self.output_dir = output_dir
        self.max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        self.running = False
        self.start_time = None
        self.band_configs = []
        self.raw_data_file = None
        self.processed_data_file = None
        self.summary_file = None
        self.config_dir = os.path.join(os.path.expanduser("~"), ".dx_cluster_analyzer")
        self.config_path = os.path.join(self.config_dir, "config.json")
        
        # Store web source flag
        self.use_web_source = use_web_source
        
        # Ensure config directory exists
        os.makedirs(self.config_dir, exist_ok=True)
        
        # DX Cluster connection settings
        self.cluster_host = "cluster.dxwatch.com"  # Primary cluster
        self.cluster_port = 8000
        self.socket = None
        self.callsign = callsign or self.load_callsign() or "ANALYZER"
        self.consecutive_disconnections = 0  # Counter for consecutive disconnections
        
        # Backup clusters if primary fails - these are known to be reliable
        self.backup_clusters = [
            ("dxc.w1nr.net", 8000),
            ("dxc.ve7cc.net", 23),
            ("dxspots.com", 8000),
            ("cluster-eu-is.com", 7300),  # Include the previous default as backup
            ("arcluster.net", 7373)
        ]
        # Current backup cluster index
        self.current_backup_index = 0
        
        # Data storage
        self.frequency_counts = defaultdict(lambda: defaultdict(int))
        self.total_spots = 0
        self.raw_data_buffer = []  # Buffer for raw data to batch write
        self.buffer_size = 10  # Number of spots to buffer before writing
        
        # Spot cache to prevent duplicates (callsign_freq → timestamp)
        self.spot_cache = {}
        self.cache_expiry = 3600  # Seconds to keep spots in cache (1 hour)
        self.last_cache_cleanup = time.time()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
    def load_callsign(self) -> Optional[str]:
        """Load callsign from configuration file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    return config.get('callsign')
            return None
        except Exception as e:
            logger.error(f"Error loading callsign configuration: {e}")
            return None
    
    def save_callsign(self, callsign: str) -> bool:
        """Save callsign to configuration file"""
        try:
            config = {}
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    try:
                        config = json.load(f)
                    except:
                        config = {}
            
            config['callsign'] = callsign
            self.callsign = callsign
            
            with open(self.config_path, 'w') as f:
                json.dump(config, f)
            
            logger.info(f"Callsign '{callsign}' saved to configuration")
            return True
        except Exception as e:
            logger.error(f"Error saving callsign configuration: {e}")
            return False
        
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
        """Setup output CSV files with fixed filenames"""
        # Raw data file
        self.raw_data_file = os.path.join(self.output_dir, "raw_spots.csv")
        with open(self.raw_data_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Frequency', 'Callsign', 'Spotter', 'Mode', 'Band', 'Region'])
        
        # Processed data file
        self.processed_data_file = os.path.join(self.output_dir, "frequency_counts.csv")
        
        # Summary file
        self.summary_file = os.path.join(self.output_dir, "summary.csv")
        
        logger.info(f"Output files created: {self.raw_data_file}")
    
    def connect_to_cluster(self) -> bool:
        """Connect to DX cluster with fallback to backup clusters if primary fails"""
        # First try the primary cluster
        if self._try_connect(self.cluster_host, self.cluster_port):
            return True
            
        # If primary fails, try backup clusters
        logger.warning(f"Primary cluster {self.cluster_host}:{self.cluster_port} failed, trying backup clusters...")
        
        for host, port in self.backup_clusters:
            logger.info(f"Trying backup cluster: {host}:{port}")
            if self._try_connect(host, port):
                # Update primary to this successful one for future reconnections
                self.cluster_host = host
                self.cluster_port = port
                logger.info(f"Using backup cluster {host}:{port} as new primary")
                return True
        
        logger.error("All clusters failed to connect")
        return False
        
    def _try_connect(self, host: str, port: int) -> bool:
        """Try to connect to a specific cluster with advanced login handling"""
        try:
            # First check if we can resolve the hostname
            try:
                socket.gethostbyname(host)
            except socket.gaierror as e:
                logger.error(f"DNS resolution failed for {host}: {e}")
                return False
                
            # Now try the actual connection
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                    
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)  # Shorter timeout for more responsive login handling
            self.socket.connect((host, port))
            
            # Advanced login handling - wait for login prompts
            login_successful = False
            start_time = time.time()
            timeout = 30  # Max time to attempt login
            
            while time.time() - start_time < timeout and not login_successful:
                try:
                    data = self.socket.recv(1024).decode('utf-8', errors='ignore')
                    if not data:
                        time.sleep(0.5)
                        continue
                        
                    logger.info(f"Cluster: {data.strip()}")
                    
                    # Check for various login prompts
                    login_prompts = [
                        "enter your call", "login", "callsign",
                        "user", "please enter", "identify"
                    ]
                    
                    if any(prompt.lower() in data.lower() for prompt in login_prompts):
                        logger.info(f"Detected login prompt, sending callsign: {self.callsign}")
                        self.socket.send(f"{self.callsign}\r\n".encode())
                    
                    # Check for successful login indicators
                    success_indicators = [
                        "welcome", "connected", "logged in", "hello",
                        "spots for you", "commands"
                    ]
                    
                    if any(indicator.lower() in data.lower() for indicator in success_indicators):
                        login_successful = True
                        logger.info(f"Successfully logged in to {host}:{port}")
                        break
                        
                except socket.timeout:
                    # Send callsign even if no prompt - some clusters don't prompt
                    logger.info(f"No prompt received, sending callsign: {self.callsign}")
                    self.socket.send(f"{self.callsign}\r\n".encode())
                    time.sleep(1)
            
            # If we didn't see success indicators, assume login worked if we got this far
            if not login_successful:
                logger.info(f"No confirmation of login success, continuing anyway")
            
            logger.info(f"Connected to DX cluster: {host}:{port}")
            
            # Enable Skimmer spots
            logger.info("Enabling Skimmer spots with SET/SKIMMER command")
            try:
                self.socket.send("SET/SKIMMER\r\n".encode())
                time.sleep(1)  # Give the cluster time to process the command
            except Exception as e:
                logger.warning(f"Failed to send SET/SKIMMER command: {e}")
            
            # Show last 100 spots
            logger.info("Requesting last 100 spots with sh/dx 100 command")
            try:
                self.socket.send("sh/dx 100\r\n".encode())
                time.sleep(1)  # Give the cluster time to process the command
            except Exception as e:
                logger.warning(f"Failed to send sh/dx 100 command: {e}")
                
            # Set a longer timeout for normal operation
            self.socket.settimeout(60)
            return True
            
        except socket.gaierror as e:
            logger.error(f"DNS resolution failed for {host}: {e}")
            return False
        except socket.timeout:
            logger.error(f"Connection timeout to {host}:{port}")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {host}:{port}: {e}")
            return False
    
    def parse_dx_spot(self, line: str) -> Tuple[str, str, str, str, float]:
        """Parse DX spot line and extract relevant information"""
        # DX spot format: DX de CALL: freq DX_CALL comment time
        # Example: DX de ON4KST: 14205.0 JA1ABC CQ                1200Z
        
        # More flexible regex pattern to handle variations in DX cluster formats
        spot_pattern = r'DX\s+de\s+([\w\d/]+)(?::|,)?\s+(\d+\.?\d*)\s+([\w\d/]+)\s+(.+?)(?:\s+(\d{3,4}Z))?$'
        match = re.search(spot_pattern, line.strip())
        
        if match:
            spotter = match.group(1)
            try:
                frequency = float(match.group(2))
            except ValueError:
                return None, None, None, None, 0.0
                
            dx_call = match.group(3)
            comment = match.group(4).strip()
            time_str = match.group(5) if match.group(5) else "0000Z"  # Default time if not provided
            
            return spotter, dx_call, comment, time_str, frequency
        
        # Try alternative formats that might be used by different clusters
        alt_patterns = [
            r'(\w+)\s+spots\s+([\w\d/]+)\s+(?:on|at)\s+(\d+\.?\d*)\s+(?:MHz|kHz)?\s+(.+?)(?:\s+(\d{3,4}Z))?$',
            r'Spot:\s+(\w+)\s+(\d+\.?\d*)\s+([\w\d/]+)\s+(.+)'
        ]
        
        for pattern in alt_patterns:
            match = re.search(pattern, line.strip())
            if match:
                # Extract based on pattern
                if pattern.startswith(r'(\w+)\s+spots'):
                    spotter = match.group(1)
                    dx_call = match.group(2)
                    try:
                        frequency = float(match.group(3))
                    except ValueError:
                        continue
                    comment = match.group(4).strip()
                    time_str = match.group(5) if len(match.groups()) >= 5 and match.group(5) else "0000Z"
                else:
                    spotter = "Unknown"
                    try:
                        frequency = float(match.group(2))
                    except (ValueError, IndexError):
                        continue
                    dx_call = match.group(3) if len(match.groups()) >= 3 else "Unknown"
                    comment = match.group(4) if len(match.groups()) >= 4 else ""
                    time_str = "0000Z"
                
                return spotter, dx_call, comment, time_str, frequency
        
        return None, None, None, None, 0.0
    
    def determine_mode_and_band(self, frequency: float, comment: str) -> Tuple[str, str, str]:
        """Determine mode and band based on frequency and comment"""
        mode = "UNKNOWN"
        band = "UNKNOWN"
        region = "UNKNOWN"
        
        # First try to determine mode and band by frequency from our configuration
        for config in self.band_configs:
            start_freq = float(config['StartFreq'])
            end_freq = float(config['EndFreq'])
            if start_freq <= frequency <= end_freq:
                mode = config['Mode']
                band = config['Band']
                region = config['Region']
                break
                
        # Then check comment for explicit mode indicators (these override frequency-based detection)
        comment_upper = comment.upper()
        
        # More precise mode detection with word boundaries and avoiding callsigns
        cw_indicators = [r'\bCW\b', r'\bQRS\b', r'\bMORSE\b']
        ssb_indicators = [r'\bSSB\b', r'\bLSB\b', r'\bUSB\b', r'\bPHONE\b']
        digital_indicators = [r'\bFT8\b', r'\bFT4\b', r'\bPSK\b', r'\bRTTY\b', r'\bDIGITAL\b']
        
        # Check for CW mode
        if any(re.search(pattern, comment_upper) for pattern in cw_indicators):
            mode = "CW"
        # Check for SSB mode
        elif any(re.search(pattern, comment_upper) for pattern in ssb_indicators):
            mode = "SSB"
        # Check for digital modes (we'll filter these out)
        elif any(re.search(pattern, comment_upper) for pattern in digital_indicators):
            mode = "DIGITAL"
        
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
    
    def flush_raw_data_buffer(self):
        """Write buffered data to the raw data file"""
        if not self.raw_data_buffer:
            return
            
        try:
            with open(self.raw_data_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(self.raw_data_buffer)
            self.raw_data_buffer = []
        except Exception as e:
            logger.error(f"Error writing raw data buffer: {e}")
    
    def send_keepalive(self) -> bool:
        """Send a keepalive message to the cluster to maintain the connection"""
        try:
            # Some clusters accept these commands as keepalives
            keepalive_commands = [
                "sh/dx",    # Show last few spots
                "sh/u",     # Show users
                "",         # Empty line
                "\r",       # Carriage return
            ]
            
            # Rotate through the commands
            command = keepalive_commands[int(time.time() / 30) % len(keepalive_commands)]
            
            if command:
                logger.debug(f"Sending keepalive: {command}")
                self.socket.send(f"{command}\r\n".encode())
            
            return True
        except Exception as e:
            logger.error(f"Keepalive failed: {e}")
            return False
            
    def process_cluster_data(self):
        """Main loop to process cluster data"""
        logger.info("Starting data collection...")
        reconnect_delay = 1  # Initial reconnect delay in seconds
        max_reconnect_delay = 300  # Maximum reconnect delay (5 minutes)
        last_keepalive = time.time()
        last_data_time = time.time()
        keepalive_interval = 30  # Send keepalive every 30 seconds if no data
        
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
                
                # Check if we need to send a keepalive
                current_time = time.time()
                if current_time - last_keepalive > keepalive_interval:
                    if self.send_keepalive():
                        last_keepalive = current_time
                
                # Set socket to non-blocking to prevent hanging
                self.socket.setblocking(0)
                
                try:
                    # Read data from cluster (non-blocking)
                    ready = select.select([self.socket], [], [], 1)
                    if ready[0]:
                        data = self.socket.recv(4096).decode('utf-8', errors='ignore')
                        if data:
                            last_data_time = time.time()
                            # Process the received data
                            lines = data.strip().split('\n')
                            for line in lines:
                                # Print all received lines in debug mode to diagnose spot format
                                if len(line.strip()) > 0:
                                    logger.debug(f"Received line: {line}")
                                
                                # Check for various spot formats (not just 'DX de')
                                if (line.startswith('DX de ') or
                                    re.match(r'^\s*\d+\.\d+\s+\w+', line) or
                                    re.search(r'\d+\.\d+\s+\w+', line)):
                                    
                                    # Try to parse the spot
                                    try:
                                        # Standard DX de format
                                        if line.startswith('DX de '):
                                            spotter, dx_call, comment, time_str, frequency = self.parse_dx_spot(line)
                                        # Frequency first format (common in sh/dx output)
                                        elif re.match(r'^\s*\d+\.\d+\s+\w+', line):
                                            # Parse format like: "14025.0 DL0WU        CQ at 1023Z"
                                            parts = line.strip().split()
                                            if len(parts) >= 2:
                                                try:
                                                    frequency = float(parts[0])
                                                    dx_call = parts[1]
                                                    comment = " ".join(parts[2:]) if len(parts) > 2 else ""
                                                    time_match = re.search(r'(\d{4}Z)', comment)
                                                    time_str = time_match.group(1) if time_match else "0000Z"
                                                    spotter = "Unknown"  # Spotter may not be in this format
                                                except ValueError:
                                                    frequency = 0.0
                                            else:
                                                frequency = 0.0
                                        else:
                                            # Try to extract a frequency and callsign from anywhere in the line
                                            # Look for common ham frequency patterns: 14.195, 7.074, 3.525 MHz
                                            freq_match = re.search(r'(\d+\.\d+)', line)
                                            # Also look for frequencies like 14195, 7074 in kHz
                                            alt_freq_match = re.search(r'\b(\d{4,5})\b', line)
                                            
                                            call_match = re.search(r'([A-Z0-9]{1,3}/)?[A-Z0-9]{1,2}[0-9][A-Z0-9]{1,3}(/[A-Z0-9]+)?', line)
                                            
                                            if freq_match and call_match:
                                                frequency = float(freq_match.group(1))
                                                # Convert to kHz if it's in MHz format (e.g., 14.195 -> 14195.0)
                                                if frequency < 30:  # Likely in MHz
                                                    frequency *= 1000
                                                dx_call = call_match.group(0)
                                                comment = line
                                                time_str = "0000Z"
                                                spotter = "Unknown"
                                            elif alt_freq_match and call_match:
                                                # Found a frequency like 14195 (already in kHz)
                                                frequency = float(alt_freq_match.group(1))
                                                dx_call = call_match.group(0)
                                                comment = line
                                                time_str = "0000Z"
                                                spotter = "Unknown"
                                            else:
                                                frequency = 0.0
                                    except Exception as e:
                                        logger.debug(f"Error parsing spot: {e}")
                                        logger.debug(f"Line was: {line}")
                                        frequency = 0.0
                                    
                                    if frequency > 0:
                                        mode, band, region = self.determine_mode_and_band(frequency, comment)
                                        
                                        # Show all spots for debugging regardless of filter
                                        print(f"\n----- DX SPOT FOUND -----")
                                        print(f"DX Call: {dx_call} on {frequency} kHz ({band}) - {mode}")
                                        print(f"Comment: {comment}")
                                        print(f"Spotted by: {spotter} at {time_str}")
                                        print(f"------------------------\n")
                                        
                                        if self.should_include_spot(frequency, mode):
                                            # Add to buffer instead of writing immediately
                                            timestamp = datetime.now().isoformat()
                                            self.raw_data_buffer.append(
                                                [timestamp, frequency, dx_call, spotter, mode, band, region]
                                            )
                                            
                                            # Update counts
                                            self.frequency_counts[frequency][mode] += 1
                                            self.total_spots += 1
                                            
                                            if self.total_spots % 100 == 0:
                                                logger.info(f"Processed {self.total_spots} spots")
                        else:
                            # Empty data means disconnection
                            self.consecutive_disconnections += 1
                            logger.warning(f"Connection closed by cluster, reconnecting... (attempt {self.consecutive_disconnections}/10)")
                            
                            # After 10 consecutive disconnections, try a different cluster
                            if self.consecutive_disconnections >= 10:
                                logger.warning("10 consecutive disconnections reached, trying a different cluster")
                                self.consecutive_disconnections = 0  # Reset counter
                                
                                # Get next backup cluster
                                self.current_backup_index = (self.current_backup_index + 1) % len(self.backup_clusters)
                                new_host, new_port = self.backup_clusters[self.current_backup_index]
                                
                                logger.info(f"Switching to backup cluster: {new_host}:{new_port}")
                                if self._try_connect(new_host, new_port):
                                    # Update primary cluster for future reconnections
                                    self.cluster_host = new_host
                                    self.cluster_port = new_port
                                    logger.info(f"Using backup cluster {new_host}:{new_port} as new primary")
                                    continue
                                else:
                                    # If failed, try the regular connect_to_cluster which will try all
                                    if not self.connect_to_cluster():
                                        continue
                            else:
                                # Regular reconnection logic
                                time.sleep(reconnect_delay)
                                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                                if not self.connect_to_cluster():
                                    continue
                    else:
                        # No data available, but connection still open
                        # Check if we've been waiting too long without data
                        if time.time() - last_data_time > 120:  # 2 minutes with no data
                            logger.warning("No data received for 2 minutes, sending keepalive...")
                            if not self.send_keepalive():
                                # Keepalive failed, try to reconnect
                                logger.warning("Keepalive failed, reconnecting...")
                                time.sleep(reconnect_delay)
                                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                                if not self.connect_to_cluster():
                                    continue
                            last_data_time = time.time()  # Reset the timer
                        
                        # Continue the loop without processing
                        continue
                except (socket.error, select.error) as e:
                    logger.error(f"Socket error: {e}")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    if not self.connect_to_cluster():
                        continue
                    continue
                
                # Restore blocking mode for other operations
                self.socket.setblocking(1)
                
                # Successful connection, reset backoff delay
                reconnect_delay = 1
                
                # Flush buffer if it reaches the buffer size
                if len(self.raw_data_buffer) >= self.buffer_size:
                    self.flush_raw_data_buffer()
                
                # Save processed data every 1000 spots
                if self.total_spots % 1000 == 0 and self.total_spots > 0:
                    self.save_frequency_counts()
                    self.generate_summary()
                
                time.sleep(0.1)  # Small delay to prevent overwhelming
                
            except socket.timeout:
                logger.warning("Socket timeout, reconnecting...")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)  # Exponential backoff
                continue
            except Exception as e:
                logger.error(f"Error processing data: {e}")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)  # Exponential backoff
    
    def fetch_web_data(self) -> List[Dict]:
        """Fetch DX spots from various DX cluster websites"""
        # URLs to try in order with their appropriate parsers
        urls_with_parsers = [
            ("https://www.hamqth.com/dxc.php", HamQTHParser),
            ("https://www.dxwatch.com/", DXWatchParser),
            ("http://www.dxsummit.fi/", GenericDXClusterParser),
            ("http://www.dx-cluster.de", GenericDXClusterParser),
            ("https://www.dx-cluster.de", GenericDXClusterParser),
            ("http://dx-cluster.de", GenericDXClusterParser),
            ("https://dx-cluster.de", GenericDXClusterParser)
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Import SSL once to avoid repeated imports
        import ssl
        context = ssl._create_unverified_context() if hasattr(ssl, '_create_unverified_context') else None
        
        # Try each URL with its specialized parser
        for url, parser_class in urls_with_parsers:
            try:
                logger.info(f"Trying to fetch DX spots from: {url}")
                req = urllib.request.Request(url, headers=headers)
                
                # Use the context when opening the URL
                if context and url.startswith('https'):
                    with urllib.request.urlopen(req, context=context, timeout=10) as response:
                        html = response.read().decode('utf-8', errors='ignore')
                else:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        html = response.read().decode('utf-8', errors='ignore')
                
                # Create and use the appropriate parser for this URL
                parser = parser_class()
                spots = parser.extract_spots_from_html(html)
                
                # If we found spots, log success and return them
                if spots:
                    logger.info(f"Successfully fetched {len(spots)} spots from {url} using {parser_class.__name__}")
                    return spots
                else:
                    logger.warning(f"No spots found in HTML from {url} using {parser_class.__name__}")
                    # Print the first 200 chars of HTML for debugging
                    logger.debug(f"HTML sample: {html[:200]}...")
                    logger.debug(f"Parser debug output: {parser.debug_output[:500]}")
            
            except Exception as e:
                logger.error(f"Error fetching from {url}: {e}")
                continue  # Try next URL
        
        # If we get here, all URLs failed
        logger.error("All URLs failed, could not fetch DX spots")
        return []
    
    def clean_spot_cache(self):
        """Clean up old entries from the spot cache to prevent memory growth"""
        current_time = time.time()
        # Only clean if it's been at least 5 minutes since last cleanup
        if current_time - self.last_cache_cleanup < 300:
            return

        # Remove entries older than cache_expiry seconds
        expired_keys = []
        for key, timestamp in self.spot_cache.items():
            if current_time - timestamp > self.cache_expiry:
                expired_keys.append(key)
        
        # Remove expired entries
        for key in expired_keys:
            del self.spot_cache[key]
        
        logger.debug(f"Cleaned {len(expired_keys)} expired entries from spot cache. Cache size: {len(self.spot_cache)}")
        self.last_cache_cleanup = current_time
    
    def process_web_data(self, spots: List[Dict]):
        """Process DX spots from web data"""
        if not spots:
            return
        
        # Clean cache if needed
        self.clean_spot_cache()
        
        # Count new spots added in this batch
        new_spots = 0
            
        for spot in spots:
            try:
                # Extract data from the spot dictionary
                frequency = spot.get('frequency', 0.0)
                dx_call = spot.get('dx_call', '')
                comment = spot.get('comment', '')
                spotter = spot.get('spotter', 'Unknown')
                
                # Handle date and time fields according to dx-cluster.de format
                date_str = spot.get('date', '')
                time_str = spot.get('time', '')
                
                # Create a combined datetime string for display
                datetime_str = f"{date_str} {time_str}"
                
                if frequency > 0 and dx_call:
                    # Check if this is a suspiciously rounded frequency (exact multiple of 1000)
                    is_rounded = frequency % 1000 == 0 and frequency >= 1000
                    
                    # If this is a suspiciously rounded frequency and we have comment text,
                    # try to extract a more specific frequency from the comment
                    if is_rounded and comment:
                        # Look for decimal frequencies in the comment (more specific)
                        more_specific = re.findall(r'(\d+\.\d+)', comment)
                        for specific_freq_str in more_specific:
                            try:
                                specific_freq = float(specific_freq_str)
                                # If it's in MHz format and in the same band as our rounded freq
                                if 1 <= specific_freq <= 30:
                                    specific_freq_khz = specific_freq * 1000
                                    # Check if it's in the same band (same thousands digit)
                                    if int(specific_freq_khz / 1000) == int(frequency / 1000):
                                        # Use the more specific frequency
                                        frequency = specific_freq_khz
                                        is_rounded = False
                                        break
                                # If it's already in kHz format and reasonably close to our freq
                                elif abs(specific_freq - frequency) < 1000 and specific_freq % 1000 != 0:
                                    frequency = specific_freq
                                    is_rounded = False
                                    break
                            except ValueError:
                                continue
                    
                    # Generate a unique cache key for this spot
                    cache_key = f"{dx_call}_{frequency}"
                    
                    # Check if this spot is already in the cache (to prevent duplicates)
                    current_time = time.time()
                    if cache_key in self.spot_cache and current_time - self.spot_cache[cache_key] < 600:
                        # Skip this spot if it was seen in the last 10 minutes
                        continue
                    
                    # Add/update this spot in the cache
                    self.spot_cache[cache_key] = current_time
                    
                    mode, band, region = self.determine_mode_and_band(frequency, comment)
                    
                    # Show all spots for debugging regardless of filter
                    print(f"\n----- DX SPOT FOUND (Web) -----")
                    print(f"DX Call: {dx_call} on {frequency} kHz ({band}) - {mode}")
                    print(f"Comment: {comment}")
                    print(f"Spotted by: {spotter} at {datetime_str}")
                    print(f"------------------------\n")
                    
                    if self.should_include_spot(frequency, mode):
                        # Add to buffer instead of writing immediately
                        timestamp = datetime.now().isoformat()
                        self.raw_data_buffer.append(
                            [timestamp, frequency, dx_call, spotter, mode, band, region]
                        )
                        
                        # Update counts
                        self.frequency_counts[frequency][mode] += 1
                        self.total_spots += 1
                        new_spots += 1
                        
                        if self.total_spots % 100 == 0:
                            logger.info(f"Processed {self.total_spots} spots")
            except Exception as e:
                logger.error(f"Error processing web spot: {e}")
                
        if new_spots > 0:
            logger.info(f"Added {new_spots} new spots in this batch. Total spots: {self.total_spots}")
    
    def process_web_mode(self):
        """Process data from dx-cluster.de website in a polling loop"""
        logger.info("Starting web data collection from dx-cluster.de (updates every 10 seconds)")
        
        poll_interval = 10  # seconds between polls
        last_poll_time = 0
        
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
                
                current_time = time.time()
                
                # Check if it's time to poll
                if current_time - last_poll_time >= poll_interval:
                    # Fetch and process web data
                    spots = self.fetch_web_data()
                    if spots:
                        logger.info(f"Fetched {len(spots)} spots from dx-cluster.de")
                        self.process_web_data(spots)
                    else:
                        logger.warning("No spots found from dx-cluster.de")
                    
                    last_poll_time = current_time
                    
                    # Flush buffer if needed
                    if len(self.raw_data_buffer) >= self.buffer_size:
                        self.flush_raw_data_buffer()
                    
                    # Save processed data every 1000 spots
                    if self.total_spots % 1000 == 0 and self.total_spots > 0:
                        self.save_frequency_counts()
                        self.generate_summary()
                
                # Sleep a bit to prevent excessive CPU usage
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in web processing: {e}")
                time.sleep(5)  # Wait a bit longer on error
    
    def run(self):
        """Main run method"""
        logger.info("Starting DX Cluster Analyzer")
        
        if not self.load_band_config():
            return False
        
        self.setup_output_files()
        
        self.running = True
        self.start_time = datetime.now()
        
        try:
            if self.use_web_source:
                # Use dx-cluster.de website as data source
                logger.info("Using dx-cluster.de website as data source")
                self.process_web_mode()
            else:
                # Use traditional cluster connection
                if not self.connect_to_cluster():
                    return False
                self.process_cluster_data()
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.running = False
            if self.socket:
                self.socket.close()
            
            # Flush any remaining data in the buffer
            self.flush_raw_data_buffer()
            
            # Final save
            self.save_frequency_counts()
            self.generate_summary()
            
            logger.info(f"Collection complete. Total spots: {self.total_spots}")
            logger.info(f"Files saved in: {self.output_dir}")
        
        return True

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="DX Cluster Frequency Analyzer - Collects and analyzes spot data from DX clusters"
    )
    parser.add_argument(
        "--callsign", "-c", 
        help="Your amateur radio callsign (will be saved for future use, optional when using --web)"
    )
    parser.add_argument(
        "--config", 
        default="band_config.csv",
        help="Path to band configuration file (default: band_config.csv)"
    )
    parser.add_argument(
        "--output", "-o", 
        default="dx_data",
        help="Output directory for data files (default: dx_data)"
    )
    parser.add_argument(
        "--cluster", 
        default="cluster.dxwatch.com:8000",
        help="DX cluster host:port (default: cluster.dxwatch.com:8000)"
    )
    parser.add_argument(
        "--maxsize", 
        type=float, 
        default=500.0,
        help="Maximum size in GB for data collection (default: 500.0)"
    )
    parser.add_argument(
        "--noskimmer",
        action="store_true",
        help="Disable the SET/SKIMMER command (use if it causes connection issues)"
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Use dx-cluster.de website as data source instead of direct cluster connection"
    )
    
    args = parser.parse_args()
    
    # Create analyzer with command line arguments
    analyzer = DXClusterAnalyzer(
        config_file=args.config,
        output_dir=args.output,
        max_size_gb=args.maxsize,
        callsign=args.callsign,
        use_web_source=args.web
    )
    
    # Only handle callsign if not using web mode or if it was explicitly provided
    if args.web:
        # Callsign not needed for web mode
        logger.info("Using web mode - callsign not required")
    elif args.callsign:
        analyzer.save_callsign(args.callsign)
        logger.info(f"Using callsign: {args.callsign} (saved for future use)")
    elif analyzer.callsign != "ANALYZER":
        logger.info(f"Using previously saved callsign: {analyzer.callsign}")
    else:
        logger.warning("Using default callsign 'ANALYZER'. Set your callsign with --callsign")
    
    # Set cluster host:port if provided
    if args.cluster and ":" in args.cluster:
        host, port_str = args.cluster.split(":")
        try:
            port = int(port_str)
            analyzer.cluster_host = host
            analyzer.cluster_port = port
        except ValueError:
            logger.error(f"Invalid port in cluster address: {args.cluster}")
    
    # Run the analyzer
    analyzer.run()

if __name__ == "__main__":
    main()
