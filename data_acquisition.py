import time
import threading
from lob_streamer import BinanceLOBStreamer  # Assuming the class is saved in `lob_streamer.py`

# Create the streamer
depth_levels = 20
duration = 60*60*5 # s
symbols = ["btcusdt", "ethusdt", "bnbusdt", "xrpusdt", "ltcusdt", "adausdt", "solusdt", "dogeusdt", "bchusdt", "dotusdt"]

lob_streamer = BinanceLOBStreamer(symbols=symbols, depth_levels=depth_levels, update_interval=100)

# Start streaming
lob_streamer.start_stream()

# Run for 30 seconds
time.sleep(duration)

# Stop the stream and save the data
lob_streamer.stop_stream()

lob_streamer.save_to_csv()
