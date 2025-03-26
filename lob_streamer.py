import websocket
import json
import pandas as pd
import threading
import time

import websocket
import json
import pandas as pd
import threading
import time


import websocket
import json
import pandas as pd
import threading
import time

class BinanceLOBStreamer:
    def __init__(self, symbols=["btcusdt"], depth_levels=10, update_interval=100, save_interval=10, max_entries=1_000_000):
        """
        Binance Limit Order Book (LOB) Streamer for multiple symbols with returns.

        :param symbols: List of trading pairs (e.g., ["btcusdt", "ethusdt"]).
        :param depth_levels: Number of order book levels (default 10).
        :param update_interval: WebSocket update interval in ms (default 100ms).
        :param save_interval: Time in seconds to save LOB data to CSV.
        :param max_entries: Maximum number of LOB entries to keep in memory.
        """
        self.symbols = [symbol.lower() for symbol in symbols]
        self.depth_levels = depth_levels
        self.update_interval = update_interval
        self.save_interval = save_interval
        self.max_entries = max_entries
        self.lob_data = {symbol: [] for symbol in self.symbols}  # Create a dictionary to store data for each symbol
        self.last_mid_price = {symbol: None for symbol in self.symbols}  # Store the last midpoint price
        self.ws = None
        self.thread = None

    def _on_message(self, ws, message):
        """Handles incoming WebSocket messages."""
        data = json.loads(message)
        timestamp = pd.Timestamp.now()

        if "result" in data:  # Ignore initial connection response
            return

        symbol = data['stream'].split('@')[0]

        # Extract top bid and ask levels
        bids = data["data"]["bids"][:self.depth_levels]
        asks = data["data"]["asks"][:self.depth_levels]

        # Calculate the midpoint price (average of best bid and ask)
        bid_price = float(bids[0][0])
        ask_price = float(asks[0][0])
        mid_price = (bid_price + ask_price) / 2

        # Calculate return as percentage change in midpoint price
        if self.last_mid_price[symbol] is not None:
            price_return = (mid_price - self.last_mid_price[symbol]) / self.last_mid_price[symbol] * 100
        else:
            price_return = 0.0  # No return for the first data point

        # Update last_mid_price for the symbol
        self.last_mid_price[symbol] = mid_price

        # Flatten order book into a dictionary
        row = {"timestamp": timestamp, "mid_price": mid_price, "return": price_return}
        for i, (bid_price, bid_vol) in enumerate(bids):
            row[f"bid_price_{i + 1}"] = float(bid_price)
            row[f"bid_vol_{i + 1}"] = float(bid_vol)
        for i, (ask_price, ask_vol) in enumerate(asks):
            row[f"ask_price_{i + 1}"] = float(ask_price)
            row[f"ask_vol_{i + 1}"] = float(ask_vol)

        # Store the row, maintaining max_entries limit for the symbol
        self.lob_data[symbol].append(row)
        if len(self.lob_data[symbol]) > self.max_entries:
            self.lob_data[symbol].pop(0)

    def _on_error(self, ws, error):
        """Handles WebSocket errors."""
        print(f"WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handles WebSocket closure."""
        print("WebSocket closed")

    def unsubscribe(self):
        """Unsubscribes from the Binance order book stream for all symbols."""
        if self.ws:
            params = [f"{symbol}@depth{self.depth_levels}@{self.update_interval}ms" for symbol in self.symbols]
            payload = {
                "method": "UNSUBSCRIBE",
                "params": params,
                "id": 1
            }
            self.ws.send(json.dumps(payload))
            print(f"Unsubscribed from {', '.join(self.symbols).upper()} LOB updates!")

    def _on_open(self, ws):
        """Subscribes to multiple Binance order book updates."""
        params = [f"{symbol}@depth{self.depth_levels}@{self.update_interval}ms" for symbol in self.symbols]
        payload = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1
        }
        ws.send(json.dumps(payload))
        print(f"Subscribed to {', '.join(self.symbols).upper()} LOB updates!")

    def start_stream(self):
        """Starts the WebSocket in a separate thread."""
        self.ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/stream",
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self.ws.on_open = self._on_open

        # Run WebSocket in a separate thread
        self.thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self.thread.start()
        print("LOB streamer started!")

    def stop_stream(self):
        """Stops the WebSocket stream."""
        if self.ws:
            # Unsubscribe from the stream
            self.unsubscribe()
            # Close the WebSocket connection
            self.ws.close()
            print("LOB streamer stopped!")

    def save_to_csv(self, filename="crypto_lob_data.csv"):
        """Saves the LOB snapshots to a CSV file periodically."""
        for symbol in self.symbols:
            if self.lob_data[symbol]:
                df = pd.DataFrame(self.lob_data[symbol])
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                df.to_csv(f"data/{symbol}_{self.depth_levels}depth_{timestamp}.csv", index=False)
                print(f"Saved {len(df)} LOB snapshots for {symbol} to {symbol}_{timestamp}_lob_data.csv")

# Example Usage
if __name__ == "__main__":
    # Start streamer for multiple symbols
    lob_streamer = BinanceLOBStreamer(symbols=["btcusdt", "ethusdt"], depth_levels=10, update_interval=100)
    lob_streamer.start_stream()

    # Run the save function in a separate thread
    save_thread = threading.Thread(target=lob_streamer.save_to_csv)
    save_thread.start()

    # Let the streamer run for 30 seconds
    time.sleep(30)

    # Stop the WebSocket stream
    lob_streamer.stop_stream()

    # Wait for the save thread to finish
    save_thread.join()