from coinbase.rest import RESTClient
from json import dumps
from dotenv import load_dotenv
import os
import http.client
import json
import datetime
import time

load_dotenv()
api_key = os.getenv("COINBASE_API_KEY")
api_secret = os.getenv("COINBASE_API_SECRET")



client = RESTClient(api_key=api_key, api_secret=api_secret)

#LIST ACCOUNTS
accounts = client.get_accounts()
# print(dumps(accounts.to_dict(), indent=2))



#GET BEST BID/ASK
product_ids = ["BTC-USD", "ETH-USD"]

response = client.get_best_bid_ask(product_ids=product_ids)

# print(dumps(response.to_dict(), indent=2))

#GET PRODUCT BOOK
product_id = "BTC-USD"

response = client.get_product_book(product_id=product_id, limit=3)

# print(dumps(response.to_dict(), indent=2))

#GET MARKET TRADES

product_id = "BTC-USD"

response = client.get_market_trades(product_id=product_id, limit=50)

# print(dumps(response.to_dict(), indent=2))

#GET LIST PRODUCTS
response = client.get_public_products()
products_data = response.to_dict().get('products', [])

# Filter out pairs that contain "USD" in the product_id
usd_pairs = [product for product in products_data if "USD" in product['product_id']]

# Save filtered USD pairs to a JSON file
with open("usd_pairs.json", "w") as output_file:
    json_data = dumps(usd_pairs, indent=2)
    output_file.write(json_data)

print(f"Filtered {len(usd_pairs)} pairs containing 'USD' and saved to 'usd_pairs.json'.")

#GET PRODUCT CANDLES

# product_id = "BTC-USD"

# response = client.get_candles(product_id=product_id, start=1704088861, end=1704175261, granularity="FIVE_MINUTE")

# print(dumps(response.to_dict(), indent=2))



#GET BACKTEST DATA=====================================================================================

# Product ID and granularity
product_ids = ["BTC-USDC", "ETH-USDC", "XRP-USDC", "DOGE-USDC", "ADA-USDC", "XLM-USDC", "LINK-USDC", "HBAR-USDC", "BCH-USDC",
               "LTC-USDC", "DAI-USDC", "ETC-USDC", "CRO-USDC", "VET-USDC", "FIL-USDC", "FET-USDC", "ALGO-USDC", "ATOM-USDC", "STX-USDC",
               "XTZ-USDC", "QNT-USDC", "MKR-USDC", "EOS-USDC", "MANA-USDC", "MATIC-USDC", "ZEC-USDC", "CHZ-USDC", "GNO-USDC", "SNX-USDC", 
               "KAVA-USDC","KSM-USDC", "DASH-USDC", "ZRX-USDC", "GLM-USDC", "ZEN-USDC", "ANKR-USDC", "BAT-USDC", "IOTX-USDC", "XYO-USDC", 
               "LRC-USDC", "RPL-USDC", "VTHO-USDC", "STORJ-USDC", "BAND-USDC", "COTI-USDC", 
               ]
granularity = "FIVE_MINUTE"  # Five-minute candles

# Date range for 4 years
end_time = int(datetime.datetime(2025, 1, 1, 0, 0, 0).timestamp()) # Fixed end time (Unix timestamp)
start_time = end_time - (4 * 365 * 24 * 60 * 60)  # 4 years ago

# Coinbase API only allows 350 data points per request.
# Calculate how many seconds each candle represents
granularity_seconds = 5 * 60  # 5 minutes in seconds
max_candles = 350
for product_id in product_ids:
    # List to store all the candles
    all_candles = []

    print("Starting data collection for {product_id}...")

    # Iterate through the time range in chunks
    while start_time < end_time:
        chunk_end_time = start_time + (max_candles * granularity_seconds)
        
        # Ensure the chunk end doesn't exceed the overall end time
        if chunk_end_time > end_time:
            chunk_end_time = end_time

        print(f"Fetching data for {product_id} from {datetime.datetime.fromtimestamp(start_time)} to {datetime.datetime.fromtimestamp(chunk_end_time)}")

        # Request the data
        response = client.get_candles(product_id=product_id, start=start_time, end=chunk_end_time, granularity=granularity)

        # Convert response to a dictionary and store in the list
        candles = response.to_dict().get('candles', [])
        all_candles.extend(candles)

        # Move the start time forward
        start_time = chunk_end_time

        # Pause to avoid rate-limiting issues
        time.sleep(1)  # Add a small delay if needed

    save_file_path = "../backtest_data/" + f"{product_id.replace('-', '_').lower()}_historical_data.json"

    # Save data to a JSON file
    with open(save_file_path, "w") as file:
        json_data = dumps(all_candles, indent=2)
        file.write(json_data)

    print(f"Data collection for {product_id} complete. Saved to {save_file_path}.")

print("All data collection tasks completed.")
