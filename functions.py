import urllib.parse
import requests
import json
import hashlib
import hmac
import base64
import schedule
import time
from datetime import datetime
import pytz
from constants import *

def get_kraken_signature(urlpath, data, secret):

    if isinstance(data, str):
        encoded = (str(json.loads(data)["nonce"]) + data).encode()
    else:
        encoded = (str(data["nonce"]) + json.dumps(data)).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def run_scheduler():
    """
    Scheduler that triggers `my_task` at midnight and noon EST.
    """
    # Schedule the task
    schedule.every().day.at("00:00").do(trade)  # Midnight EST
    schedule.every().day.at("12:00").do(trade)  # Noon EST

    # Continuously run the scheduler
    while True:
        schedule.run_pending()
        print("last price is " + str(get_last_closed_price(quoteurl, asset_pair)))
        time.sleep(1)  # Sleep for 1 second to avoid high CPU usage


def trade():
	last_bid_price_float = get_last_bid_price(quoteurl, asset_pair)
	print("buying " + str(dca_amount / last_bid_price_float) + "of " + asset_pair)

	# Prepare the payload
	post_data = {
	    "nonce": str(int(time.time() * 1000)),  # Use the current timestamp in milliseconds
	    "ordertype": "market",
	    "type": "buy",
	    "volume": str(dca_amount / last_bid_price_float),
	    "pair": asset_pair,
	    "price": str(last_bid_price_float),
	    "ordertype": "limit"
	}

	# Generate API-Sign
	api_sign = get_kraken_signature(url, post_data, api_secret)

	# Prepare headers
	headers = {
	    'Content-Type': 'application/json',
	    'Accept': 'application/json',
	    'API-Key': private_key,
	    'API-Sign': api_sign
	}

	# Make the request
	response = requests.request("POST", api_base_url + url, headers=headers, data=json.dumps(post_data))

	print(response.text)


def get_last_bid_price(quote_url: str, pair: str) -> float:
    """
    Fetches the last bid price for a given trading pair from the Kraken API.

    :param quote_url: The full URL to the Kraken API endpoint (e.g., "https://api.kraken.com/0/public/Ticker")
    :param pair: The trading pair (e.g., "XXBTZCAD")
    :return: The last bid price as a float
    """
    payload = {}
    headers = {
        'Accept': 'application/json'
    }

    # Make the GET request
    response = requests.get(quote_url, headers=headers, data=payload)

    # Check for a successful response
    if response.status_code != 200:
        raise ValueError(f"Error: Received status code {response.status_code} from Kraken API")

    # Parse the JSON data
    data = response.json()

    # Check for errors in the Kraken response
    if data.get("error"):
        raise ValueError(f"Kraken API returned an error: {data['error']}")

    # Extract the last bid price
    try:
        last_bid_price_str = data["result"][pair]["b"][0]
        return float(last_bid_price_str)
    except KeyError as e:
        raise KeyError(f"Missing expected data in API response: {e}")


def get_last_closed_price(quote_url: str, pair: str) -> float:
    """
    Fetches the last bid price for a given trading pair from the Kraken API.

    :param quote_url: The full URL to the Kraken API endpoint (e.g., "https://api.kraken.com/0/public/Ticker")
    :param pair: The trading pair (e.g., "XXBTZCAD")
    :return: The last bid price as a float
    """
    payload = {}
    headers = {
        'Accept': 'application/json'
    }

    # Make the GET request
    response = requests.get(quote_url, headers=headers, data=payload)

    # Check for a successful response
    if response.status_code != 200:
        raise ValueError(f"Error: Received status code {response.status_code} from Kraken API")

    # Parse the JSON data
    data = response.json()

    # Check for errors in the Kraken response
    if data.get("error"):
        raise ValueError(f"Kraken API returned an error: {data['error']}")

    # Extract the last bid price
    try:
        last_bid_price_str = data["result"][pair]["b"][0]
        return float(last_bid_price_str)
    except KeyError as e:
        raise KeyError(f"Missing expected data in API response: {e}")