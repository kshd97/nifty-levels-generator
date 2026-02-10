
import requests
import json
import time

def get_gift_nifty_price():
    url = "https://scanner.tradingview.com/global/scan"
    
    # Payload for GIFT Nifty (NSEIX:NIFTY1!)
    payload = {
        "symbols": {
            "tickers": ["NSEIX:NIFTY1!"],
            "query": {"types": []}
        },
        "columns": ["close", "time"]
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if data and "data" in data and len(data["data"]) > 0:
            # data["data"][0]["d"] is list of column values
            price = data["data"][0]["d"][0]
            timestamp = data["data"][0]["d"][1]
            
            # Format timestamp
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            
            print(f"GIFT Nifty Price: {price}")
            print(f"Last Updated: {time_str}")
            return price
            
        else:
            print("No data found in API response.")
            return None
            
    except Exception as e:
        print(f"Error fetching GIFT Nifty price: {e}")
        return None

if __name__ == "__main__":
    get_gift_nifty_price()
