import time

import requests
from fake_useragent import UserAgent
from requests.exceptions import RequestException

def safe_request(url, headers={}, wait_time=1, max_retries=3):
    if headers == {}:
        # make a user agent
        ua = UserAgent()

        headers = {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*"
            ";q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referrer": "https://www.google.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    current_wait_time = wait_time
    # Send the initial request
    for i in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            html = response.content
            break
        except RequestException as e:
            print(f"Request failed (attempt {i + 1}/{max_retries}): {e}")
            if i == max_retries:
                print(f"skipping url {url}")
                html = None
            print(f"Retrying in {current_wait_time} seconds...")
            time.sleep(current_wait_time)
            i += 1

    return html