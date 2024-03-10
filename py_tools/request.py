import requests
from py_tools.pylog import get_logger
import time
import random
from urllib.parse import urljoin
import os
logger = get_logger("py-tools.request")


class Request:
    def __init__(self, token, base_url, headers=None, skip_logging_codes=None):
        self.token = token
        self.base_url = base_url
        self.skip_logging_codes = skip_logging_codes or {}
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self.token}"
        self.headers = headers

    def __call__(self, token, base_url):
        if token:
            self.token = token
        if base_url:
            self.base_url = base_url
        return self

    def invoke(self, method, path, body=None, params=None, max_retries=5):
        retry_wait = 1  # initial wait time in seconds
        retries = 0
        url = f"{self.base_url}/{path}"
        error_codes = self.skip_logging_codes.get(method.lower(), [])
        while retries < max_retries:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=body,
                params=params,
            )
            if response.ok:
                return response
            elif response.status_code == 429:
                # Check if "Retry-After" header is available
                if "Retry-After" in response.headers:
                    wait = int(response.headers["Retry-After"])
                else:
                    # Implement exponential backoff with jitter
                    wait = retry_wait + random.uniform(0, 1)
                    retry_wait *= 3  # triple the wait time for next retry

                logger.info(f"Waiting {wait} seconds")
                time.sleep(wait)
                retries += 1
            else:
                self.log_response(method, url, response)
                if response.status_code not in error_codes:
                    # Handle other HTTP errors or raise an exception
                    print(response.text)
                    response.raise_for_status()

        raise Exception(f"Request failed after {max_retries} retries")

    def log_response(self, method, path, response):
        logger.error(
            "Request %s:%s failed with code %s" % (method, path, response.status_code),
            extra={"text": response.text},
        )
