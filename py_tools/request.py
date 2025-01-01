import requests
from py_tools.pylog import get_logger
import time
import random

logger = get_logger("py-tools.request")


class Request:
    def __init__(
        self, token, base_url, headers=None, skip_raising_codes=None, max_retries=5
    ):
        self.base_url = base_url
        self.max_retries = max_retries
        self.skip_raising_codes = skip_raising_codes or {}
        headers = headers or {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self.token = token or ""
        self.headers = headers

    def __call__(self, token, base_url):
        if token:
            self.token = token
        if base_url:
            self.base_url = base_url
        return self

    def invoke(self, method, path, body=None, params=None):
        retry_wait = 1  # initial wait time in seconds
        retries = 0
        url = f"{self.base_url}/{path}"
        skip_raising_codes = self.skip_raising_codes.get(method.lower(), [])

        while retries <= self.max_retries:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=body,
                params=params,
            )

            if response.ok:
                return response
            if response.status_code in skip_raising_codes:
                self.log_response(method, url, response)
                return response

            elif response.status_code in [400, 429, 500]:
                # Check if "Retry-After" header is available
                if "Retry-After" in response.headers:
                    wait = int(response.headers["Retry-After"])
                else:
                    # Implement exponential backoff with jitter
                    wait = retry_wait + random.uniform(0, 1)
                    retry_wait *= 2  # double the wait time for next retry

                logger.info(f"Waiting {wait} seconds")
                time.sleep(wait)
                retries += 1

            else:
                self.log_response(method, url, response)
                # Handle other HTTP errors or raise an exception
                response.raise_for_status()

        raise Exception(
            f"Request failed after {self.max_retries} retries with response: {response.text}"
        )

    def log_response(self, method, path, response):
        logger.info(
            "Request %s:%s failed with code %s" % (method, path, response.status_code),
            extra={"text": response.text},
        )
