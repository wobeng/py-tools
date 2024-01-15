import requests
from py_tools.pylog import get_logger

logger = get_logger("py-tools.request")


class Request:
    def __init__(self, token, base_url, skip_logging_codes=None):
        self.token = token
        self.base_url = base_url
        self.skip_logging_codes = skip_logging_codes or {}

    def __call__(self, token, base_url):
        if token:
            self.token = token
        if base_url:
            self.base_url = base_url
        return self

    def invoke(self, method, path, **kwargs):
        url = self.base_url + path
        headers = {"Authorization": f"Bearer {self.token}"}

        response = self.make_request(method, url, headers=headers, **kwargs)

        if not response.ok:
            errors = self.skip_logging_codes.get(method.lower(), [])
            if response.status_code not in errors:
                self.log_response(method, path, response)
                response.raise_for_status()

        return response

    def make_request(self, method, url, **kwargs):
        return getattr(requests, method)(url=url, **kwargs)

    def log_response(self, method, path, response):
        attrs = {
            "method": method,
            "path": path,
            "status_code": response.status_code,
            "text": response.text,
        }
        for k, v in attrs.items():
            logger.debug("Failed %s: %s" % (k, v))
