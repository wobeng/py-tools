import requests
from py_tools.pylog import get_logger

logger = get_logger("py-tools.request")


class Request:
    def __init__(self, token, base_url):
        self.token = token
        self.base_url = base_url

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
            attrs = {
                "method": method,
                "path": path,
                "status_code": response.status_code,
                "text": response.text,
            }
            for k, v in attrs.items():
                logger.info("Failed %s: %s" % (k, v))
        return response
        
    def make_request(self, method, url, **kwargs):
        return getattr(requests, method)(url=url, **kwargs)

    def handle_response(self, response):
        if not response.ok:
            logger.debug(response.text)
    