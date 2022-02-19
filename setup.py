from setuptools import setup

setup(
    name="py-tools",
    version="1.0.0",
    packages=["py_tools"],
    url="https://github.com/wobeng/py-tools",
    license="",
    author="wobeng",
    author_email="wobeng@yblew.com",
    description="python tools",
    install_requires=[
        "pytz",
        "simplejson",
        "boto3",
        "requests",
        "python-dateutil",
        "slack_sdk",
        "jsonschema",
        "cryptography",
        "jwcrypto",
        "dotty-dict",
        "backoff",
        "pynamodb",
    ],
)
