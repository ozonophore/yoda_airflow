import logging
from time import sleep

import requests
from requests import Response

request_iteration = 10
request_timeout = 60


def post(url, data=None, json=None, **kwargs) -> Response:
    i = request_iteration
    resp = requests.post(url, data=data, json=json, **kwargs)
    while resp.status_code != 200 and i > 0:
        logging.warning("Attemption: %d status_code: %d error: %s", i, resp.status_code, resp.text)
        sleep(60)
        resp = requests.post(url, data=data, json=json, **kwargs)
        i -= 1
    resp.raise_for_status()
    return resp


def get(url, params=None, **kwargs) -> Response:
    i = request_iteration
    resp = requests.get(url, params=params, **kwargs)
    while resp.status_code != 200 and i > 0:
        sleep(request_timeout)
        resp = requests.get(url, params=params, **kwargs)
        i -= 1
    resp.raise_for_status()
    return resp
