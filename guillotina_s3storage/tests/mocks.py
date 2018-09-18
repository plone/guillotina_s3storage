from aiohttp import ClientResponse
from aiohttp.helpers import TimerNoop
from unittest.mock import Mock
from yarl import URL
from aiohttp import StreamReader
from multidict import CIMultiDict
import os
import aiohttp
import json
import hashlib


class CassetteFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.request_to_response = {}

    def _load(self):
        try:
            with open(self.file_path, 'r') as f:
                self.request_to_response = json.loads(f.read())
        except FileNotFoundError:
            self.request_to_response = {}

    def play_response(self, request):
        try:
            return self.request_to_response[request.uuid]
        except KeyError:
            raise ResponseNotFoundError

    def append(self, request, response):
        if request.uuid in self.request_to_response:
            raise ResponseAlreadyRecorded
        self.request_to_response[request.uuid] = response

    def _save(self):
        with open(self.file_path, 'w') as f:
            f.write(json.dumps(self.request_to_response))


class MockedRequest:
    def __init__(self, method, url, data=None, params=None, headers=None):
        self.method = method
        self.url = url
        self.data = data
        self.params = params or {}
        self.headers = headers or {}

    @property
    def uuid(self):
        base = f'{self.method}|{self.url}|'\
                '{json.dumps({self.data})}|'\
                '{json.dumps({self.params})}|'\
                '{json.dumps({self.headers})}'
        return hashlib.md5(base.encode('utf-8')).hexdigest()


class ResponseNotFoundError(Exception):
    pass


class ResponseAlreadyRecorded(Exception):
    pass


class Cassette:
    _cassette_file = None
    ignore = set()
    cassette = 'test.yaml'

    def __init__(self, ignored=None, cassette='test.yaml'):
        self.cassette = cassette
        if not self._cassette_file:
            self._cassette_file = self.load_cassette()

        self.ignore = {
            'localhost',
            '127.0.0.1',
        }
        if ignored:
            self.add_ignored_host(ignored)

    def add_ignored_host(self, host):
        if type(host) != set:
            host = {host}
        self.ignore.update(host)

    def load_cassette(self):
        folder = os.path.join(os.path.dirname(__file__),
                              f'cassettes/{self.cassette}')
        cassette = CassetteFile(folder)
        cassette._load()
        return cassette

    def __call__(self):
        return self._cassette_file

    def skip(self, url):
        for ignore in self.ignore:
            if ignore in url:
                return True
        return False

    async def store_response(self, method, url, params,
                             data, headers, response):
        if type(url) == URL:
            url = url.human_repr()

        if not self.skip(url):
            print(f'STORING request: {url}')
            request = MockedRequest(method, url, data, headers)
            response_serialized = {
                'status': {
                    'code': response.status,
                    'message': response.reason,
                },
                'headers': dict(response.headers),
                'body': {'string': (await response.text())},  # NOQA: E999
                'url': str(response.url),
            }
            self._cassette_file.append(request, response_serialized)
            self._cassette_file._save()

    def build_response(self, method, url, params, data, headers):
        try:
            if type(url) == URL:
                url = url.human_repr()

            if self.skip(url):
                return None

            request = MockedRequest(method, url, data=data,
                                    params=params, headers=headers)
            resp_json = self._cassette_file.play_response(request)
        except ResponseNotFoundError:
            # Return None if response was not found
            return None

        if not isinstance(url, URL):
            url = URL(url)

        resp = ClientResponse(
            method,
            url,
            writer=Mock(),
            continue100=None,
            timer=TimerNoop(),
            request_info=Mock(),
            traces=[],
            loop=Mock(),
            session=Mock(),
        )

        resp.status = resp_json['status']['code']
        # XXX NOTE: Setting status_code otherwise aiobotocore fails
        # (weird...)
        setattr(resp, 'status_code', resp.status)
        data = resp_json['body']['string']

        try:
            resp_json['headers']['Content-Type']
        except KeyError:
            resp_json['headers']['Content-Type'] = 'plain/text'

        resp._headers = CIMultiDict(resp_json['headers'])
        resp._raw_headers = tuple([(k.encode(), v.encode()) for k, v in
                                   resp.headers.items()])
        resp.content = StreamReader(Mock())
        if isinstance(data, str):
            data = data.encode('utf8')
        resp.content.feed_data(data)
        resp.content.feed_eof()

        return resp


# Declare the singleton
cassette = Cassette()


async def handle_request(self, method: str, url: str,
                         params=None, data=None, headers=None,
                         *args, **kwargs):

    """Return mocked response object or raise connection error."""
    resp = cassette.build_response(method, url, params, data, headers)

    if not resp:
        resp = await self._original_request(
            method, url, params=params, data=data,
            headers=headers, *args, **kwargs)
        await cassette.store_response(method, url, params,
                                      data, headers, resp)
    return resp


class AiohttpMocker:
    @classmethod
    def start(cls):
        aiohttp.client.ClientSession._original_request = aiohttp.client.ClientSession._request
        aiohttp.client.ClientSession._request = handle_request

    @classmethod
    def cleanup(cls):
        aiohttp.client.ClientSession._request = aiohttp.client.ClientSession._original_request
