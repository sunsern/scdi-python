from __future__ import division, print_function
from .utils import md5, md5_ba, getSize

import requests
import logging

BASE_URL = 'https://scdi-api.philinelabs.net'
API_URL = BASE_URL + '/api/v1/'

formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
LOGGER = logging.getLogger('scdi')
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(handler)

class Scdi:
    def __init__(self, username, api_key):
        self._username = username
        self._api_key = api_key
        self._headers = { 'APIKEY' : self._api_key }
        self._s = requests.Session()

    def _make_request(self, verb, uri, params=None, data=None, json=None, timeout=10.0):
        if verb not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise Exception('method not supported')

        try:
            r = self._s.request(verb, uri, params=params,
                headers=self._headers, data=data, json=json, timeout=timeout)
            return r

        except requests.exceptions.Timeout as e:
            LOGGER.error("Connection timeout!")
            raise e

        except requests.exceptions.ConnectionError as e:
            LOGGER.error("Connection error!")
            raise e

    def create_timeseries_bucket(self, bucketname, columns):
        uri = API_URL + self._username + '/' + bucketname + '?create'
        payload = dict()
        payload['type'] = 'timeseries'
        payload['columns'] = columns
        try:
            r = self._make_request('POST', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return Timeseries(self, bucketname)

    def get_timeseries_bucket(self, bucketname):
        bucket = Timeseries(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("Bucket not found")
        return bucket

    def create_geotemporal_bucket(self, bucketname, columns):
        uri = API_URL + self._username + '/' + bucketname + '?create'
        payload = dict()
        payload['type'] = 'geotemporal'
        payload['columns'] = columns
        try:
            r = self._make_request('POST', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return Geotemporal(self, bucketname)

    def get_geotemporal_bucket(self, bucketname):
        bucket = Geotemporal(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("Bucket not found")
        return bucket

    def create_keyvalue_bucket(self, bucketname):
        uri = API_URL + self._username + '/' + bucketname + '?create'
        try:
            r = self._make_request('POST', uri, json={'type':'keyvalue'})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return Keyvalue(self, bucketname)

    def get_keyvalue_bucket(self, bucketname):
        bucket = Keyvalue(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("Bucket not found")
        return bucket

    def create_kws_bucket(self, bucketname):
        uri = API_URL + self._username + '/' + bucketname + '?create'
        try:
            r = self._make_request('POST', uri, json={'type':'object'})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return Kws(self, bucketname)

    def get_kws_bucket(self, bucketname):
        bucket = Kws(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("bucket not found")
        return bucket

    def drop_bucket(self, bucketname):
        uri = API_URL + self._username + '/' + bucketname + '?delete'
        try:
            r = self._make_request('DELETE', uri)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return r

class BaseBucket:
    def __init__(self, conn, bucketname):
        self._conn = conn
        self._bucketname = bucketname

    def get_info(self):
        uri = API_URL + self._conn._username + '/' + self._bucketname + '?meta'
        r = self._conn._make_request('GET', uri)
        if len(r.text) > 1:
            return r.json()
        else:
            return None


class Kws(BaseBucket):
    def _put_part(self, objectName, partNumber, byteArr):
        md5hex = md5_ba(byteArr)
        headers = {'APIKEY': self._conn._api_key, 'Content-MD5': md5hex, 'Content-Length': str(len(byteArr))}
        uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
        try:
            r = self._conn._s.put(uri, params={'partNumber' : partNumber},
                stream=True, data=byteArr, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)

    def get_object_as_file(self, objectName, filename):
        data = self.get_object(objectName)
        with open(filename, 'wb') as f:
            f.write(data)

    def get_object(self, objectName):
        uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
        try:
            r = self._conn._make_request('GET', uri)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return r.content

    def get_object_url(self, objectName):
        uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
        return uri

    def put_object(self, objectName, path, max_size=3000000):
        flenght = getSize(path)
        if int(flenght) < max_size:
            # do single part upload
            md5hex = md5(path)
            headers = {'APIKEY': self._conn._api_key, 'Content-MD5': md5hex, 'Content-Length': flenght}
            uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
            try:
                r = self._conn._s.put(uri, stream=True, data=open(path, 'rb'), headers=headers)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                LOGGER.warn(r.text)
            return r
        else:
            # do multipart upload
            uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName + '?create'
            try:
                r = self._conn._make_request('POST', uri)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                LOGGER.error(r.text)

            partNo = 1
            with open(path, 'rb') as fh:
                ba = bytes(fh.read())
                while len(ba) > max_size:
                    self._put_part(objectName, partNo, ba[:max_size])
                    partNo += 1
                    ba = ba[max_size:]
                # leftover bytes
                if len(ba) > 0:
                    self._put_part(objectName, partNo, ba)

            uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName + '?complete'
            try:
                r = self._conn._make_request('POST', uri)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                LOGGER.error(r.text)

            return r.text

    def delete_object(self, objectName):
        uri = API_URL + self._conn._username + '/' + self._bucketName + '/' + objectName
        r = self._conn._make_request('DELETE', uri)
        r.raise_for_status()
        return r

class Timeseries(BaseBucket):
    def add_row(self, payload):
        uri = API_URL + self._conn._username + '/' + self._bucketname
        try:
            r = self._conn._make_request('PUT', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.error(r.text)
        return r.text

    def add_rows(self, payload):
        uri = API_URL + self._conn._username + '/' + self._bucketname
        try:
            r = self._conn._make_request('POST', uri + '?batch', json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.error(r.text)
        return r.text

    def query(self, fromEpoch=None, toEpoch=None, limit=None, where=None, aggregate=None):
        uri = API_URL + self._conn._username + '/' + self._bucketname
        payload = dict()
        if fromEpoch is not None: payload['fromEpoch'] = fromEpoch
        if toEpoch is not None: payload['toEpoch'] = toEpoch
        if limit is not None: payload['limit'] = limit
        if where is not None: payload['where'] = where
        if aggregate is not None: payload['aggregate'] = aggregate
        try:
            r = self._conn._make_request('POST', uri + '?query', json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.error(r.text)
        if len(r.text) > 1:
            return r.json()
        else:
            return []

class Geotemporal(Timeseries):
    pass

class Keyvalue(BaseBucket):
    def put(self, key, value):
        uri = API_URL + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('POST', uri, params={'key' : key}, data=value)
        r.raise_for_status()
        return r.content

    def get(self, key):
        uri = API_URL + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('GET', uri, params={'key' : key})
        r.raise_for_status()
        return r.content
