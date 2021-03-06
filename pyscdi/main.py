from __future__ import division, print_function
from .utils import md5, md5_ba, getSize
from .settings import API_URL
import requests
import logging
import time

LOGGER = logging.getLogger('scdi')
RETRY_DELAY = 5.0

class ScdiException(Exception):
    pass

class Scdi:
    """SCDI Connection

        This provides a connection to the SCDI cloud services. The connection
        object is required when managing a bucket.

    """

    def __init__(self, username, api_key, api_url=API_URL):
        """SCDI connector class.

        Args:
           username (str): SCDI username.
           api_key (str): a valid API key.
           api_url (str): an endpoint to scdi server

        """
        self._username = username
        self._api_key = api_key
        self._headers = {
            'APIKEY': self._api_key,
            'User-Agent': 'pyscdi/0.2'
        }
        self._s = requests.Session()
        self._api_url = api_url

    def _make_request(self, verb, uri, params=None, data=None, json=None,
            timeout=60.0, max_retries=10, stream=None, headers=None):
        if verb not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise ScdiException('method not supported')
        retry_count = 0
        merged_headers = dict(self._headers)
        if headers is not None:
            for k in headers:
                merged_headers[k] = headers[k]
        while retry_count < max_retries:
            try:
                r = self._s.request(verb, uri, params=params, headers=merged_headers,
                                    data=data, json=json, timeout=timeout, stream=stream)
                LOGGER.debug('req time: %.2fs', r.elapsed.total_seconds())
                r.raise_for_status()
                return r

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                # resource not ready
                # LOGGER.debug('status_code = {}'.format(status_code))
                if status_code == 403:
                    LOGGER.warn("Bucket not ready. Retrying in %.1fs...", RETRY_DELAY)
                    retry_count += 1
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    raise e

            except requests.exceptions.Timeout as e:
                LOGGER.error("Connection timeout!")
                raise e

            except requests.exceptions.ConnectionError as e:
                LOGGER.error("Connection error!")
                raise e

    def create_tabular_bucket(self, bucketname, columns):
        """Creates a generic tabular bucket.

        Args:
           bucketname (str): name of the bucket.
           columns (list): a list of columns.

        """
        try:
            uri = self._api_url + self._username + '/' + bucketname + '?create'
            payload = dict()
            payload['type'] = 'tabular'
            payload['columns'] = columns
            r = self._make_request('POST', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn("Bucket %s already exists", bucketname)
        return Tabular(self, bucketname)

    def get_tabular_bucket(self, bucketname):
        """Connects to an existing tabular bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Tabular(self, bucketname)
        if bucket.get_info() is None:
            raise ScdiException("Bucket not found")
        return bucket

    def create_timeseries_bucket(self, bucketname, columns):
        """Creates a new timeseries bucket.

        Args:
           bucketname (str): name of the bucket.
           columns (list): a list of columns.

        """
        try:
            uri = self._api_url + self._username + '/' + bucketname + '?create'
            payload = dict()
            payload['type'] = 'timeseries'
            payload['columns'] = columns
            r = self._make_request('POST', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn("Bucket %s already exists", bucketname)
        return Timeseries(self, bucketname)

    def get_timeseries_bucket(self, bucketname):
        """Connects to an existing timeseries bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Timeseries(self, bucketname)
        if bucket.get_info() is None:
            raise ScdiException("Bucket not found")
        return bucket

    def create_geotemporal_bucket(self, bucketname, columns):
        """Creates a new geotemporal bucket.

        Args:
           bucketname (str): name of the bucket.
           columns (list): a list of columns.

        """
        try:
            uri = self._api_url + self._username + '/' + bucketname + '?create'
            payload = dict()
            payload['type'] = 'geotemporal'
            payload['columns'] = columns
            r = self._make_request('POST', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn("Bucket %s already exists", bucketname)
        return Geotemporal(self, bucketname)

    def get_geotemporal_bucket(self, bucketname):
        """Connects to an existing geotemporal bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Geotemporal(self, bucketname)
        if bucket.get_info() is None:
            raise ScdiException("Bucket not found")
        return bucket

    def create_keyvalue_bucket(self, bucketname):
        """Creates a new key-value bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        try:
            uri = self._api_url + self._username + '/' + bucketname + '?create'
            r = self._make_request('POST', uri, json={'type': 'keyvalue'})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn("Bucket %s already exists", bucketname)
        return Keyvalue(self, bucketname)

    def get_keyvalue_bucket(self, bucketname):
        """Connects to an existing key-value bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Keyvalue(self, bucketname)
        if bucket.get_info() is None:
            raise ScdiException("Bucket not found")
        return bucket

    def create_kws_bucket(self, bucketname):
        """Creates a new KWS bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        try:
            uri = self._api_url + self._username + '/' + bucketname + '?create'
            r = self._make_request('POST', uri, json={'type': 'object'})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn("Bucket %s already exists", bucketname)
        return Kws(self, bucketname)

    def get_kws_bucket(self, bucketname):
        """Connects to an existing KWS bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Kws(self, bucketname)
        if bucket.get_info() is None:
            raise ScdiException("bucket not found")
        return bucket

    def drop_bucket(self, bucketname):
        """Removes an existing bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        uri = self._api_url + self._username + '/' + bucketname + '?delete'
        r = self._make_request('DELETE', uri)
        r.raise_for_status()
        return r

    def get_buckets(self):
        """Get all buckets."""
        uri = self._api_url + self._username
        r = self._make_request('GET', uri)
        if r.status_code == 200:
            return r.json()
        else:
            return []


class BaseBucket:
    """Base class for bucket"""
    def __init__(self, conn, bucketname):
        self._conn = conn
        self._bucketname = bucketname
        self._api_url = conn._api_url

    def get_info(self):
        uri = self._api_url + self._conn._username + '/' + self._bucketname + '?meta'
        r = self._conn._make_request('GET', uri)
        if len(r.text) > 1:
            return r.json()
        else:
            return None

    def list_objects(self):
        """List objects in a bucket."""
        uri = self._api_url + self._conn._username + '/' + self._bucketname + '?list'
        r = self._conn._make_request('GET', uri)
        if r.status_code == 200:
            return r.json()
        else:
            return []

class Kws(BaseBucket):
    """KWS Bucket"""
    def _put_part(self, objectName, partNumber, byteArr):
        md5hex = md5_ba(byteArr)
        headers = {'APIKEY': self._conn._api_key, 'Content-MD5': md5hex, 'Content-Length': str(len(byteArr))}
        uri = self._api_url + self._conn._username + '/' + self._bucketname + '/' + objectName
        r = self._conn._make_request('PUT', uri, params={'partNumber': partNumber},
                                stream=True, data=byteArr, headers=headers)
        r.raise_for_status()

    def get_object_as_file(self, objectName, filename):
        """Downloads an object as a file

        Args:
            objectName (str): name of the object.
            filename (str): the output file.

        """
        data = self.get_object(objectName)
        with open(filename, 'wb') as f:
            f.write(data)

    def get_object(self, objectName):
        """Downloads an object as bytes

        Args:
            objectName (str): name of the object.

        Returns:
            bytes.
        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname + '/' + objectName
        r = self._conn._make_request('GET', uri)
        r.raise_for_status()
        return r.content

    def get_object_url(self, objectName):
        """Gets the object URL.

        Args:
            objectName (str): name of the object.

        Returns:
            str.
        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname + '/' + objectName
        return uri

    def put_object(self, objectName, path, max_size=3000000):
        """Uploads a file to an object.

        The object will be split into chunks of max_size (or smaller) and
        each chunk will be uploaded one by one.

        Args:
            objectName (str): name of the object.
            path (str): location of the file to upload

        Kwargs:
            max_size (int):

        Returns:
            str. HTTP Response text.
        """
        flenght = getSize(path)
        if int(flenght) < max_size:
            # do single part upload
            md5hex = md5(path)
            headers = {'APIKEY': self._conn._api_key, 'Content-MD5': md5hex, 'Content-Length': flenght}
            uri = self._api_url + self._conn._username + '/' + self._bucketname + '/' + objectName
            r = self._conn._make_request('PUT', uri, stream=True,
                                         data=open(path, 'rb'), headers=headers)
            r.raise_for_status()
            return r.text

        else:
            # do multipart upload
            uri = self._api_url + self._conn._username + '/' + self._bucketname + '/' + objectName + '?create'
            r = self._conn._make_request('POST', uri)
            r.raise_for_status()

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

            uri = self._api_url + self._conn._username + '/' + self._bucketname + '/' + objectName + '?complete'
            r = self._conn._make_request('POST', uri)
            r.raise_for_status()
            return r.text

    def delete_object(self, objectName):
        """Deletes an object.

        Args:
            objectName (str): name of the object.
        """
        uri = self._api_url + self._conn._username + '/' + self._bucketName + '/' + objectName
        r = self._conn._make_request('DELETE', uri)
        r.raise_for_status()
        return r.text

class Timeseries(BaseBucket):
    def add_row(self, payload):
        """Adds a row to the timeseries bucket.

        Args:
            payload (dict): row data

        Returns:
            str. HTTP Response text.
        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('PUT', uri, json=payload)
        r.raise_for_status()
        return r.text

    def add_rows(self, payload):
        """Adds multiple rows to the timeseries bucket.

        Args:
            payload (list): rows of data

        Returns:
            str. HTTP Response text.
        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('POST', uri + '?batch', json=payload)
        r.raise_for_status()
        return r.text

    def query(self, fromEpoch=None, toEpoch=None, limit=None, where=None, aggregate=None):
        """Queries data

        Kwargs:
            fromEpoch (float): Begin time (epoch) time
            toEpoch (float): End time (epoch) time
            limit (int): Maximum number of records returned
            where (list): a list of where filter
            aggregate (list): a list of aggregate filter

        Returns:
            list. List of returned rows.
        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname
        payload = dict()
        if fromEpoch is not None: payload['fromEpoch'] = fromEpoch
        if toEpoch is not None: payload['toEpoch'] = toEpoch
        if limit is not None: payload['limit'] = limit
        if where is not None: payload['where'] = where
        if aggregate is not None: payload['aggregate'] = aggregate
        r = self._conn._make_request('POST', uri + '?query', json=payload)
        r.raise_for_status()
        if len(r.text) > 1:
            return r.json()
        return []

class Geotemporal(Timeseries):
    pass

class Tabular(Timeseries):
    pass

class Keyvalue(BaseBucket):
    def put(self, key, value):
        """Puts a key-value pair

        Args:
            key (str): key string
            value (bytes): bytes

        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('POST', uri, params={'key' : key}, data=value)
        r.raise_for_status()
        return r.content

    def get(self, key):
        """Gets a value of a given key

        Args:
            key (str): key string

        Returns:
            bytes. Value of a key.

        """
        uri = self._api_url + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('GET', uri, params={'key' : key})
        r.raise_for_status()
        return r.content
