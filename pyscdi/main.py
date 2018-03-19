from __future__ import division, print_function
from .utils import md5, md5_ba, getSize
from .settings import API_URL
import requests
import logging

formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
LOGGER = logging.getLogger('scdi')
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(handler)

class Scdi:
    """SCDI Connection

        This provides a connection to the SCDI cloud services. The connection
        object is required when managing a bucket.

    """

    def __init__(self, username, api_key):
        """SCDI connector class.

        Args:
           username (str): SCDI username.
           api_key (str): a valid API key.

        """
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
        """Creates a new timeseries bucket.

        Args:
           bucketname (str): name of the bucket.
           columns (list): a list of columns.

        """
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
        """Connects to an existing timeseries bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Timeseries(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("Bucket not found")
        return bucket

    def create_geotemporal_bucket(self, bucketname, columns):
        """Creates a new geotemporal bucket.

        Args:
           bucketname (str): name of the bucket.
           columns (list): a list of columns.

        """
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
        """Connects to an existing geotemporal bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Geotemporal(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("Bucket not found")
        return bucket

    def create_keyvalue_bucket(self, bucketname):
        """Creates a new key-value bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        uri = API_URL + self._username + '/' + bucketname + '?create'
        try:
            r = self._make_request('POST', uri, json={'type':'keyvalue'})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return Keyvalue(self, bucketname)

    def get_keyvalue_bucket(self, bucketname):
        """Connects to an existing key-value bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Keyvalue(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("Bucket not found")
        return bucket

    def create_kws_bucket(self, bucketname):
        """Creates a new KWS bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        uri = API_URL + self._username + '/' + bucketname + '?create'
        try:
            r = self._make_request('POST', uri, json={'type':'object'})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return Kws(self, bucketname)

    def get_kws_bucket(self, bucketname):
        """Connects to an existing KWS bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        bucket = Kws(self, bucketname)
        if bucket.get_info() is None:
            raise Exception("bucket not found")
        return bucket

    def drop_bucket(self, bucketname):
        """Removes an existing bucket.

        Args:
           bucketname (str): name of the bucket.

        """
        uri = API_URL + self._username + '/' + bucketname + '?delete'
        try:
            r = self._make_request('DELETE', uri)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return r

    def get_buckets(self):
        """Get all buckets."""
        uri = API_URL + self._username
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

    def get_info(self):
        uri = API_URL + self._conn._username + '/' + self._bucketname + '?meta'
        r = self._conn._make_request('GET', uri)
        if len(r.text) > 1:
            return r.json()
        else:
            return None

class Kws(BaseBucket):
    """KWS Bucket"""
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
        uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
        try:
            r = self._conn._make_request('GET', uri)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warn(r.text)
        return r.content

    def get_object_url(self, objectName):
        """Gets the object URL.

        Args:
            objectName (str): name of the object.

        Returns:
            str.
        """
        uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
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
            uri = API_URL + self._conn._username + '/' + self._bucketname + '/' + objectName
            try:
                r = self._conn._s.put(uri, stream=True, data=open(path, 'rb'), headers=headers)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                LOGGER.warn(r.text)
            return r.text
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
        """Deletes an object.

        Args:
            objectName (str): name of the object.
        """
        uri = API_URL + self._conn._username + '/' + self._bucketName + '/' + objectName
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
        uri = API_URL + self._conn._username + '/' + self._bucketname
        try:
            r = self._conn._make_request('PUT', uri, json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.error(r.text)
        return r.text

    def add_rows(self, payload):
        """Adds multiple rows to the timeseries bucket.

        Args:
            payload (list): rows of data

        Returns:
            str. HTTP Response text.
        """
        uri = API_URL + self._conn._username + '/' + self._bucketname
        try:
            r = self._conn._make_request('POST', uri + '?batch', json=payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.error(r.text)
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
        """Puts a key-value pair

        Args:
            key (str): key string
            value (bytes): bytes

        """
        uri = API_URL + self._conn._username + '/' + self._bucketname
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
        uri = API_URL + self._conn._username + '/' + self._bucketname
        r = self._conn._make_request('GET', uri, params={'key' : key})
        r.raise_for_status()
        return r.content
