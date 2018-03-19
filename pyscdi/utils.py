import hashlib
import codecs
import os

def md5(fname, block_size=4096):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def md5_ba(ba, block_size=4096):
    hash_md5 = hashlib.md5()
    for i in range(0, len(ba), block_size):
        hash_md5.update(ba[i:min(len(ba), i+block_size)])
    return hash_md5.hexdigest()

def getSize(filename):
    st = os.stat(filename)
    return str(st.st_size)

def multipartMd5(files):
    md5s = []
    for f in files:
        md5s.append(md5(f))
    hex_data = codecs.decode(''.join(md5s), 'hex')
    hmd5 = hashlib.md5()
    hmd5.update(hex_data)
    return hmd5.hexdigest() + '-' + str(len(files))

def readPartBytes(filename, partSize, partNumber):
    totalSize = os.stat(filename)
