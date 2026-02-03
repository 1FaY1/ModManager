import hashlib


def get_file_hash(path):
    sha1 = hashlib.sha1()
    try:
        with open(path, "rb") as file_handle:
            for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
                sha1.update(chunk)
    except OSError:
        return None
    return sha1.hexdigest()
