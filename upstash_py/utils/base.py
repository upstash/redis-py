from base64 import b64decode


def base64_to_string(text):
    return b64decode(text).decode()