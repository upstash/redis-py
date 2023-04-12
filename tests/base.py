from base64 import b64encode


def string_to_base64(text: str) -> str:
    return b64encode(bytes(text, "utf-8")).decode()