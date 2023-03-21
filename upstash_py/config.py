"""
For now, we'll store the defaults here.
In the future, we might have an option to load from .env in development environments.
"""
config: dict[str, str | int | bool] = {
    "REST_ENCODING": "base64",
    "REST_RETRIES": 1,
    "REST_RETRY_INTERVAL": 3,
    "ENABLE_TELEMETRY": False,
    "ALLOW_DEPRECATED": True,
    "FORMAT_RETURN": True
}
