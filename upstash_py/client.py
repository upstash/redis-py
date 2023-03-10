from upstash_py.http.execute import execute


class Redis:
    def __init__(self, url: str, token: str, enable_telemetry: bool = False):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry

    @staticmethod
    def command(func):
        # We need to explicitly pass "self" to bind the function
        def wrapper(self, *args):
            """
            Every function decorated with @command should return
            the command in a form that can be passed to the REST API
            """
            command = func(self, *args)
            return execute(url=self.url, token=self.token, command=command)

        return wrapper

    @command
    def get(self, key):
        return f'get {key}'

    @command
    def set(self, key, value):
        return f'set {key} {value}'


redis = Redis(
    url="",
    token="")

print(redis.set("a", "b"))
print(redis.get("a"))
