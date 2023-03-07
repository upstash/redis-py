class Redis:
    def __init__(self, url: str, token: str, enable_telemetry: bool = False):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry

    @staticmethod
    def command(func):
        # We need to explicitly pass "self" to bind the function
        def wrapper(self):
            """
            Every function decorated with @command should return
            the command in a form that can be passed to the REST API
            """
            command = func(self)
            return "ok"

        return wrapper

    @command
    def get(self):
        pass

