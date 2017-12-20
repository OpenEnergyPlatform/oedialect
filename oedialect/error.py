class CursorError(Exception):
    def __init__(self, message):
        self.message = message

class NotSupportedError(Exception):
    pass
