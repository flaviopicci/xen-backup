from handlers.common import Common


class Task(Common):
    _type = "task"

    def __init__(self, xapi, ref=None, params=None):
        super().__init__(xapi, ref, params)

    def cancel(self):
        self.xapi.cancel(self.ref)
