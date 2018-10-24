from handlers.common import Common


class SR(Common):
    _type = "SR"

    def __init__(self, xapi, ref=None, params=None):
        super().__init__(xapi, ref, params)
