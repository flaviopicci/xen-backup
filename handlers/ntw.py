from handlers.common import Common


class Network(Common):
    _type = "network"

    def __init__(self, xapi, ref=None, params=None):
        super().__init__(xapi, ref, params)
