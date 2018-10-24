import re

from handlers.common import Common
from handlers.ntw import Network
from handlers.sr import SR


class Pool(Common):
    _type = "pool"

    def __init__(self, xapi):
        pools = xapi.pool.get_all()
        if len(pools) == 0:
            raise Exception("Host is not a pool member")
        super().__init__(xapi, pools[0])

    def get_default_sr(self):
        return SR(self._xapi, self.xapi.get_default_SR(self.ref))

    def get_default_ntw(self):
        return Network(self._xapi, self._xapi.network.get_all()[0])
