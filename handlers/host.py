from handlers.common import Common
from handlers.vm import VM


class Host(Common):

    def __init__(self, xapi, master_url, session_id):
        pools = self.xapi.pool.get_all_refs()
        if len(pools) == 0:
            raise Exception("Host is not a pool member")
        super().__init__(pools[0], xapi.pool, master_url, session_id)

        self.ntw = xapi.network
        self.sr = xapi.SR

        self.vm = xapi.VM
        self.vif = xapi.VIF
        self.vbd = xapi.VBD
        self.vdi = xapi.VDI
