from handlers.common import CommonHandler


class PoolHandler(CommonHandler):
    def __init__(self, xapi):
        super().__init__(xapi.pool)

        pools = self.get_all_refs()
        if len(pools) > 0:
            self.pool_ref = pools[0]

    def get_default_sr(self):
        return self.xapi.get_default_SR(self.pool_ref)
