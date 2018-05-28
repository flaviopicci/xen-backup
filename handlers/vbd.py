from handlers.common import CommonHandler


class VbdHandler(CommonHandler):
    def __init__(self, xapi):
        super().__init__(xapi.VBD)

    def get_type(self, vbd_ref):
        return self.xapi.get_type(vbd_ref)

    def get_vdi(self, vbd_ref):
        return self.xapi.get_VDI(vbd_ref)

    def get_vm(self, vbd_ref):
        return self.xapi.get_VM(vbd_ref)
