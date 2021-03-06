from handlers.common import Common


class VBD(Common):
    _type = "VBD"

    def __init__(self, xapi, ref=None, params=None):
        super().__init__(xapi, ref, params)

    def is_disk(self):
        return self.get_type() == "Disk"

    def get_type(self):
        return self.xapi.get_type(self.ref)

    def get_vdi_ref(self, disk_only=False):
        if not disk_only or self.is_disk():
            vdi_ref = self.xapi.get_VDI(self.ref)
            if vdi_ref != "OpaqueRef:NULL":
                return vdi_ref  # VDI(self._xapi, self.master_url, self.session_id, vdi_ref)

    def get_vm(self):
        return self.xapi.get_VM(self.ref)
