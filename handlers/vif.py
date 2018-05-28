from handlers.common import CommonHandler
from lib.functions import random_xen_mac


class VifHandler(CommonHandler):
    def __init__(self, xapi, ntw_handler=None):
        super().__init__(xapi.VIF, None, None)
        self.network = ntw_handler if ntw_handler is not None else CommonHandler(xapi.network)

    def get_vm(self, vbd_ref):
        return self.xapi.get_VM(vbd_ref)

    def get_record_with_ntw_label(self, vif_ref):
        vif_record = self.get_record(vif_ref)
        vif_record["network_label"] = self.network.get_label(vif_record["network"])
        return vif_record

    def get_ipv4(self, vif_ref):
        return self.xapi.get_ipv4_addresses(vif_ref)

    def restore(self, vif_record, network_map=None, restore=False):

        if not self.network.exists(vif_record["network"]):
            if network_map is not None and vif_record["uuid"] in network_map:
                vif_record["network"] = self.network.get_by_uuid(network_map[vif_record["uuid"]])
            elif network_map is not None and vif_record["network_label"] in network_map:
                vif_record["network"] = self.network.get_by_label(network_map[vif_record["network_label"]])[0]
            else:
                vif_record["network"] = self.network.get_all_refs()[0]
                self.logger.warning("Assigning default network (%s) to interface %s",
                                    self.network.get_label(vif_record["network"]), vif_record["device"])

        if not restore:
            vif_record["MAC"] = random_xen_mac()

        return self.create(vif_record)
