import logging

from handlers.common import Common, get_by_uuid, get_by_label
from handlers.ntw import Network
from handlers.pool import Pool
from lib.functions import random_xen_mac


class VIF(Common):
    _type = "VIF"

    def __init__(self, xapi, ref=None, params=None):
        super().__init__(xapi, ref, params)

    def get_vm(self):
        return self.xapi.get_VM(self.ref)

    def get_network(self):
        return self.xapi.get_network(self.ref)

    def get_record_with_ntw_label(self):
        vif_record = self.get_record()
        vif_record["network_label"] = Network(self._xapi, vif_record["network"]).get_label()
        return vif_record

    def get_ipv4(self, vif_ref):
        return self.xapi.get_ipv4_addresses(vif_ref)


def restore(xapi, vif_record, network_map=None, restore=False):
    logger = logging.getLogger("VIF")

    ntw = Network(xapi, vif_record["network"])
    if not ntw.exists():
        ntw_ref = None
        if network_map is not None and vif_record["uuid"] in network_map:
            ntw_ref = get_by_uuid(ntw.xapi, network_map[vif_record["uuid"]])
        elif network_map is not None and vif_record["network_label"] in network_map:
            ntw_refs = get_by_label(ntw.xapi, network_map[vif_record["network_label"]])
            if len(ntw_refs) > 0:
                ntw_ref = ntw_refs[0]
        else:
            ntw_refs = get_by_label(ntw.xapi, vif_record["network_label"])
            if len(ntw_refs) > 0:
                ntw_ref = ntw_refs[0]

        if ntw_ref is None:
            ntw = Pool(xapi).get_default_ntw()
            ntw_ref = ntw.ref
            logger.warning("Assigning default network (%s) to interface %s", ntw.get_label(), vif_record["device"])

        vif_record["network"] = ntw_ref

    if not restore:
        vif_record["MAC"] = random_xen_mac()

    return VIF(xapi, params=vif_record)
