import logging
import lib.XenAPI as XenAPI

from lib.functions import timestamp_to_datetime, get_saned_string


class CommonHandler(object):
    error_template = "Error exporting {} {} ({}). {} error: {}"
    xapi = None

    def __init__(self, xapi, master_url=None, session_id=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.type = self.__class__.__name__.replace("Handler", "").upper()
        self.xapi = xapi
        self.master_url = master_url
        self.session_id = session_id

    def exists(self, obj_ref):
        return obj_ref in self.get_all_refs()

    def create(self, params, description=None):
        return self.xapi.create(params) if description is None else self.xapi.create(params, description)

    def get_all_refs(self):
        return self.xapi.get_all()

    def get_all_records(self):
        return self.xapi.get_all_records()

    def get_by_label(self, obj_label):
        return self.xapi.get_by_name_label(obj_label)

    def get_by_uuid(self, obj_uuid):
        try:
            return self.xapi.get_by_uuid(obj_uuid)
        except XenAPI.Failure as e:
            self.logger.error("Error getting reference of %s with UUID '%s': %s", self.type, obj_uuid, e.details)

    def get_record(self, obj_ref):
        return self.xapi.get_record(obj_ref)

    def get_uuid(self, obj_ref):
        return self.xapi.get_uuid(obj_ref)

    def get_label(self, obj_ref):
        return self.xapi.get_name_label(obj_ref)

    def get_label_sane(self, obj_ref):
        return get_saned_string(self.get_label(obj_ref))

    def get_snapshots(self, obj_ref):
        return self.xapi.get_snapshots(obj_ref)

    def get_snapshot_of(self, obj_ref):
        return self.xapi.get_snapshot_of(obj_ref)

    def get_snapshot_time(self, obj_ref, to_str=True, ts_format="%Y%m%dT%H%M00Z"):
        return timestamp_to_datetime(self.xapi.get_snapshot_time(obj_ref).value, to_str, to_format=ts_format)

    def snapshot(self, obj_ref, snap_name=None):
        self.logger.debug("Taking snapshot of %s '%s'", self.type, self.get_label(obj_ref))
        return self.xapi.snapshot(obj_ref) if snap_name is None else self.xapi.snapshot(obj_ref, snap_name)

    def destroy(self, obj_ref):
        self.logger.debug("Destroying %s '%s'", self.type, self.get_label(obj_ref))
        self.xapi.destroy(obj_ref)
