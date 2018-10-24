import logging

from lib.XenAPI import Failure
from lib.functions import timestamp_to_datetime, get_saned_string


class Common(object):
    _type = None

    def __init__(self, xapi, ref=None, params=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self._xapi = xapi
        if ref is None:
            self.ref = self.create(params)
        else:
            self.ref = ref

    def exists(self):
        return self.ref in self.xapi.get_all()

    def create(self, params):
        if isinstance(params, list):
            ref = self.xapi.create(*params)
        else:
            ref = self.xapi.create(params)
        return ref

    def get_record(self):
        return self.xapi.get_record(self.ref)

    def get_uuid(self):
        return self.xapi.get_uuid(self.ref)

    def get_label(self):
        return self.xapi.get_name_label(self.ref)

    def get_label_sane(self):
        return get_saned_string(self.get_label())

    @property
    def xapi(self):
        return getattr(self._xapi, self._type)


class CommonEntities(Common):
    export_template = "Exporting {} {} '{}' (to {})"
    export_error_template = "Error exporting {} {} ({}). {} error: {}"

    def __init__(self, xapi, master_url, session_id, ref=None, params=None):
        super().__init__(xapi, ref, params)

        self.master_url = master_url
        self.session_id = session_id

    def get_allowed_operations(self):
        self.xapi.get_allowed_operations(self.ref)

    def get_snapshots(self):
        return (self.__class__(self._xapi, self.master_url, self.session_id, snap_ref) for snap_ref in
                self.xapi.get_snapshots(self.ref))

    def is_snapshot(self):
        return self.xapi.get_is_a_snapshot(self.ref)

    def get_snapshot_of(self):
        if self.is_snapshot():
            return self.__class__(self._xapi, self.master_url, self.session_id, self.xapi.get_snapshot_of(self.ref))
        else:
            return None

    def get_snapshot_time(self, to_str=True, ts_format="%Y%m%dT%H%M%S"):
        if self.is_snapshot():
            return timestamp_to_datetime(self.xapi.get_snapshot_time(self.ref).value, to_str, to_format=ts_format)
        else:
            return None

    def snapshot(self, snap_name=None):
        self.logger.debug("Taking snapshot of %s '%s'", self._type, self.get_label())
        snap_ref = self.xapi.snapshot(self.ref) if snap_name is None else self.xapi.snapshot(self.ref, snap_name)
        return self.__class__(self._xapi, self.master_url, self.session_id, snap_ref)

    def destroy(self):
        self.logger.debug("Destroying %s '%s'", self._type, self.get_label())
        self.xapi.destroy(self.ref)


def get_all_refs(xapi):
    return xapi.get_all()


def get_by_uuid(xapi, uuid):
    return xapi.get_by_uuid(uuid)


def get_by_label(xapi, label):
    return xapi.get_by_name_label(label)
