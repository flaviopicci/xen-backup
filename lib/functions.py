import json
import logging
import os
import random
from datetime import datetime, timezone

from lib.datetime_encoder import DateTimeEncoder

logger = logging.getLogger("Utils")
vdi_file_format = "vhd"


def exit_gracefully(signum, _):
    raise SystemExit(signum)


def get_saned_string(string):
    return string.replace(" ", "_").replace("/", "_")


def get_vms_to_backup(vms, excluded_vms=None, vm_ref_list=None, vm_uuid_list=None, vm_name_list=None):
    vm_refs = set()

    if vm_ref_list is not None:
        vm_refs.update(vm_ref_list)
    if vm_uuid_list is not None:
        for vm_uuid in vm_uuid_list:
            vm_ref = vms.get_by_uuid(vm_uuid)
            if vm_ref is not None:
                vm_refs.add(vm_ref)
    if vm_name_list is not None:
        for vm_label in vm_name_list:
            vm_refs.update(vms.get_by_label(vm_label))

    if vm_ref_list is None and vm_uuid_list is None and vm_name_list is None:
        vm_refs.update(vms.get_all_refs())

    if excluded_vms is not None:
        for vm_uuid in excluded_vms:
            vm_ref = vms.get_by_uuid(vm_uuid)
            if vm_ref is not None:
                try:
                    vm_refs.remove(vm_ref)
                except KeyError:
                    pass
    return vm_refs


def vm_definition_to_file(vm_definition, base_folder, vm_back_dir, timestamp):
    backup_def_fn = os.path.join(base_folder, vm_back_dir, timestamp + ".json")
    with open(backup_def_fn, "w") as backup_def_file:
        json.dump(vm_definition, backup_def_file, indent=4, cls=DateTimeEncoder)
    return backup_def_fn


def vm_definition_from_file(backup_def_fn):
    with open(backup_def_fn) as backup_def_file:
        return json.load(backup_def_file)


def timestamp_to_datetime(ts_string, to_str=True, from_format="%Y%m%dT%H:%M:%SZ", to_format="%Y%m%dT%H%M%S"):
    ts = datetime.strptime(ts_string, from_format)
    ts = ts.replace(tzinfo=timezone.utc).astimezone()  # to local timezone
    return ts.strftime(to_format) if to_str else ts


def get_timestamp(to_str=False, to_format="%Y%m%dT%H%M%S"):
    ts = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone()
    return datetime_to_timestamp(ts, to_format) if to_str else ts


def datetime_to_timestamp(dt, to_format="%Y%m%dT%H%M%S"):
    return dt.strftime(to_format)


def random_xen_mac():
    mac = [random.randint(0x00, 0xff) for _ in range(0, 3)] + \
          [random.randint(0x00, 0x7f)] + \
          [random.randint(0x00, 0xff) for _ in range(0, 2)]
    mac[0] = (mac[0] & 0xfc) | 0x02
    return ":".join(['{0:02x}'.format(x) for x in mac])
