import errno
import json
import logging
import os
import shutil
import ssl
from _ssl import CERT_NONE
from urllib import request
from urllib.error import HTTPError

from handlers import vdi, vif
from handlers.common import CommonEntities, get_by_uuid, get_by_label, get_all_refs
from handlers.task import Task
from handlers.vbd import VBD
from handlers.vdi import VDI
from handlers.vif import VIF
from lib.XenAPI import Failure
from lib.functions import get_saned_string, get_timestamp, vm_definition_to_file, vm_definition_from_file

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = CERT_NONE


def generator(vm_refs, xapi, master_url, session_id):
    for vm_ref in vm_refs:
        yield VM(xapi.VM, master_url, session_id, vm_ref)


class VM(CommonEntities):
    backup_snap_prefix = "__backup__"
    _type = "VM"

    def __init__(self, xapi, master_url, session_id, ref=None, params=None):
        super().__init__(xapi, master_url, session_id, ref, params)

    @property
    def vdis(self):
        return getattr(self._xapi, "VDI")

    @property
    def vbds(self):
        return getattr(self._xapi, "VBD")

    def get_vm_back_dir(self):
        return "vm_" + self.get_uuid()

    def is_control_domain(self):
        return self.xapi.get_is_control_domain(self.ref)

    def get_power_state(self):
        return self.xapi.get_power_state(self.ref)

    def is_running(self):
        return self.get_power_state() == 'Running'

    def is_halted(self):
        return self.get_power_state() == 'Halted'

    def is_paused(self):
        return self.get_power_state() == 'Paused'

    def is_suspended(self):
        return self.get_power_state() == 'Suspended'

    def can_export(self):
        return "export" in self.xapi.get_allowed_operations(self.ref)

    def start(self, paused=False, force=False):
        self.xapi.start(self.ref, paused, force)

    def shutdown(self):
        self.logger.debug("Shutting down VM")
        self.xapi.shutdown(self.ref)

    def pause(self):
        self.xapi.pause(self.ref)

    def unpause(self):
        self.xapi.unpause(self.ref)

    def suspend(self):
        self.xapi.suspend(self.ref)

    def resume(self):
        self.xapi.resume(self.ref)

    def set_power_state(self, power_state):
        if power_state != self.get_power_state():
            if power_state == "Halted":
                self.shutdown()
            else:
                if self.is_halted():
                    self.start()
                elif self.is_paused():
                    self.unpause()
                elif self.is_suspended():
                    self.resume()

                if power_state == "Paused":
                    self.pause()
                elif power_state == "Suspended":
                    self.suspend()

    def set_name(self, name):
        self.xapi.set_name_label(self.ref, name)

    def set_is_template(self, is_template):
        self.xapi.set_is_a_template(self.ref, is_template)

    def get_vifs(self):
        return (VIF(self._xapi, vif_ref) for vif_ref in self.xapi.get_VIFs(self.ref))

    def get_vbds(self):
        return (
            VBD(self._xapi, vbd_ref) for vbd_ref in self.xapi.get_VBDs(self.ref)
        )

    def get_vdis(self, disk_only=False):
        return (
            VDI(self._xapi, self.master_url, self.session_id, vbd.get_vdi_ref()) for vbd in self.get_vbds()
            if vbd.get_vdi_ref(disk_only) is not None
        )

    def destroy(self, keep_vdis=None):
        vdis = self.get_vdis(disk_only=True)

        for vdi in vdis:
            if keep_vdis is None or vdi.ref not in keep_vdis:
                vdi.destroy()

        super().destroy()

    def export(self, base_back_dir, base_vm_name=None, vm_name=None, clean_on_failure=True):
        if vm_name is None:
            vm_name = (self.get_snapshot_of() if self.is_snapshot() else self).get_label()
        if base_vm_name is None:
            base_vm_name = "{}__{}__{}".format(
                self.get_snapshot_of().get_uuid() if self.is_snapshot() else self.get_uuid(),
                self.get_snapshot_time() if self.is_snapshot() else get_timestamp(),
                get_saned_string(vm_name)
            )

        file_name = base_vm_name + ".xva"
        full_file_name = os.path.join(base_back_dir, file_name)

        desc = self.export_template.format("full", "VM", vm_name, full_file_name)
        self.logger.debug(desc)

        task = Task(self._xapi, params=[vm_name + " export", desc])

        url = "{}/export?session_id={}&task_id={}&ref={}&use_compression=true".format(
            self.master_url, self.session_id, task.ref, self.ref)

        try:
            with request.urlopen(url, context=ctx) as response, open(full_file_name, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
        except (HTTPError, IOError, SystemExit) as e:
            self.logger.error("VM export failed: %s", e)
            try:
                task.cancel()
            except Failure:
                self.logger.exception("Error cancelling export task")
            if os.path.exists(full_file_name) and clean_on_failure:
                try:
                    os.remove(full_file_name)
                except IOError:
                    self.logger.exception("Error deleting failed VM export file %s", full_file_name)
            raise e

        self.logger.debug("VM %s export completed", vm_name)

        return full_file_name

    def get_backup_snapshots(self, snap_type=""):
        return (snapshot for snapshot in self.get_snapshots()
                if snapshot.get_label().startswith(self.backup_snap_prefix + snap_type))

    def backup_snapshot(self, name=""):
        separator = "__" if name else ""

        backup_snap = self.snapshot("{}{}{}{}".format(
            self.backup_snap_prefix, name, separator, self.get_label()))

        return backup_snap

    def backup(self, failed_vms, base_folder, num_vm=1, num_vms=1, backup_new_snap=True):
        vm_uuid = self.get_uuid()
        vm_name = self.get_label()

        backup_filename = None

        self.logger.info("VM (%d of %d) '%s' --- Performing full backup", num_vm, num_vms, vm_name)

        delete_snapshot = False
        vm_backup_snaps = self.get_backup_snapshots("base")
        backup_snap = next(vm_backup_snaps, None)
        if backup_snap is None or backup_new_snap:
            backup_snap = self.backup_snapshot("full_tmp")
            delete_snapshot = True

        backup_name = backup_snap.get_label()
        backup_new_name = "{} - backup {}".format(
            vm_name, backup_snap.get_snapshot_time(ts_format="%Y-%m-%d %H:%M:%S"))

        backup_snap.set_is_template(False)
        backup_snap.set_name(backup_new_name)

        try:
            backup_filename = backup_snap.export(base_folder, vm_name=vm_name)
        except Failure as e:
            failed_vms.update({self.ref: self.export_error_template.format("VM", vm_name, vm_uuid, "XenAPI", e)})
            self.logger.error("XenApi error: %s", str(e))
        except HTTPError as e:
            failed_vms.update({self.ref: self.export_error_template.format("VM", vm_name, vm_uuid, "HTTP", e)})
            self.logger.error("HTTP error: %s", str(e))
        except IOError as e:
            failed_vms.update({self.ref: self.export_error_template.format("VM", vm_name, vm_uuid, "Storage", e)})
            if e.errno == errno.ENOSPC:
                raise e
            else:
                self.logger.error("Storage error: %s", str(e))
        except SystemExit as e:
            failed_vms.update(
                {self.ref: self.export_error_template.format("VM", vm_name, vm_uuid, "Generic", "interrupt")})
            raise e
        else:
            self.logger.info("Full backup of VM %s successfully completed", vm_name)
        finally:
            if delete_snapshot:
                backup_snap.destroy()
            else:
                backup_snap.set_name(backup_name)

        return backup_filename

    # Perform delta backup of a single VM
    def backup_delta(self, failed_vms, base_folder, num_vm=1, num_vms=1):
        vm_uuid = self.get_uuid()
        vm_name = self.get_label()

        base_backup_snap = None
        backup_vdis_map = None

        # Check if we are doing a base delta (full) or delta backup
        vm_backup_snaps = self.get_backup_snapshots("base")
        backup_snap = next(vm_backup_snaps, None)

        if backup_snap is not None:
            base_backup_snap = backup_snap
            self.logger.info("VM (%d of %d) '%s' --- Performing delta backup", num_vm + 1, num_vms, vm_name)
            backup_snap = self.backup_snapshot("delta_tmp")
            # Map used to check whether perform a delta or full VDI backup
            backup_vdis_map = {
                vdi.get_snapshot_of().ref: vdi for vdi in base_backup_snap.get_vdis(disk_only=True)
            }
        else:
            self.logger.info("VM (%d of %d) '%s' --- Performing base delta (a.k.a. full) backup",
                             num_vm + 1, num_vms, vm_name)
            backup_snap = self.backup_snapshot("base")

        backup_record = backup_snap.get_record()
        backup_record["is_a_template"] = False
        backup_record["name_label"] = "{} - backup {}".format(
            vm_name, backup_snap.get_snapshot_time(ts_format="%Y-%m-%d %H:%M"))

        backup_vifs = {vif.ref: vif.get_record_with_ntw_label() for vif in backup_snap.get_vifs()}
        backup_vbds = {}
        backup_vdis = {}
        retain_vdis = {}

        vm_back_dir = self.get_vm_back_dir()
        os.makedirs(os.path.join(base_folder, vm_back_dir), 0o755, True)

        try:
            for vbd in backup_snap.get_vbds():
                backup_vbds[vbd.ref] = vbd.get_record()
                vdi_ref = vbd.get_vdi_ref(disk_only=True)
                if vdi_ref is not None:
                    vdi = VDI(self._xapi, self.master_url, self.session_id, vdi_ref)
                    vdi_record = vdi.backup(base_folder, vm_back_dir, backup_vdis_map)
                    backup_vdis[vdi.ref] = vdi_record

                    if backup_vdis_map is not None and vdi.get_snapshot_of().ref not in backup_vdis_map:
                        vbd_record = vbd.get_record()
                        vbd_record["VM"] = base_backup_snap.ref
                        vbd_record["VDI"] = vdi.ref

                        retain_vdis[vdi.ref] = vbd_record

        # except XenAPI.Failure as e:
        #     failed_vms.update{vm: self.error_template.format("VDI of VM", vm_name, vm_uuid, "XenAPI", e)}
        #     self.logger.exception("XenApi error")
        except HTTPError as e:
            failed_vms.update(
                {self.ref: self.export_error_template.format("VDI of VM", vm_name, vm_uuid, "HTTP", str(e))})
            self.logger.error("HTTP error %s", str(e))
        except IOError as e:
            failed_vms.update(
                {self.ref: self.export_error_template.format("VDI of VM", vm_name, vm_uuid, "Storage", str(e))})
            if e.errno == errno.ENOSPC:
                raise e
            else:
                self.logger.error("Storage error %s", str(e))
        except SystemExit as e:
            failed_vms.update(
                {self.ref: self.export_error_template.format("VDI of VM", vm_name, vm_uuid, "Generic", "interrupt")})
            raise e
        else:
            with open(os.path.join(base_folder, vm_back_dir, self.get_label_sane()), "w") as vm_name_file:
                vm_name_file.write(vm_name)

            vm_definition = {
                "vm": backup_record,
                "vbds": backup_vbds,
                "vdis": backup_vdis,
                "vifs": backup_vifs
            }
            vm_def_fn = vm_definition_to_file(
                vm_definition, base_folder, vm_back_dir, backup_snap.get_snapshot_time())
            self.logger.info("Backup of VM %s completed", vm_name)
            return vm_back_dir, vm_def_fn
        finally:
            if self.ref in failed_vms:
                self.logger.error("Error during backup. %s", failed_vms[self.ref])
                retain_vdis = {}
                for backup_vdi_record in backup_vdis.values():
                    vdi_file = os.path.join(base_folder, backup_vdi_record["backup_file"])
                    try:
                        os.remove(vdi_file)
                    except OSError:
                        self.logger.exception("Error removing VDI file %s", vdi_file)

            # is delta backup
            if backup_vdis_map is not None:
                try:
                    backup_snap.destroy(retain_vdis.keys())
                except Failure:
                    self.logger.exception("Error destroying delta snapshot")
                for vbd_record in retain_vdis.values():
                    try:
                        # create new VBD
                        VBD(self._xapi, params=vbd_record)
                    except Failure:
                        self.logger.exception("Error creating new VBD for missing backup VDI")

    def clean_backups(self, base_folder, num_backups_to_retain):
        vm_uuid = self.get_uuid()
        vm_files = [vm_file for vm_file in os.listdir(base_folder) if
                    vm_file.startswith(vm_uuid) and vm_file.endswith(".xva")]
        vm_files.sort()

        vm_files_discard = vm_files[:-num_backups_to_retain]

        for vm_file in vm_files_discard:
            try:
                os.remove(os.path.join(base_folder, vm_file))
            except IOError as e:
                self.logger.error("Error deleting VM file %s %s", vm_file, str(e))
            else:
                self.logger.debug("VM file %s deleted", vm_file)

    def clean_delta_backups(self, base_folder, num_backups_to_retain):
        vm_back_dir = self.get_vm_back_dir()
        vm_def_files = [vm_file for vm_file in os.listdir(os.path.join(base_folder, vm_back_dir)) if
                        vm_file.endswith(".json")]
        vm_def_files.sort()

        vm_def_files_discard = vm_def_files[:-num_backups_to_retain]

        used_base_vdis = []

        for vm_def_file in vm_def_files:
            discard = vm_def_file in vm_def_files_discard
            with open(os.path.join(base_folder, vm_back_dir, vm_def_file)) as vm_file:
                for vdi_record in json.load(vm_file)["vdis"].values():
                    used_base_vdis = used_base_vdis + vdi.clean(vdi_record, base_folder, discard)

            if discard:
                try:
                    os.remove(os.path.join(base_folder, vm_back_dir, vm_def_file))
                except IOError as e:
                    self.logger.error("Error deleting VM definition file %s %s", vm_def_file, str(e))
                else:
                    self.logger.debug("VM definition file '%s' deleted", os.path.join(vm_back_dir, vm_def_file))

        vdi.clean_unused(os.path.join(base_folder, vm_back_dir), used_base_vdis)


"""
    Restore functions
"""


def restore(xapi, master_url, session_id, file_name, sr_map=None, auto_start=False, restore=False):
    logger = logging.getLogger("VM")

    backup_name = os.path.basename(file_name)[:-4]
    try:
        vm_uuid, backup_ts, vm_name = backup_name.split("__")
    except ValueError:
        vm_uuid = None
        vm_name = backup_name

    logger.info("Restoring VM %s", vm_name)

    task = Task(xapi, params=[vm_name + " VM import", "Importing full VM " + vm_name])

    url = "{}/import?session_id={}&task_id={}".format(master_url, session_id, task.ref)
    if sr_map is not None and vm_uuid in sr_map:
        url = url + "&sr_id=" + sr_map[vm_uuid]
    if restore:
        url = url + "&restore=true"

    try:
        with open(file_name, 'rb') as vm_file:
            req = request.Request(url, data=vm_file, method="PUT")
            req.add_header("Content-Length", str(os.path.getsize(file_name)))
            req.add_header("content-type", "application/octet-stream")
            request.urlopen(req, context=ctx)
    except (HTTPError, IOError, SystemExit) as e:
        logger.error("VM restore failed: %s", e)
        try:
            task.cancel()
        except Failure as f:
            logger.error("Error cancelling export task %s", f.details)
        raise e
    else:
        logger.info("VM %s restored", vm_name)


def restore_delta(xapi, master_url, session_id, vm_def_file, base_folder, sr_map=None, network_map=None,
                  auto_start=False, restore=False):
    logger = logging.getLogger("VM")

    vm_definition = vm_definition_from_file(vm_def_file)

    logger.info("Restoring VM %s", vm_definition["vm"]["name_label"])
    vm = VM(xapi, master_url, session_id, params=vm_definition["vm"])

    try:
        for vbd_record in vm_definition["vbds"].values():
            if vbd_record["VDI"] != "OpaqueRef:NULL":
                vdi_ref = vdi.restore(
                    xapi, master_url, session_id, vm_definition["vdis"][vbd_record["VDI"]], base_folder, sr_map)
                vbd_record["VDI"] = vdi_ref

            vbd_record["VM"] = vm.ref
            VBD(xapi, params=vbd_record)

        for vif_ref, vif_record in vm_definition["vifs"].items():
            vif_record["VM"] = vm.ref
            vif.restore(xapi, vif_record, network_map, restore)

    except (HTTPError, IOError, SystemExit, Failure) as e:
        logger.error("Error restoring VM '%s' %s", vm.get_label(), str(e))
        vm.destroy()
        raise e
    else:
        if auto_start:
            vm.start(False, False)

    logger.info("VM %s restore completed", vm.get_label())

    return vm.ref


def get_all_vm_refs(xapi, template=False, snapshot=False, control_domain=False):
    vm_xapi = xapi.VM
    return [
        vm_ref for vm_ref in get_all_refs(vm_xapi)
        if (snapshot or not vm_xapi.get_is_a_snapshot(vm_ref))
           and (template or not vm_xapi.get_is_a_template(vm_ref))
           and (control_domain or not vm_xapi.get_is_control_domain(vm_ref))
    ]


def get_vms_to_backup(xapi, master_url, session_id,
                      excluded_vms=None, vm_ref_list=None, vm_uuid_list=None, vm_name_list=None):
    vm_refs = set()

    if vm_ref_list is not None:
        vm_refs.update(vm_ref_list)
    if vm_uuid_list is not None:
        for vm_uuid in vm_uuid_list:
            try:
                vm_ref = get_by_uuid(xapi.VM, vm_uuid)
            except Failure:
                pass
            else:
                vm_refs.add(vm_ref)
    if vm_name_list is not None:
        for vm_label in vm_name_list:
            vm_refs.update(get_by_label(xapi.VM, vm_label))

    if vm_ref_list is None and vm_uuid_list is None and vm_name_list is None:
        vm_refs.update(get_all_vm_refs(xapi))

    if excluded_vms is not None:
        for vm_uuid in excluded_vms:
            vm_ref = get_by_uuid(xapi.VM, vm_uuid)
            if vm_ref is not None:
                try:
                    vm_refs.remove(vm_ref)
                except KeyError:
                    pass

    return (VM(xapi, master_url, session_id, vm_ref) for vm_ref in vm_refs), len(vm_refs)
