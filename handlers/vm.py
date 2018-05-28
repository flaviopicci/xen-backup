import errno
import json
import os
import shutil
import ssl
import time
import urllib.request
from _ssl import CERT_NONE
from urllib.error import HTTPError

from handlers.common import CommonHandler
from handlers.pool import PoolHandler
from handlers.task import TaskHandler
from handlers.vbd import VbdHandler
from handlers.vdi import VdiHandler
from handlers.vif import VifHandler
from lib.XenAPI import Failure
from lib.functions import vm_definition_to_file, vm_definition_from_file, timestamp_to_datetime, get_saned_string, \
    get_timestamp

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = CERT_NONE


class VMHandler(CommonHandler):
    backup_snap_prefix = "__backup__"
    _export_retries = 2

    def __init__(self, xapi, master_url, session_id, vif_handler=None, vbd_handler=None, vdi_handler=None,
                 task_handler=None):
        super().__init__(xapi.VM, master_url, session_id)

        self.task = task_handler if task_handler is not None else TaskHandler(xapi)
        self.vif = vif_handler if vif_handler is not None else VifHandler(xapi)
        self.vbd = vbd_handler if vbd_handler is not None else VbdHandler(xapi)
        self.vdi = vdi_handler if vdi_handler is not None else VdiHandler(
            xapi, master_url, session_id, self.vbd, self.task)

    def get_all_refs(self, template=False, snapshot=False, control_domain=False):
        return [
            vm_ref for vm_ref in self.xapi.get_all()
            if (snapshot or not self.xapi.get_is_a_snapshot(vm_ref))
               and (template or not self.xapi.get_is_a_template(vm_ref))
               and (control_domain or not self.xapi.get_is_control_domain(vm_ref))
        ]

    def get_by_label(self, vm_label):
        return [
            vm_ref for vm_ref in self.xapi.get_by_name_label(vm_label)
            if not self.xapi.get_is_control_domain(vm_ref)
        ]

    def get_backup_snapshots(self, vm_ref, snap_type=""):
        return [
            snap_ref for snap_ref in self.get_snapshots(vm_ref)
            if self.xapi.get_name_label(snap_ref).startswith(self.backup_snap_prefix + snap_type)
        ]

    def get_vbds(self, vm_ref, disk_only=False):
        if disk_only:
            return [
                vbd_ref for vbd_ref in self.xapi.get_VBDs(vm_ref) if
                not (disk_only and self.vbd.get_type(vbd_ref) != "Disk")
            ]
        else:
            return self.xapi.get_VBDs(vm_ref)

    def get_vdis(self, vm_ref, disk_only=False, with_vbd=False):
        if with_vbd:
            return {self.vbd.get_vdi(vbd_ref): vbd_ref for vbd_ref in self.get_vbds(vm_ref, disk_only)
                    if self.vbd.get_vdi(vbd_ref) != "OpaqueRef:NULL"}
        else:
            return [self.vbd.get_vdi(vbd_ref) for vbd_ref in self.get_vbds(vm_ref, disk_only)
                    if self.vbd.get_vdi(vbd_ref) != "OpaqueRef:NULL"]

    def get_vifs(self, vm_ref):
        return self.xapi.get_VIFs(vm_ref)

    def get_power_state(self, vm_ref):
        return self.xapi.get_power_state(vm_ref)

    def is_running(self, vm_ref):
        return self.get_power_state(vm_ref) == 'Running'

    def is_halted(self, vm_ref):
        return self.get_power_state(vm_ref) == 'Halted'

    def is_paused(self, vm_ref):
        return self.get_power_state(vm_ref) == 'Paused'

    def is_suspended(self, vm_ref):
        return self.get_power_state(vm_ref) == 'Suspended'

    def can_export(self, vm_ref):
        return "export" in self.xapi.get_allowed_operations(vm_ref)

    def start(self, vm_ref, paused=False, force=False):
        self.xapi.start(vm_ref, paused, force)

    def shutdown(self, vm_ref):
        self.logger.debug("Shutting down VM")
        self.xapi.shutdown(vm_ref)

    def pause(self, vm_ref):
        self.xapi.pause(vm_ref)

    def unpause(self, vm_ref):
        self.xapi.unpause(vm_ref)

    def suspend(self, vm_ref):
        self.xapi.suspend(vm_ref)

    def resume(self, vm_ref):
        self.xapi.resume(vm_ref)

    def set_power_state(self, vm_ref, power_state):
        if power_state != self.get_power_state(vm_ref):
            if power_state == "Halted":
                self.shutdown(vm_ref)
            else:
                if self.is_halted(vm_ref):
                    self.start(vm_ref)
                elif self.is_paused(vm_ref):
                    self.unpause(vm_ref)
                elif self.is_suspended(vm_ref):
                    self.resume(vm_ref)

                if power_state == "Paused":
                    self.pause(vm_ref)
                elif power_state == "Suspended":
                    self.suspend(vm_ref)

    def destroy(self, vm_ref, keep_vdis=None):
        self.logger.debug("Destroying %s '%s'", self.type, self.get_label(vm_ref))
        vdi_refs = self.get_vdis(vm_ref, disk_only=True)

        self.xapi.destroy(vm_ref)

        for vdi in vdi_refs:
            if keep_vdis is None or vdi not in keep_vdis:
                done = False
                max_retries = 3
                while not done and max_retries > 0:
                    try:
                        self.vdi.destroy(vdi)
                    except Failure:
                        max_retries = max_retries - 1
                        self.logger.warning(
                            "Error destroying VDI%s", ". Retrying in 5 seconds" if max_retries > 0 else "")
                        time.sleep(5)
                    else:
                        done = True

    def set_is_template(self, vm_ref, is_template):
        self.xapi.set_is_a_template(vm_ref, is_template)

    def set_name(self, vm_ref, name):
        self.xapi.set_name_label(vm_ref, name)

    def backup_snapshot(self, vm_ref, name=""):
        assert vm_ref is not None

        separator = "__" if name else ""

        backup_ref = self.snapshot(vm_ref, "{}{}{}{}".format(
            self.backup_snap_prefix, name, separator, self.get_label(vm_ref)))

        return backup_ref

    def backup_vdis_snapshot(self, vm_ref):
        assert vm_ref is not None

        return {
            vm_vdi: self.vdi.snapshot(vm_vdi) for vm_vdi in self.get_vdis(vm_ref)
        }

    def export(self, vm_ref, base_back_dir, base_vm_name=None, vm_name=None, clean_on_failure=True):
        export_retries = self._export_retries
        export_done = False

        is_snapshot = self.xapi.get_is_a_snapshot(vm_ref)
        if vm_name is None:
            vm_name = self.get_label(self.get_snapshot_of(vm_ref) if is_snapshot else vm_ref)
        if base_vm_name is None:
            base_vm_name = "{}__{}__{}".format(
                self.get_uuid(self.get_snapshot_of(vm_ref)) if is_snapshot else self.get_uuid(vm_ref),
                self.get_snapshot_time(vm_ref) if is_snapshot else get_timestamp(),
                get_saned_string(vm_name))

        file_name = base_vm_name + ".xva"
        full_file_name = os.path.join(base_back_dir, file_name)

        self.logger.debug("Exporting VM '%s' to '%s'", vm_name, full_file_name)

        task_ref = self.task.create(vm_name + " export", "Exporting full VM " + vm_name + " to " + base_back_dir)

        url = "{}/export?session_id={}&task_id={}&ref={}&use_compression=true".format(
            self.master_url, self.session_id, task_ref, vm_ref)

        while not export_done:
            export_retries -= 1
            try:
                with urllib.request.urlopen(url, context=ctx) as response, open(full_file_name, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
            except (HTTPError, IOError, SystemExit) as e:
                try:
                    self.task.cancel(task_ref)
                except Failure:
                    self.logger.error("Error cancelling export task")
                if os.path.exists(full_file_name) and clean_on_failure:
                    try:
                        os.remove(full_file_name)
                    except IOError:
                        self.logger.error("Error deleting failed VM export file %s", full_file_name)
                # Retry if error is IOError but no insufficient space
                if isinstance(e, IOError) and e.errno != errno.ENOSPC and export_retries > 0:
                    self.logger.warning("Error exporting VM %s (%s). Retrying", vm_name, e)
                else:
                    self.logger.error("VM export failed: %s", e)
                    raise e
            else:
                export_done = True

        self.logger.debug("VM %s export completed", vm_name)

        return full_file_name

    def export_meta(self, vm_ref, file_name):
        url = "{}/export_metadata?session_id={}&ref={}".format(self.master_url, self.session_id, vm_ref)

        with urllib.request.urlopen(url, context=ctx) as response, open(file_name + ".xml", 'wb') as out_file:
            shutil.copyfileobj(response, out_file)

    def clean_backups(self, vm_ref, base_folder, num_backups_to_retain):
        vm_uuid = self.get_uuid(vm_ref)
        vm_files = [vm_file for vm_file in os.listdir(base_folder) if
                    vm_file.startswith(vm_uuid) and vm_file.endswith(".xva")]
        vm_files.sort()

        vm_files_discard = vm_files[:-num_backups_to_retain]

        for vm_file in vm_files_discard:
            try:
                os.remove(os.path.join(base_folder, vm_file))
            except IOError as e:
                self.logger.error("Error deleteting VM file %s %s", vm_file, e.strerror)
            else:
                self.logger.debug("VM file %s deleted", vm_file)

    def clean_delta_backups(self, vm_ref, base_folder, num_backups_to_retain):
        vm_back_dir = self.get_vm_back_dir(vm_ref)
        vm_def_files = [vm_file for vm_file in os.listdir(os.path.join(base_folder, vm_back_dir)) if
                        vm_file.endswith(".json")]
        vm_def_files.sort()

        vm_def_files_discard = vm_def_files[:-num_backups_to_retain]

        used_base_vdis = []

        for vm_def_file in vm_def_files:
            discard = vm_def_file in vm_def_files_discard
            with open(os.path.join(base_folder, vm_back_dir, vm_def_file)) as vm_file:
                for vdi_record in json.load(vm_file)["vdis"].values():
                    used_base_vdis = used_base_vdis + self.vdi.clean(vdi_record, base_folder, discard)
            if discard:
                try:
                    os.remove(os.path.join(base_folder, vm_back_dir, vm_def_file))
                except IOError as e:
                    self.logger.error("Error deleteting VM definition file %s %s", vm_def_file, e.strerror)
                else:
                    self.logger.debug("VM definition file '%s' deleted", os.path.join(vm_back_dir, vm_def_file))

        self.vdi.clean_unused(os.path.join(base_folder, vm_back_dir), used_base_vdis)

    def get_vm_back_dir(self, vm_ref=None, vm_uuid=None):
        assert vm_ref is not None or vm_uuid is not None
        return "vm_" + (self.get_uuid(vm_ref) if vm_ref is not None else vm_uuid)

    def backup(self, vm_ref, failed_vms, base_folder, num_vm=1, num_vms=1, backup_new_snap=True):
        vm_uuid = self.get_uuid(vm_ref)
        vm_name = self.get_label(vm_ref)
        backup_filename = None

        self.logger.info("VM (%d of %d) '%s' --- Performing full backup", num_vm, num_vms, vm_name)

        delete_snapshot = False
        vm_backup_snaps = self.get_backup_snapshots(vm_ref, "base")
        if len(vm_backup_snaps) == 0 or backup_new_snap:
            backup_ref = self.backup_snapshot(vm_ref, "full_tmp")
            delete_snapshot = True
        else:
            backup_ref = vm_backup_snaps[0]

        backup_name = self.get_label(backup_ref)
        backup_new_name = "{} - backup {}".format(
            vm_name, self.get_snapshot_time(backup_ref, ts_format="%Y-%m-%d %H:%M:%S"))

        self.set_is_template(backup_ref, False)
        self.set_name(backup_ref, backup_new_name)

        try:
            backup_filename = self.export(backup_ref, base_folder, vm_name=vm_name)
        except Failure as e:
            failed_vms.update({vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "XenAPI", e)})
            self.logger.error("XenApi error: %s", e.details)
        except HTTPError as e:
            failed_vms.update({vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "HTTP", e)})
            self.logger.error("HTTP error: %s", e.details)
        except IOError as e:
            failed_vms.update({vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "Storage", e)})
            if e.errno == errno.ENOSPC:
                raise e
            else:
                self.logger.error("Storage error: %s", e.strerror)
        except SystemExit as e:
            failed_vms.update(
                {vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "Generic", "interrupt")})
            raise e
        else:
            self.logger.info("Full backup of VM %s successfully completed", vm_name)
        finally:
            if delete_snapshot:
                self.destroy(backup_ref)
            else:
                self.set_name(backup_ref, backup_name)

        return backup_filename

    # Perform delta backup of a single VM
    def backup_delta(self, vm_ref, failed_vms, base_folder, num_vm=1, num_vms=1):
        vm_uuid = self.get_uuid(vm_ref)
        vm_name = self.get_label(vm_ref)

        base_backup_ref = None
        backup_vdis_map = None

        # Check if we are doing a base delta (full) or delta backup
        vm_backup_snaps = self.get_backup_snapshots(vm_ref, "base")
        if len(vm_backup_snaps) > 0:
            base_backup_ref = vm_backup_snaps[0]
            self.logger.info("VM (%d of %d) '%s' --- Performing delta backup", num_vm + 1, num_vms, vm_name)
            backup_ref = self.backup_snapshot(vm_ref, "delta_tmp")
            backup_vdis_map = {
                self.vdi.get_snapshot_of(vdi_ref): vdi_ref for vdi_ref in self.get_vdis(base_backup_ref, disk_only=True)
            }
        else:
            self.logger.info("VM (%d of %d) '%s' --- Performing base delta (a.k.a. full) backup", num_vm + 1, num_vms,
                             vm_name)
            backup_ref = self.backup_snapshot(vm_ref, "base")

        backup_record = self.get_record(backup_ref)
        backup_record["is_a_template"] = False
        backup_record["name_label"] = "{} - backup {}".format(
            vm_name, self.get_snapshot_time(backup_ref, ts_format="%Y-%m-%d %H:%M:%S"))

        backup_vifs = {vif_ref: self.vif.get_record_with_ntw_label(vif_ref) for vif_ref in self.get_vifs(backup_ref)}
        backup_vbds = {vbd_ref: self.vbd.get_record(vbd_ref) for vbd_ref in self.get_vbds(backup_ref)}
        backup_vdis = {}
        retain_vdis = {}

        vm_back_dir = self.get_vm_back_dir(vm_uuid=vm_uuid)
        os.makedirs(os.path.join(base_folder, vm_back_dir), 0o755, True)

        try:
            for vdi_ref, vbd_ref in self.get_vdis(backup_ref, disk_only=True, with_vbd=True).items():
                vdi_record, vbd_record = self.vdi.backup(
                    vdi_ref, vbd_ref, base_folder, vm_back_dir, base_backup_ref, backup_vdis_map)

                backup_vdis[vdi_ref] = vdi_record
                if vbd_record is not None:
                    retain_vdis[vdi_ref] = vbd_record
        # except XenAPI.Failure as e:
        #     failed_vms.update{vm: self.error_template.format("VDI of VM", vm_name, vm_uuid, "XenAPI", e)}
        #     self.logger.exception("XenApi error")
        except HTTPError as e:
            failed_vms.update({vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "HTTP", e.reason)})
            self.logger.error("HTTP error %s", e.reason)
        except IOError as e:
            failed_vms.update(
                {vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "Storage", e.strerror)})
            if e.errno == errno.ENOSPC:
                raise e
            else:
                self.logger.error("Storage error %s", e.strerror)
        except SystemExit as e:
            failed_vms.update(
                {vm_ref: self.error_template.format("VDI of VM", vm_name, vm_uuid, "Generic", "interrupt")})
            raise e
        else:
            with open(os.path.join(base_folder, vm_back_dir, self.get_label_sane(vm_ref)), "w") as vm_name_file:
                vm_name_file.write(vm_name)

            vm_definition = {
                "vm": backup_record,
                "vbds": backup_vbds,
                "vdis": backup_vdis,
                "vifs": backup_vifs
            }
            vm_def_fn = vm_definition_to_file(
                vm_definition, base_folder, vm_back_dir, self.get_snapshot_time(backup_ref))
            self.logger.info("Backup of VM %s completed", vm_name)
            return vm_back_dir, vm_def_fn
        finally:
            if vm_ref in failed_vms:
                self.logger.error("Error during backup. %s", failed_vms[vm_ref])
                retain_vdis = {}
                for backup_vdi_record in backup_vdis.values():
                    vdi_file = os.path.join(base_folder, backup_vdi_record["backup_file"])
                    try:
                        os.remove(vdi_file)
                    except OSError:
                        self.logger.exception("Error removing VDI file %s", vdi_file)

            if backup_vdis_map is not None:
                try:
                    self.destroy(backup_ref, retain_vdis.keys())
                except Failure:
                    self.logger.exception("Error destroying delta snapshot")
                for vbd_record in retain_vdis.values():
                    try:
                        self.vbd.create(vbd_record)
                    except Failure:
                        self.logger.exception("Error creating new VBD for missing backup VDI")

    def restore(self, file_name, sr_map=None, auto_start=False, restore=False):
        backup_name = os.path.basename(file_name)[:-4]
        try:
            vm_uuid, backup_ts, vm_name = backup_name.split("__")
        except ValueError:
            vm_uuid = None
            backup_ts = None
            vm_name = backup_name

        self.logger.info("Restoring VM %s", vm_name)

        task_ref = self.task.create(vm_name + " VM import", "Importing full VM " + vm_name)

        url = "{}/import?session_id={}&task_id={}".format(self.master_url, self.session_id, task_ref)
        if sr_map is not None and vm_uuid in sr_map:
            url = url + "&sr_id=" + sr_map[vm_uuid]
        if restore:
            url = url + "&restore=true"

        try:
            with open(file_name, 'rb') as vm_file:
                request = urllib.request.Request(url, data=vm_file, method="PUT")
                request.add_header("Content-Length", str(os.path.getsize(file_name)))
                request.add_header("content-type", "application/octet-stream")
                result = urllib.request.urlopen(request, context=ctx)
        except (HTTPError, IOError, SystemExit) as e:
            self.logger.error("VM restore failed: %s", e)
            try:
                self.task.cancel(task_ref)
            except Failure as f:
                self.logger.error("Error cancelling export task %s", f.details)
            raise e

            # vm = self.get_by_label(backup_name)
            # if len(vm) > 0:
            #     vm = vm[0]
            #     self.set_name_label(vm, "{} - backup {}".format(
            #         vm_name, timestamp_to_datetime(backup_ts, from_format="%Y%m%dT%H%M%SZ", to_format="%Y-%m-%d %H:%M:%S")))
            #     self.set_is_template(vm, False)
            #
            #     if auto_start:
            #         self.xapi.start(vm, False, False)
            #
            #     self.logger.debug("VM %s restore completed", vm_name)
            #
            #     return vm
            # else:
            #     self.logger.warning("Cannot get imported VM reference. You have to manually set it up.")

    def restore_delta(self, vm_def_file, base_folder, sr_map=None, network_map=None, auto_start=False, restore=False):
        vm_definition = vm_definition_from_file(vm_def_file)

        self.logger.info("Restoring VM %s", vm_definition["vm"]["name_label"])
        vm_ref = self.create(vm_definition["vm"])
        try:
            for vbd_record in vm_definition["vbds"].values():
                if vbd_record["VDI"] != "OpaqueRef:NULL":
                    vdi_ref = self.vdi.restore(vm_definition["vdis"][vbd_record["VDI"]], base_folder, sr_map)
                    vbd_record["VDI"] = vdi_ref

                vbd_record["VM"] = vm_ref
                self.vbd.create(vbd_record)

            for vif_ref, vif_record in vm_definition["vifs"].items():
                vif_record["VM"] = vm_ref
                self.vif.restore(vif_record, network_map, restore)

        except (HTTPError, IOError, SystemExit, Failure) as e:
            self.logger.error("Error restoring VM '%s' %s", self.get_label(vm_ref), e.strerror)
            self.destroy(vm_ref)
            raise e
        else:
            if auto_start:
                self.xapi.start(vm_ref, False, False)

        self.logger.info("VM %s restore completed", self.get_label(vm_ref))

        return vm_ref
