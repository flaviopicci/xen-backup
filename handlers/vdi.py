import errno
import re
import shutil
import ssl
import urllib.request
from _ssl import CERT_NONE

import os
from urllib.error import HTTPError

from handlers.common import CommonHandler
from handlers.pool import PoolHandler
from handlers.task import TaskHandler
from handlers.vbd import VbdHandler
from lib.XenAPI import Failure

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = CERT_NONE


class VdiHandler(CommonHandler):
    vdi_file_format = "vhd"
    full_extension = "full." + vdi_file_format
    _export_retries = 3

    def __init__(self, xapi, master_url, session_id, vbd_handler=None, task_handler=None,
                 sr_handler=None, pool_handler=None):
        super().__init__(xapi.VDI, master_url, session_id)

        self.pool = pool_handler if pool_handler is not None else PoolHandler(xapi)
        self.sr = sr_handler if sr_handler is not None else CommonHandler(xapi.SR)
        self.vbd = vbd_handler if vbd_handler is not None else VbdHandler(xapi)
        self.task = task_handler if task_handler is not None else TaskHandler(xapi)

    def get_type(self, vdi_ref):
        return self.xapi.get_type(vdi_ref)

    def get_vbds(self, vdi_ref):
        self.xapi.get_VBDs(vdi_ref)

    def get_orphans(self):
        regex = re.compile("^(base copy|.*\.(ISO|iso|img))$")
        return [
            vdi_ref for vdi_ref in self.get_all_refs() if
            self.xapi.get_type(vdi_ref) == 'user'
            and len(self.xapi.get_VBDs(vdi_ref)) == 0
            and "destroy" in self.xapi.get_allowed_operations(vdi_ref)
            and not regex.match(self.get_label(vdi_ref))
        ]

    def get_export_file(self, vdi_ref, vdi_back_dir, export_type="full"):
        return os.path.join(
            vdi_back_dir, "{}_{}.{}".format(self.get_snapshot_time(vdi_ref), export_type, self.vdi_file_format))

    def export(self, vdi_ref, base_back_dir, vdi_back_dir, base_vdi=None, overwrite=True, clean_on_failure=True):
        export_done = False
        export_retries = self._export_retries

        vdi_name = self.get_label(vdi_ref)
        export_type = "full" if base_vdi is None else "delta"

        file_name = self.get_export_file(vdi_ref, vdi_back_dir, export_type)
        full_file_name = os.path.join(base_back_dir, file_name)

        if overwrite or not os.path.exists(full_file_name):
            while not export_done:
                export_retries -= 1

                self.logger.debug("Exporting VDI '%s' to '%s'", vdi_name, full_file_name)

                task_ref = self.task.create(vdi_name + " export", "Exporting {} VDI {}".format(export_type, vdi_name))

                os.makedirs(os.path.join(base_back_dir, vdi_back_dir), 0o755, True)

                url = "{}/export_raw_vdi?session_id={}&task_id={}&format={}&vdi={}".format(
                    self.master_url, self.session_id, task_ref, self.vdi_file_format, vdi_ref)
                if base_vdi is not None:
                    url = "{}&base={}".format(url, base_vdi)

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
                            self.logger.error("Error failed deleting VDI %s", vdi_name)
                    # Retry if error is IOError but no insufficient space
                    if isinstance(e, IOError) and e.errno != errno.ENOSPC and export_retries > 0:
                        self.logger.warning("Error exporting VDI %s (%s). Retrying", vdi_name, e)
                    else:
                        self.logger.error("VDI export failed: %s", e)
                        raise e
                else:
                    export_done = True

        return file_name

    def clean(self, vdi_record, base_folder, delete):
        used_vdi_files = []
        if delete:
            try:
                os.remove(os.path.join(base_folder, vdi_record["backup_file"]))
            except IOError as e:
                self.logger.error("Error deleteting VDI file %s %s", vdi_record["backup_file"], e.strerror)
            else:
                self.logger.debug("VDI file %s deleted", vdi_record["backup_file"])
        else:
            if "backup_base_file" in vdi_record:
                used_vdi_files.append(os.path.join(base_folder, vdi_record["backup_base_file"]))
            used_vdi_files.append(os.path.join(base_folder, vdi_record["backup_file"]))
        return used_vdi_files

    def clean_unused(self, vm_back_dir, used_vdi_files):
        for dp, dn, filenames in os.walk(vm_back_dir):
            for filename in filenames:
                if filename.endswith(self.vdi_file_format) and os.path.join(dp, filename) not in used_vdi_files:
                    try:
                        os.remove(os.path.join(dp, filename))
                    except IOError as e:
                        self.logger.error("Error deleteting VDI file %s %s", os.path.join(dp, filename), e.strerror)
                    else:
                        self.logger.debug("VDI file %s deleted", os.path.join(dp, filename))

    def backup(self, vdi_ref, vbd_ref, base_folder, vm_back_dir, base_backup_ref=None, backup_vdis_map=None):
        vbd_record = None
        base_vdi = None
        base_vdi_file_name = None

        vm_vdi = self.get_snapshot_of(vdi_ref)
        vdi_back_dir = os.path.join(vm_back_dir, "vdi_" + self.get_uuid(vm_vdi))

        if backup_vdis_map is not None:
            if vm_vdi in backup_vdis_map:
                base_vdi = backup_vdis_map[vm_vdi]
                # if base vdi export is missing re-export it
                base_vdi_file_name = self.export(base_vdi, base_folder, vdi_back_dir, overwrite=False)
            else:
                vbd_record = self.vbd.get_record(vbd_ref)
                vbd_record["VM"] = base_backup_ref
                vbd_record["VDI"] = vdi_ref

        vdi_file_name = self.export(vdi_ref, base_folder, vdi_back_dir, base_vdi)

        vdi_record = self.get_record(vdi_ref)
        vdi_record["SR_label"] = self.sr.get_label(vdi_record["SR"])
        vdi_record["backup_file"] = vdi_file_name
        if base_vdi_file_name is not None:
            vdi_record["backup_base_file"] = base_vdi_file_name

        return vdi_record, vbd_record

    def import_data(self, vdi_ref, vdi_name, vdi_fn, export_type):
        task_ref = self.task.create(vdi_name + " import", "Importing {} VDI {}".format(export_type, vdi_name))

        self.logger.debug("Importing VDI data of %s", vdi_name)

        url = "{}/import_raw_vdi?session_id={}&task_id={}&format={}&vdi={}".format(
            self.master_url, self.session_id, task_ref, self.vdi_file_format, vdi_ref)

        with open(vdi_fn, 'rb') as vdi_file:
            request = urllib.request.Request(url, data=vdi_file, method="PUT")
            request.add_header("Content-Length", str(os.path.getsize(vdi_fn)))
            try:
                result = urllib.request.urlopen(request, context=ctx)
            except (HTTPError, IOError, SystemExit) as e:
                try:
                    self.task.cancel(task_ref)
                except Failure:
                    self.logger.exception("Error cancelling import task")
                raise e
            else:
                self.logger.debug("VDI data import completed")

    def restore(self, vdi_record, base_back_dir, sr_map=None):

        if not self.sr.exists(vdi_record["SR"]):
            if sr_map is not None and vdi_record["uuid"] in sr_map:
                vdi_record["SR"] = self.sr.get_by_uuid(sr_map[vdi_record["uuid"]])
            elif sr_map is not None and vdi_record["SR_label"] in sr_map:
                vdi_record["SR"] = self.sr.get_by_label(sr_map[vdi_record["SR_label"]])[0]
            else:
                vdi_record["SR"] = self.pool.get_default_sr()

        delta_restore = "backup_base_file" in vdi_record

        vdi_ref = self.create(vdi_record)
        vdi_name = self.get_label(vdi_ref)

        try:
            if delta_restore:
                vdi_file = os.path.join(base_back_dir, vdi_record["backup_base_file"])
                self.import_data(vdi_ref, vdi_name, vdi_file, "full")

            vdi_file = os.path.join(base_back_dir, vdi_record["backup_file"])
            self.import_data(vdi_ref, vdi_name, vdi_file, "delta" if delta_restore else "full")
        except (HTTPError, IOError, SystemExit) as e:
            self.logger.error("Error importing VDI %s", e.strerror)
            self.destroy(vdi_ref)
            raise e

        return vdi_ref
