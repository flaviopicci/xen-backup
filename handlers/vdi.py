import errno
import logging
import os
import re
import shutil
import ssl
import time
from urllib import request
from urllib.error import HTTPError

from handlers import common
from handlers.common import CommonEntities, get_by_uuid, get_by_label
from handlers.pool import Pool
from handlers.sr import SR
from handlers.task import Task
from handlers.vbd import VBD
from lib.XenAPI import Failure

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


class VDI(CommonEntities):
    vdi_file_format = "vhd"
    full_extension = "full." + vdi_file_format
    _export_retries = 3
    _destroy_retries = 3

    _type = "VDI"

    def __init__(self, xapi, master_url, session_id, ref=None, params=None):
        super().__init__(xapi, master_url, session_id, ref, params)

    def get_type(self):
        return self.xapi.get_type(self.ref)

    def get_vbds(self):
        return (VBD(self._xapi, vbd_ref) for vbd_ref in self.xapi.get_VBDs(self.ref))

    def get_export_file(self, vdi_back_dir, export_type="full"):
        return os.path.join(
            vdi_back_dir,
            "{}_{}.{}".format(self.get_snapshot_time(ts_format="%Y%m%dT%H%M00"), export_type, self.vdi_file_format))

    def import_data(self, vdi_name, vdi_fn, export_type):

        task = Task(self._xapi, params=[vdi_name + " import", "Importing {} VDI {}".format(export_type, vdi_name)])

        self.logger.debug("Importing VDI data of %s", vdi_name)

        url = "{}/import_raw_vdi?session_id={}&task_id={}&format={}&vdi={}".format(
            self.master_url, self.session_id, task.ref, self.vdi_file_format, self.ref)

        with open(vdi_fn, 'rb') as vdi_file:
            req = request.Request(url, data=vdi_file, method="PUT")
            req.add_header("Content-Length", str(os.path.getsize(vdi_fn)))
            try:
                request.urlopen(req, context=ctx)
            except (HTTPError, IOError, SystemExit) as e:
                try:
                    task.cancel()
                except Failure:
                    self.logger.exception("Error cancelling import task")
                raise e
            else:
                self.logger.debug("VDI data import completed")

    def export(self, base_back_dir, vdi_back_dir, base_vdi=None, overwrite=True, clean_on_failure=True):
        export_done = False
        export_retries = self._export_retries

        vdi_name = self.get_label()
        export_type = "full" if base_vdi is None else "delta"

        file_name = self.get_export_file(vdi_back_dir, export_type)
        full_file_name = os.path.join(base_back_dir, file_name)

        if overwrite or not os.path.exists(full_file_name):
            while not export_done:
                export_retries -= 1

                self.logger.debug("Exporting VDI '%s' to '%s'", vdi_name, full_file_name)

                task = Task(self._xapi, params=[
                    vdi_name + " export", "Exporting {} VDI {}".format(export_type, vdi_name)
                ])

                os.makedirs(os.path.join(base_back_dir, vdi_back_dir), 0o755, True)

                url = "{}/export_raw_vdi?session_id={}&task_id={}&format={}&vdi={}".format(
                    self.master_url, self.session_id, task.ref, self.vdi_file_format, self.ref)
                if base_vdi is not None:
                    url = "{}&base={}".format(url, base_vdi.ref)

                try:
                    with request.urlopen(url, context=ctx) as response, open(full_file_name, 'wb') as out_file:
                        shutil.copyfileobj(response, out_file)
                except (HTTPError, IOError, SystemExit) as e:
                    try:
                        task.cancel()
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

    def backup(self, base_folder, vm_back_dir, backup_vdis_map=None):
        base_vdi = None
        base_vdi_file_name = None

        vm_vdi = self.get_snapshot_of()
        vdi_back_dir = os.path.join(vm_back_dir, "vdi_" + vm_vdi.get_uuid())

        # We are performing a delta backup
        if backup_vdis_map is not None and vm_vdi.ref in backup_vdis_map:
            base_vdi = backup_vdis_map[vm_vdi.ref]
            # if base vdi export is missing re-export it
            base_vdi_file_name = base_vdi.export(base_folder, vdi_back_dir, overwrite=False)

        vdi_file_name = self.export(base_folder, vdi_back_dir, base_vdi)

        vdi_record = self.get_record()
        vdi_record["SR_label"] = SR(self._xapi, vdi_record["SR"]).get_label()
        vdi_record["backup_file"] = vdi_file_name
        if base_vdi_file_name is not None:
            vdi_record["backup_base_file"] = base_vdi_file_name

        return vdi_record

    def destroy(self):
        done = False
        retries = self._destroy_retries
        while not done and retries > 0:
            try:
                super().destroy()
            except Failure:
                retries = retries - 1
                self.logger.warning(
                    "Error destroying VDI%s", ". Retrying in 5 seconds" if retries > 0 else "!")
                time.sleep(5)
            else:
                done = True


def restore(xapi, master_url, session_id, vdi_record, base_back_dir, sr_map=None):
    logger = logging.getLogger("VDI")

    sr = SR(xapi, vdi_record["SR"])
    if not sr.exists():
        if sr_map is not None and vdi_record["uuid"] in sr_map:
            sr_ref = get_by_uuid(sr.xapi, sr_map[vdi_record["uuid"]])
        elif sr_map is not None and vdi_record["SR_label"] in sr_map:
            sr_ref = get_by_label(sr.xapi, sr_map[vdi_record["SR_label"]])[0]
        else:
            sr_ref = Pool(xapi).get_default_sr().ref
        vdi_record["SR"] = sr_ref

    delta_restore = "backup_base_file" in vdi_record

    vdi = VDI(xapi, master_url, session_id, params=vdi_record)
    vdi_name = vdi.get_label()

    try:
        if delta_restore:
            vdi_file = os.path.join(base_back_dir, vdi_record["backup_base_file"])
            vdi.import_data(vdi_name, vdi_file, "full")

        vdi_file = os.path.join(base_back_dir, vdi_record["backup_file"])
        vdi.import_data(vdi_name, vdi_file, "delta" if delta_restore else "full")
    except (HTTPError, IOError, SystemExit) as e:
        logger.error("Error importing VDI %s", e.strerror)
        vdi.destroy()
        raise e

    return vdi.ref


def clean(vdi_record, base_folder, delete):
    logger = logging.getLogger("VDI")
    used_vdi_files = []
    if delete:
        try:
            os.remove(os.path.join(base_folder, vdi_record["backup_file"]))
        except IOError as e:
            logger.error("Error deleting VDI file %s %s", vdi_record["backup_file"], e.strerror)
        else:
            logger.debug("VDI file %s deleted", vdi_record["backup_file"])
    else:
        if "backup_base_file" in vdi_record:
            used_vdi_files.append(os.path.join(base_folder, vdi_record["backup_base_file"]))
        used_vdi_files.append(os.path.join(base_folder, vdi_record["backup_file"]))
    return used_vdi_files


def clean_unused(vm_back_dir, used_vdi_files):
    logger = logging.getLogger("VDI")

    for dp, dn, filenames in os.walk(vm_back_dir):
        for filename in filenames:
            if filename.endswith(VDI.vdi_file_format) and os.path.join(dp, filename) not in used_vdi_files:
                try:
                    os.remove(os.path.join(dp, filename))
                except IOError as e:
                    logger.error("Error deleting VDI file %s %s", os.path.join(dp, filename), e.strerror)
                else:
                    logger.debug("VDI file %s deleted", os.path.join(dp, filename))


def get_orphan_vdis(xapi):
    regex = re.compile("^(base copy|.*\.(ISO|iso|img))$")
    return (
        vdi for vdi in (
        VDI(xapi, None, None, vdi_ref) for vdi_ref in common.get_all_refs(xapi.VDI)
    )
        if vdi.get_type() == 'user'
           and len(vdi.get_vbds()) == 0
           and "destroy" in vdi.get_allowed_operations()
           and not regex.match(vdi.get_label())
    )
