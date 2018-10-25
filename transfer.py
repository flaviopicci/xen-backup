import logging
import os
import sys
from http.client import CannotSendRequest
from urllib.error import HTTPError

from handlers.common import get_by_uuid, get_by_label
from handlers.vm import VM, restore as restore_vm
from lib import XenAPI
from lib.XenAPI import Failure
from lib.functions import get_timestamp

logger = logging.getLogger("Xen transfer")


def transfer(args):
    username = args.username
    password = args.password
    backup_dir = args.base_dir

    if args.uuid is None and args.vm_name is None:
        raise ValueError("VM UUID or name required!")

    src_master_url = "https://" + args.src_master
    dst_master_url = "https://" + args.dst_master

    src_session = XenAPI.Session(src_master_url, ignore_ssl=True)
    dst_session = XenAPI.Session(dst_master_url, ignore_ssl=True)

    try:
        src_session.xenapi.login_with_password(username, password)
        dst_session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
        raise e
    else:
        src_xapi = src_session.xenapi
        dst_xapi = dst_session.xenapi

        src_s_id = src_session.handle
        dst_s_id = dst_session.handle

        logger.info("Transferring %d VMs", len(args.uuid))

        for vm_uuid in args.uuid:
            src_vm = None
            dst_vm = None
            power_state = "halt"
            try:
                try:
                    src_vm_ref = get_by_uuid(src_xapi.VM, vm_uuid)
                except Failure:
                    pass
                else:
                    src_vm = VM(src_xapi, src_master_url, src_s_id, src_vm_ref)
                    vm_name = src_vm.get_label()
                    logger.info("Transferring VM %s", vm_name)
                    vm_uuid = src_vm.get_uuid()
                    power_state = src_vm.get_power_state()

                    if args.shutdown and not src_vm.is_halted():
                        try:
                            src_vm.shutdown()
                        except XenAPI.Failure as e:
                            logger.warning("Error shutting down VM")

                    take_snapshot = not src_vm.can_export()

                    export_datetime = get_timestamp(to_str=True)
                    # export_vm_name = "{} - backup {}".format(vm_name, datetime_to_timestamp(
                    #     export_datetime, to_format="%Y-%m-%d %H:%M:%S"))
                    export_name = "{}__{}__{}".format(vm_uuid, export_datetime, vm_name)

                    if take_snapshot:
                        src_vm = src_vm.snapshot(export_name)
                        src_vm.set_is_template(False)
                    else:
                        src_vm.set_name(export_name)

                    try:
                        exported_file_name = src_vm.export(backup_dir, export_name, vm_name)
                    finally:
                        if take_snapshot:
                            src_vm.destroy()
                        else:
                            src_vm.set_name(vm_name)

                    restore_vm(dst_xapi, dst_master_url, dst_s_id, exported_file_name, restore=args.restore)

                    dst_vm = VM(dst_xapi, dst_master_url, dst_s_id, get_by_label(dst_xapi.VM, export_name)[0])
                    dst_vm.set_name(vm_name)
                    dst_vm.set_power_state(power_state)

                    os.remove(exported_file_name)
            except SystemExit:
                logger.info("VM transfer aborted on user request")
                if src_vm is not None and (dst_vm is None or dst_vm.get_power_state() != power_state):
                    logger.info("Restoring old VM power state")
                    src_vm.set_power_state(power_state)
            except (ValueError, FileNotFoundError, HTTPError, IOError) as e:
                logger.error("VM transfer failed %s", e)
                if src_vm is not None and (dst_vm is None or dst_vm.get_power_state() != power_state):
                    logger.info("Restoring old VM power state")
                    src_vm.set_power_state(power_state)
            else:
                logger.info("VM transfer completed")

        try:
            src_session.xenapi.session.logout()
            dst_session.xenapi.session.logout()
        except (CannotSendRequest, XenAPI.Failure) as e:
            logger.error("Xen logout failed: %s", e)
