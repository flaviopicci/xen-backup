import argparse
import logging.config
import os
import signal
import sys
from datetime import datetime
from http.client import CannotSendRequest
from urllib.error import HTTPError

from lib import XenAPI
from lib.functions import exit_gracefully, datetime_to_timestamp

if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    parser = argparse.ArgumentParser()
    parser.add_argument("--src-master", type=str, help="Master host")
    parser.add_argument("--dst-master", type=str, help="Master host")
    parser.add_argument("-U", "--username", type=str, help="Username")
    parser.add_argument("-P", "--password", type=str, help="Password")
    parser.add_argument("-b", "--base-dir", type=str, help="Base backup directory")
    parser.add_argument("-u", "--uuid", action="append", type=str, required=True)
    parser.add_argument("-s", "--shutdown", action='store_true', help="Shutdown vm before exporting")
    parser.add_argument("-r", "--restore", action='store_true', help="Perform full restore")
    # parser.add_argument("-n", "--vm-name", type=str)

    args = parser.parse_args()
    username = args.username
    password = args.password
    backup_dir = args.base_dir

    logging.config.fileConfig("log.conf")
    logger = logging.getLogger("Xen backup")

    if args.uuid is None and args.vm_name is None:
        logger.error("VM UUID or name required!")
        sys.exit(1)

    src_master_url = "https://" + args.src_master
    dst_master_url = "https://" + args.dst_master

    src_session = XenAPI.Session(src_master_url, ignore_ssl=True)
    dst_session = XenAPI.Session(dst_master_url, ignore_ssl=True)

    try:
        src_session.xenapi.login_with_password(username, password)
        dst_session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
    else:
        src_xapi = src_session.xenapi
        dst_xapi = dst_session.xenapi

        src_vms = VMHandler(src_xapi, src_master_url, src_session.handle)
        dst_vms = VMHandler(dst_xapi, dst_master_url, dst_session.handle)

        logger.info("Transferring %d VMs", len(args.uuid))

        for vm_uuid in args.uuid:
            old_vm_ref = None
            new_vm_ref = None
            try:
                old_vm_ref = src_vms.get_by_uuid(vm_uuid)
                if old_vm_ref is not None:
                    vm_name = src_vms.get_label(old_vm_ref)
                    logger.info("Transferring VM %s", vm_name)
                    vm_uuid = src_vms.get_uuid(old_vm_ref)
                    power_state = src_vms.get_power_state(old_vm_ref)

                    if args.shutdown and not src_vms.is_halted(old_vm_ref):
                        try:
                            src_vms.shutdown(old_vm_ref)
                        except XenAPI.Failure as e:
                            logger.warning("Error shutting down VM")

                    take_snapshot = not src_vms.can_export(old_vm_ref)

                    export_datetime = datetime.utcnow()
                    # export_vm_name = "{} - backup {}".format(vm_name, datetime_to_timestamp(
                    #     export_datetime, to_format="%Y-%m-%d %H:%M:%S"))
                    export_name = "{}__{}__{}".format(vm_uuid, datetime_to_timestamp(export_datetime), vm_name)

                    if take_snapshot:
                        export_vm_ref = src_vms.snapshot(old_vm_ref, export_name)
                        src_vms.set_is_template(export_vm_ref, False)
                    else:
                        src_vms.set_name(old_vm_ref, export_name)
                        export_vm_ref = old_vm_ref

                    try:
                        exported_file_name = src_vms.export(export_vm_ref, backup_dir, export_name, vm_name)
                    finally:
                        if take_snapshot:
                            src_vms.destroy(export_vm_ref)
                        else:
                            src_vms.set_name(export_vm_ref, vm_name)

                    dst_vms.restore(exported_file_name, restore=args.restore)

                    new_vm_ref = dst_vms.get_by_label(export_name)[0]
                    dst_vms.set_name(new_vm_ref, vm_name)
                    dst_vms.set_power_state(new_vm_ref, power_state)

                    os.remove(exported_file_name)
            except SystemExit:
                logger.info("VM transfer aborted on user request")
                if old_vm_ref is not None and (
                                new_vm_ref is None or dst_vms.get_power_state(new_vm_ref) != power_state):
                    logger.info("Restoring old VM power state")
                    src_vms.set_power_state(old_vm_ref, power_state)

                break
            except (ValueError, FileNotFoundError, HTTPError, IOError) as e:
                logger.error("VM transfer failed %s", e)
                if old_vm_ref is not None and (
                                new_vm_ref is None or dst_vms.get_power_state(new_vm_ref) != power_state):
                    logger.info("Restoring old VM power state")
                    src_vms.set_power_state(old_vm_ref, power_state)
            else:
                logger.info("VM transfer completed")

        try:
            src_session.xenapi.session.logout()
            dst_session.xenapi.session.logout()
        except (CannotSendRequest, XenAPI.Failure) as e:
            logger.error("Xen logout failed: %s", e)
