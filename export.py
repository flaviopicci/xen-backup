import argparse
import cmd
import logging.config
import signal
from datetime import datetime, timezone
from http.client import CannotSendRequest

import sys
from urllib.error import HTTPError

from handlers.common import get_by_uuid, get_by_label
from handlers.vm import VM
from lib import XenAPI
from lib.functions import exit_gracefully, datetime_to_timestamp

if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    parser = argparse.ArgumentParser()
    parser.add_argument("-M", "--master", type=str, help="Master host")
    parser.add_argument("-U", "--username", type=str, help="Username")
    parser.add_argument("-P", "--password", type=str, help="Password")
    parser.add_argument("-b", "--base-dir", type=str, help="Base backup directory")
    parser.add_argument("-u", "--uuid", type=str)
    parser.add_argument("-n", "--vm-name", type=str)

    args = parser.parse_args()
    username = args.username
    password = args.password
    backup_dir = args.base_dir

    logging.config.fileConfig("log.conf")
    logger = logging.getLogger("Xen backup")

    if args.uuid is None and args.vm_name is None:
        logger.error("VM UUID or name required!")
        sys.exit(1)

    master_url = "https://" + args.master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
    else:
        try:
            xapi = session.xenapi
            session_id = session.handle

            vm_ref = None
            if args.uuid is not None:
                vm_ref = get_by_uuid(xapi.VM, args.uuid)
            elif args.vm_name is not None:
                vm_ref = get_by_label(xapi.VM, args.vm_name)
                if len(vm_ref) == 0:
                    logger.error("VM with name '%s' not found!", args.vm_name)
                    sys.exit(1)
                else:
                    vm_ref = vm_ref[0]

            vm = VM(xapi, master_url, session_id, vm_ref)

            vm_name = vm.get_label()
            vm_uuid = vm.get_uuid()
            take_snapshot = vm.is_running()

            export_datetime = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone()

            export_vm_name = "{} - backup {}".format(
                vm_name, datetime_to_timestamp(export_datetime, to_format="%Y-%m-%d %H:%M"))
            export_file_name = "{}__{}".format(vm_name, datetime_to_timestamp(export_datetime))

            if take_snapshot:
                vm = vm.snapshot(export_vm_name)
                vm.set_is_template(False)
            else:
                vm.set_name(export_vm_name)

            try:
                exported_file_name = vm.export(backup_dir, export_file_name, vm_name)
            except (HTTPError, IOError, SystemExit) as e:
                logger.error("Export of VM %s failed: %s", vm_name, e)
            else:
                print("exported file :", exported_file_name)
            finally:
                if take_snapshot:
                    vm.destroy()
                else:
                    vm.set_name(vm_name)
        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure) as e:
                logger.error("Xen logout failed: %s", e.details)
