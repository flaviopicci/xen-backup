import logging
import sys
from datetime import datetime, timezone
from http.client import CannotSendRequest
from urllib.error import HTTPError

from handlers.common import get_by_uuid, get_by_label
from handlers.vm import VM
from lib import XenAPI
from lib.functions import datetime_to_timestamp

logger = logging.getLogger("Xen export")


def export(args):
    username = args.username
    password = args.password
    backup_dir = args.base_dir

    if args.uuid is None and args.vm_name is None:
        raise ValueError("VM UUID or name required!")

    master_url = "https://" + args.master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
        raise e
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
