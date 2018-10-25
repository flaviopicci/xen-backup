import logging
from http.client import CannotSendRequest

import lib.XenAPI as XenAPI
from handlers.common import get_by_uuid
from handlers.vm import VM


def delete(args):
    vm_uuids = args.uuid

    logger = logging.getLogger("Xen delete")

    master_url = "https://" + args.master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(args.username, args.password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
        raise e
    else:
        try:
            xapi = session.xenapi

            for vm_uuid in vm_uuids:
                try:
                    vm_ref = get_by_uuid(xapi.VM, vm_uuid)
                except XenAPI.Failure:
                    raise ValueError("Cannot find VM with uuid {}".format(vm_uuid))
                else:
                    VM(xapi, None, None, vm_ref).destroy()

        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure):
                logger.exception("Xen logout failed")
