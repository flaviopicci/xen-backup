import logging.config
from http.client import CannotSendRequest

import lib.XenAPI as XenAPI
import argparse

from handlers.common import get_by_uuid
from handlers.vm import VM

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="Set configuration file", default="config.yml")
    parser.add_argument("-M", "--master", type=str, help="Master host")
    parser.add_argument("-U", "--username", type=str, help="Username")
    parser.add_argument("-P", "--password", type=str, help="Password")
    parser.add_argument("-u", "--uuid", type=str, action="append")

    args = parser.parse_args()

    vm_uuids = args.uuid

    logging.config.fileConfig("log.conf")
    logger = logging.getLogger("Xen backup")

    master_url = "https://" + args.master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(args.username, args.password)
    except (CannotSendRequest, XenAPI.Failure):
        logger.exception("Error logging in Xen host")
    else:
        try:
            xapi = session.xenapi
            session_id = session.handle

            for vm_uuid in vm_uuids:
                try:
                    vm_ref = get_by_uuid(xapi.VM,vm_uuid)
                except XenAPI.Failure:
                    logger.error("Cannot find VM with uuid %s", vm_uuid)
                else:
                    VM(xapi, None, None, vm_ref).destroy()

        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure):
                logger.exception("Xen logout failed")
