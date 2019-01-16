import logging
import time
from http.client import CannotSendRequest
from multiprocessing.pool import Pool

import yaml

from handlers.vm import get_vms_to_backup
from lib import XenAPI

logger = logging.getLogger("Xen backup")

max_subproc = 2


def clean_all(name, master, username, password, excluded_vms=None):
    master_url = "https://" + master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.error("Error logging in Xen host")
        raise e
    else:
        try:
            logger.info("Cleaning backup snapshots in pool %s", name)
            xapi = session.xenapi
            session_id = session.handle

            for vm in get_vms_to_backup(xapi, master_url, session_id, excluded_vms):
                for snap in vm.get_backup_snapshots():
                    snap.destroy()
        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure):
                logger.error("Xen logout failed")


def clean(args):
    if args.type == "full":
        config_filename = args.config

        try:
            with open(config_filename, "r") as config_file:
                config = yaml.load(config_file)
        except OSError as e:
            logger.error("Error opening config file : %s", e)
            raise e

        logger.info("Cleaning %d Xen pool(s)", len(config["pools"]))

        backup_procs = []
        proc_pool = Pool(processes=max_subproc)
        for pool_config in config["pools"]:
            backup_procs.append(
                {"name": pool_config["name"], "result": proc_pool.apply_async(clean_all, kwds=pool_config)})

        time.sleep(0.1)  # Wait for all tasks have been scheduled
        proc_pool.close()
        try:
            proc_pool.join()
        except SystemExit:
            logger.warning("Terminating backup")
