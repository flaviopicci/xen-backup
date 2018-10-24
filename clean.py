import argparse
import logging.config
import signal
import sys
from http.client import CannotSendRequest
from multiprocessing.pool import Pool

import yaml

from handlers.vm import get_vms_to_backup
from lib import XenAPI
from lib.functions import exit_gracefully

max_subproc = 2
exit_code = 0


def clean_all(name, master, username, password, excluded_vms=None):
    master_url = "https://" + master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure):
        logger.exception("Error logging in Xen host")
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
                logger.exception("Xen logout failed")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="Set configuration file", default="config.yml")
    parser.add_argument("-t", "--type", type=str, help="Type of backup to perform", default="delta")

    args = parser.parse_args()
    if args.type == "full":
        config_filename = args.config

        logging.config.fileConfig("log.conf")
        logger = logging.getLogger("Xen backup")

        signal.signal(signal.SIGINT, exit_gracefully)
        signal.signal(signal.SIGTERM, exit_gracefully)

        try:
            with open(config_filename, "r") as config_file:
                config = yaml.load(config_file)
        except Exception as ge:
            logger.error("Error opening config file : %s", ge)
            sys.exit(1)

        logger.info("Cleaning %d Xen pool(s)", len(config["pools"]))

        backup_procs = []
        proc_pool = Pool(processes=max_subproc)
        for pool_config in config["pools"]:
            backup_procs.append(
                {"name": pool_config["name"], "result": proc_pool.apply_async(clean_all, kwds=pool_config)})

        proc_pool.close()
        try:
            proc_pool.join()
        except SystemExit:
            logger.warning("Terminating backup")

    sys.exit(exit_code)
