import argparse
import json
import logging.config
import multiprocessing
import os
import signal
import sys
from http.client import CannotSendRequest
from multiprocessing.pool import Pool

import yaml

from handlers.vm import VMHandler
from lib import XenAPI
from lib.functions import get_vms_to_backup, exit_gracefully

max_subproc = 2
exit_code = 0


def backup(name, master, username, password, delta, backup_new_snap=True, excluded_vms=None, vm_uuid_list=None,
           base_folder=".", backups_to_retain=1):
    return_status = {}
    master_url = "https://" + master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
        return_status["error"] = e.details
    else:
        try:
            xapi = session.xenapi
            vms = VMHandler(xapi, master_url, session.handle)

            vm_refs = get_vms_to_backup(vms, excluded_vms=excluded_vms, vm_uuid_list=vm_uuid_list)
            num_vms = len(vm_refs)

            logger.info("Backing up %d VMs in pool %s", num_vms, name)

            return_status["failed_vms"] = {}
            for v, vm_ref in enumerate(vm_refs):
                if delta:
                    vms.backup_delta(vm_ref, return_status["failed_vms"], base_folder, v, num_vms)
                    vms.clean_delta_backups(vm_ref, base_folder, backups_to_retain)
                else:
                    vms.backup(vm_ref, return_status["failed_vms"], base_folder, v, num_vms, backup_new_snap)
                    vms.clean_backups(vm_ref, base_folder, backups_to_retain)

            if len(return_status["failed_vms"]) == 0:
                logger.info("Backup of %d VMs in pool %s completed", num_vms, name)
            else:
                logger.error("Backup of %d VMs in pool %s completed with errors:", num_vms, name)
                for vm_error in return_status["failed_vms"].values():
                    logger.error(vm_error)
        except IOError as e:
            return_status["error"] = e.strerror
            logger.warning("Backup of pool %s aborted. Error: %s", name, e.strerror)
        except XenAPI.Failure as e:
            return_status["error"] = e.details
            logger.warning("Backup of pool %s aborted. Error: %s", name, e.details)
        except SystemExit:
            logger.warning("Backup of pool %s aborted  on external request", name)
        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure) as e:
                logger.error("Xen logout failed: %s", e.details)

    return return_status


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="Set configuration file", default="config.yml")
    parser.add_argument("-M", "--master", type=str, help="Master host")
    parser.add_argument("-U", "--username", type=str, help="Username")
    parser.add_argument("-P", "--password", type=str, help="Password")
    parser.add_argument("-f", "--folder", type=str, help="Backup destination folder")
    parser.add_argument("-t", "--type", type=str, help="Type of backup to perform", default="delta")
    parser.add_argument("-n", "--new-snapshot", type=bool, help="Always perform new snapshot to backup")
    parser.add_argument("-u", "--vm-uuid", type=str, action="append", help="UUIDs of the VMs to backup")
    parser.add_argument("-b", "--backups-to-retain", type=int, help="Number of backups to retain")

    args = parser.parse_args()
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

    if args.master is not None and args.username is not None and args.password is not None:
        config["pools"] = [{
            "name": "Main pool",
            "master": args.master,
            "username": args.username,
            "password": args.password
        }]

    if os.path.exists(config["mail"]["content"]):
        os.remove(config["mail"]["content"])

    base_back_dir = args.folder if args.folder is not None else config[
        args.type + "_backup_dir"] if args.type + "_backup_dir" in config else None

    backup_new_snap = args.new_snapshot if args.new_snapshot is not None else config[
        "backup_new_snap"] if "backup_new_snap" in config else None

    backups_to_retain = args.backups_to_retain if args.backups_to_retain is not None else config[
        args.type + "_backups_to_retain"] if args.type + "_backups_to_retain" in config else None

    logger.info("Backing up %d Xen pool(s)", len(config["pools"]))

    backup_procs = []
    proc_pool = Pool(processes=max_subproc)
    for pool_config in config["pools"]:
        pool_config["delta"] = (args.type == "delta")
        if base_back_dir is not None:
            pool_config["base_folder"] = base_back_dir
        if backup_new_snap is not None:
            pool_config["backup_new_snap"] = backup_new_snap
        if backups_to_retain is not None:
            pool_config["backups_to_retain"] = backups_to_retain
        if args.vm_uuid is not None:
            pool_config["vm_uuid_list"] = args.vm_uuid

        backup_procs.append({"name": pool_config["name"], "result": proc_pool.apply_async(backup, kwds=pool_config)})

    proc_pool.close()
    try:
        proc_pool.join()
    except SystemExit:
        logger.warning("Backup aborted on external request")

    mail_content = {
        "subject": config["mail"]["subject"].format(args.type.title()),
        "body": {}
    }
    for backup_proc in backup_procs:
        try:
            backup_status = backup_proc["result"].get(10)
        except (SystemExit, multiprocessing.context.TimeoutError):
            logger.error("Backup process '%s' aborted", backup_proc["name"])
        else:
            mail_pool_content = {
                "errors": [],
                "vms": []
            }
            if "error" in backup_status:
                exit_code = 1
                mail_pool_content["errors"].append(backup_status["error"])
            if "failed_vms" in backup_status and len(backup_status["failed_vms"]) > 0:
                exit_code = 1
                mail_pool_content["vms"] = list(backup_status["failed_vms"].values())

            mail_content["body"].update({
                backup_proc["name"]: mail_pool_content
            })

    with open(config["mail"]["content"], "w") as mail_file:
        json.dump(mail_content, mail_file)

    sys.exit(exit_code)
