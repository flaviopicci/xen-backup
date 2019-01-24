import json
import logging
import multiprocessing
import os
import time
from http.client import CannotSendRequest
from multiprocessing.pool import Pool

import yaml

from handlers.pool import Pool as XenPool
from handlers.vm import get_vms_to_backup
from lib import XenAPI

logger = logging.getLogger("Xen backup")


def do_backup(name, master, username, password, delta, backup_new_snap=True, excluded_vms=None, vm_uuid_list=None,
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
            session_id = session.handle

            pool = XenPool(xapi)
            vms, num_vms = get_vms_to_backup(
                xapi, master_url, session_id, excluded_vms=excluded_vms, vm_uuid_list=vm_uuid_list)

            logger.info("Backing up %d VMs in pool %s", num_vms, pool.get_label())

            return_status["failed_vms"] = {}
            for v, vm in enumerate(vms):
                if delta:
                    vm.backup_delta(return_status["failed_vms"], base_folder, v, num_vms)
                    vm.clean_delta_backups(base_folder, backups_to_retain)
                else:
                    vm.backup(return_status["failed_vms"], base_folder, v, num_vms, backup_new_snap)
                    vm.clean_backups(base_folder, backups_to_retain)

            if len(return_status["failed_vms"]) == 0:
                logger.info("Backup of %d VMs in pool %s completed", num_vms, pool.get_label())
            else:
                logger.error("Backup of %d VMs in pool %s completed with errors:", num_vms, pool.get_label())
                for vm_error in return_status["failed_vms"].values():
                    logger.error(vm_error)
        except (IOError, XenAPI.Failure) as e:
            return_status["error"] = str(e)
            logger.warning("Backup of pool %s aborted. Error: %s", name, str(e))
        except SystemExit:
            logger.warning("Backup of pool %s aborted  on external request", name)
        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure) as e:
                logger.error("Xen logout failed: %s", str(e))

    return return_status


def backup(args):
    error = False
    max_subproc = 2

    config_filename = args.config

    try:
        with open(config_filename, "r") as config_file:
            config = yaml.load(config_file)
    except OSError as e:
        logger.error("Error opening config file : %s", e)
        raise e

    if args.master is not None and args.username is not None and args.password is not None:
        config["pools"] = [{
            "name": "Main pool",
            "master": args.master,
            "username": args.username,
            "password": args.password
        }]

    if os.path.exists(config["mail"]["content"]):
        os.remove(config["mail"]["content"])

    base_back_dir = args.base_dir if args.base_dir is not None else config[
        args.type + "_backup_dir"] if args.type + "_backup_dir" in config else None

    backup_new_snap = args.new_snapshot if args.new_snapshot is not None else config[
        "backup_new_snap"] if "backup_new_snap" in config else None

    backups_to_retain = args.backups_to_retain if args.backups_to_retain is not None else config[
        args.type + "_backups_to_retain"] if args.type + "_backups_to_retain" in config else None

    logger.info("Backing up %d Xen pool(s)", len(config["pools"]))

    backup_procs = {}
    proc_pool = Pool(max_subproc)
    for pool_config in config["pools"]:
        pool_config["delta"] = (args.type == "delta")
        if base_back_dir is not None:
            pool_config["base_folder"] = base_back_dir
        if backup_new_snap is not None:
            pool_config["backup_new_snap"] = backup_new_snap
        if backups_to_retain is not None:
            pool_config["backups_to_retain"] = backups_to_retain
        if args.uuid is not None:
            pool_config["vm_uuid_list"] = args.uuid

        backup_procs[pool_config["name"]] = proc_pool.apply_async(do_backup, kwds=pool_config)

    time.sleep(0.1)  # Wait for all tasks have been scheduled
    proc_pool.close()
    try:
        proc_pool.join()
    except SystemExit:
        logger.warning("Backup aborted on external request")

    mail_content = {
        "subject": config["mail"]["subject"].format(args.type.title()),
        "body": {}
    }
    for pool_name, result in backup_procs.items():
        try:
            backup_status = result.get(10)
        except (SystemExit, multiprocessing.context.TimeoutError):
            logger.error("Backup process '%s' aborted", pool_name)
            error = True
        else:
            mail_pool_content = {
                "errors": [],
                "vms": []
            }
            if "error" in backup_status:
                error = True
                mail_pool_content["errors"].append(backup_status["error"])
            if "failed_vms" in backup_status and len(backup_status["failed_vms"]) > 0:
                error = True
                mail_pool_content["vms"] = list(backup_status["failed_vms"].values())

            mail_content["body"].update({
                pool_name: mail_pool_content
            })

    with open(config["mail"]["content"], "w") as mail_file:
        json.dump(mail_content, mail_file)

    if error:
        raise SystemExit("An error occurred while performing backup")
