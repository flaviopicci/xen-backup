import argparse
import cmd
import logging.config
import signal
from http.client import CannotSendRequest

from handlers import vm
from lib import XenAPI
from handlers.pool import Pool as XenPool

# def restore(name, master, username, password, vm_file, auto_start=False, restore=False,
#             sr=None, sr_map=None, network_map=None, delta=False, base_folder="."):
#     logger = logging.getLogger(name)
#     master_url = "https://" + master
#
#     session = XenAPI.Session(master_url, ignore_ssl=True)
#     try:
#         session.xenapi.login_with_password(username, password)
#     except (CannotSendRequest, XenAPI.Failure) as e:
#         logger.exception("Error logging in Xen host")
#     else:
#         try:
#             xapi = session.xenapi
#             vms = VMHandler(xapi, master_url, session.handle)
#
#             if delta:
#                 vms.restore_delta(vm_file, base_folder, sr_map, network_map, auto_start, restore)
#             else:
#                 vms.restore(vm_file, sr, auto_start, restore)
#         finally:
#             try:
#                 session.xenapi.session.logout()
#             except (CannotSendRequest, XenAPI.Failure):
#                 logger.exception("Xen logout failed")
#
#
# class HelloWorld(cmd.Cmd):
#     """Simple command processor example."""
#
#     prompt = "> "
#
#     FRIENDS = ['Alice', 'Adam', 'Barbara', 'Bob']
#
#     def do_greet(self, person):
#         """Greet the person"""
#         if person and person in self.FRIENDS:
#             greeting = 'hi, %s!' % person
#         elif person:
#             greeting = "hello, " + person
#         else:
#             greeting = 'hello'
#         print(greeting)
#
#     def complete_greet(self, text, line, begidx, endidx):
#         if not text:
#             completions = self.FRIENDS[:]
#         else:
#             completions = [f
#                            for f in self.FRIENDS
#                            if f.startswith(text)
#                            ]
#         return completions
#
#     def do_EOF(self, line):
#         return True
#
from lib.functions import exit_gracefully, vm_definition_from_file

if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    parser = argparse.ArgumentParser()
    parser.add_argument("-M", "--master", type=str, help="Master host")
    parser.add_argument("-U", "--username", type=str, help="Username")
    parser.add_argument("-P", "--password", type=str, help="Password")
    parser.add_argument("-f", "--file", type=str, help="Backup XVA file or VM definition")
    parser.add_argument("-b", "--base-dir", type=str, help="Base backup directory")
    parser.add_argument("-t", "--type", type=str, help="Type of backup to perform", default="delta")
    parser.add_argument("-r", "--restore", action='store_true', help="Type of backup to perform")
    parser.add_argument("-n", "--network-map", type=str, action="append")
    parser.add_argument("-s", "--storage-map", type=str, action="append")

    args = parser.parse_args()
    username = args.username
    password = args.password

    backup_dir = args.base_dir
    backup_file = args.file

    network_map = args.network_map
    if network_map is not None:
        network_map = dict(mapping.split("=") for mapping in network_map)
    storage_map = args.storage_map
    if storage_map is not None:
        storage_map = dict(mapping.split("=") for mapping in storage_map)

    logging.config.fileConfig("log.conf")
    logger = logging.getLogger("Xen backup")

    master_url = "https://" + args.master

    session = XenAPI.Session(master_url, ignore_ssl=True)
    try:
        session.xenapi.login_with_password(username, password)
    except (CannotSendRequest, XenAPI.Failure) as e:
        logger.exception("Error logging in Xen host")
    else:
        try:
            if args.type == "delta":
                vm.restore_delta(
                    session.xenapi, master_url, session.handle,
                    backup_file, backup_dir, sr_map=storage_map, network_map=network_map, restore=args.restore)
            else:
                vm.restore(
                    session.xenapi, master_url, session.handle,
                    backup_file, sr_map=storage_map, restore=args.restore)
        finally:
            try:
                session.xenapi.session.logout()
            except (CannotSendRequest, XenAPI.Failure) as e:
                logger.error("Xen logout failed: %s", e.details)
