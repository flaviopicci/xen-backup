import argparse
import signal

from backup import backup
from clean import clean
from restore import restore
from transfer import transfer


def exit_gracefully(signum, _):
    raise SystemExit(signum)


actions = {
    "backup": backup,
    "restore": restore,
    "transfer": transfer,
    "clean": clean
}

if __name__ == "__main__":
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    parser = argparse.ArgumentParser()
    parser.add_argument("action", type=str, help="Action to perform")
    parser.add_argument("-c", "--config", type=str, help="Set configuration file", default="config.yml")
    parser.add_argument("-M", "--master", type=str, help="Master host")
    parser.add_argument("--src-master", type=str, help="Source master host (for migration)")
    parser.add_argument("--dst-master", type=str, help="Destination master host (for migration)")
    parser.add_argument("-U", "--username", type=str, help="Username")
    parser.add_argument("-P", "--password", type=str, help="Password")
    parser.add_argument("-d", "--base-dir", type=str, help="Backups directory")
    parser.add_argument("-f", "--file", type=str, help="Backup XVA file or VM definition")
    parser.add_argument("-t", "--type", type=str, help="Type of backup to perform", default="delta")
    parser.add_argument("-n", "--new-snapshot", type=bool, help="Always perform new snapshot to backup")
    parser.add_argument("-u", "--uuid", type=str, action="append", help="UUIDs of the VMs to backup")
    parser.add_argument("-b", "--backups-to-retain", type=int, help="Number of backups to retain")
    parser.add_argument("-r", "--restore", action='store_true', help="Perform full restore")
    parser.add_argument("-s", "--shutdown", action='store_true', help="Shutdown vm before exporting")

    parser.add_argument("--network-map", type=str, action="append")
    parser.add_argument("--storage-map", type=str, action="append")

    args = parser.parse_args()

    actions[args.action](args)
