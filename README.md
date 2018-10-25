# xen-backup
Python scripts to perform full or differential (delta) backups of a Xen infrastructure.

### TODO:
* Proper backup restore *CLI*
* Documentation

### Usage:
./xen-br action [options]

where **action** can be:
* backup
* restore
* clean
* transfer

and **options** are:
* -h: show help
* -c (--config): config file path (see config.example.yml)
* -M (--master): Xen pool master IP/hostname
* --src-master and --dst-master: source and destination masters IPs for VM transfer
* -U (--username): Xen username
* -P (--password): XEN password
* -d (--base-dir): backups directory
* -t (--type): backup type, delta or full
* -n (--new-snapshot): always perform a new snapshot to backup/export
* -u (--uuid): UUID of the VM(s) to be backupped/exported (it can be specified multiple times)
* -b (--backups-to-retain): number of backups to retain
* -s (--shutdown): shutdown VM before exporting
* -f (--file): file containing the backup to restore
* -r (--restore): perform a full restore (see restore flag in XenAPI)
* --network-map: network mapping for restore old_ntw=new_ntw (labels or UUIDs)
* --storage-map: same as network map but for storage repository mapping