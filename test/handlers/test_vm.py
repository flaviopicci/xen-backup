import os
from unittest import TestCase

import yaml

from handlers.common import get_by_uuid
from handlers.vm import get_vms_to_backup, get_all_vm_refs, VM
from lib import XenAPI


class TestGetVmsToBackup(TestCase):
    def setUp(self):
        test_folder = os.path.normpath(os.path.join(os.path.realpath(__file__), "../.."))
        config_filename = os.path.join(test_folder, "config.yml")

        with open(config_filename, "r") as config_file:
            config = yaml.load(config_file)

        assert len(config["pools"]) == 1

        master_url = "https://" + config["pools"][0]["master"]
        username = config["pools"][0]["username"]
        password = config["pools"][0]["password"]

        session = XenAPI.Session(master_url, ignore_ssl=True)
        self.xapi = session.xenapi
        self.master_url = master_url
        self.session_id = session.handle

        self.xapi.login_with_password(username, password)

        self.test_vm_uuid = config["pools"][0]["test_vm_uuid"]

    def tearDown(self):
        self.xapi.session.logout()

    def test_get_vms_to_backup_all(self):
        vms, _ = get_vms_to_backup(self.xapi, self.master_url, self.session_id)
        all_vm_refs = get_all_vm_refs(self.xapi)
        for vm in vms:
            self.assertIn(vm.ref, all_vm_refs)

    def test_get_vms_to_backup_uuid(self):
        vm_ref = get_by_uuid(self.xapi.VM, self.test_vm_uuid)
        vms, n_vms = get_vms_to_backup(self.xapi, self.master_url, self.session_id, vm_uuid_list=[self.test_vm_uuid])
        self.assertEqual(n_vms, 1, "get_vms_to_backup fetched {} VMS instead of 1 VM".format(n_vms))
        self.assertEqual(vm_ref, next(vms).ref, "Fetched wrong VM to backup")

    def test_get_vms_to_backup_label(self):
        vm_ref = get_by_uuid(self.xapi.VM, self.test_vm_uuid)
        vm_name = VM(self.xapi, self.master_url, self.session_id, vm_ref).get_label()
        vms, n_vms = get_vms_to_backup(self.xapi, self.master_url, self.session_id, vm_name_list=[vm_name])
        self.assertEqual(n_vms, 1, "get_vms_to_backup fetched {} VMS instead of 1 VM".format(n_vms))
        self.assertEqual(vm_ref, next(vms).ref, "Fetched wrong VM to backup")

    def test_get_vms_to_backup_excluded_vms(self):
        vm_ref = get_by_uuid(self.xapi.VM, self.test_vm_uuid)
        vms, _ = get_vms_to_backup(
            self.xapi, self.master_url, self.session_id, excluded_vms=[self.test_vm_uuid])
        vm_refs = [vm.ref for vm in vms]
        self.assertNotIn(vm_ref, vm_refs)
