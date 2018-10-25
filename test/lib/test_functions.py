import os
import re
from unittest import TestCase

import yaml

from handlers.common import get_by_uuid
from handlers.vm import VM
from lib import XenAPI
from lib.functions import random_xen_mac, vm_definition_to_file, get_timestamp, vm_definition_from_file


class TestRandomXenMac(TestCase):
    def test_random_xen_mac(self):
        random_mac = random_xen_mac()
        assert re.match("^([a-z0-9]{2}:){5}[a-z0-9]{2}$", random_mac)


class TestVmDefinitionHandling(TestCase):
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

    def test_vm_definition_to_from_file(self):
        vm_ref = get_by_uuid(self.xapi.VM, self.test_vm_uuid)
        vm_def = VM(self.xapi, self.master_url, self.session_id, vm_ref).get_record()
        ts = get_timestamp(to_str=True)

        backup_def_fn = vm_definition_to_file(vm_def, ".", ".", ts)
        self.assertTrue(os.path.exists(backup_def_fn))

        vm_def_read = vm_definition_from_file(backup_def_fn)
        self.assertDictEqual(vm_def, vm_def_read)

        os.remove(backup_def_fn)
