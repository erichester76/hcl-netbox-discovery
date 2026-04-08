"""Targeted tests for the shipped VMware example mapping."""

from collector.config import load_config


class TestVMwareExamplePlatformPrereqs:
    def test_vmware_example_platform_prereqs_do_not_set_manufacturer(self):
        cfg = load_config("mappings/vmware.hcl.example")
        platform_prereqs = []
        for obj in cfg.objects:
            for prereq in obj.prerequisites:
                if prereq.method != "ensure_platform":
                    continue
                platform_prereqs.append((obj.name, prereq))
                assert "manufacturer" not in prereq.args
                assert "manufacturer_id" not in prereq.args
                assert "manufacturer_name" not in prereq.args

        vm_platform_prereqs = [prereq for name, prereq in platform_prereqs if name == "vm"]
        assert len(vm_platform_prereqs) == 1
        assert vm_platform_prereqs[0].args["name"] == "source('guest.guestFullName') or 'Unknown'"


class TestVMwareExamplePhysicalNicMapping:
    def test_host_physical_nics_include_host_context_for_description(self):
        cfg = load_config("mappings/vmware.hcl.example")
        host_obj = next(o for o in cfg.objects if o.name == "host")
        physical_nic_block = host_obj.interfaces[0]

        assert (
            physical_nic_block.source_items
            == "[{'_nic': nic, '_host_name': source('name')} for nic in (source('config.network.pnic') or [])]"
        )

        fields = {f.name: f for f in physical_nic_block.fields}
        assert fields["name"].value == "source('_nic.device')"
        assert fields["mac_address"].value == "upper(source('_nic.mac'))"
        assert fields["type"].value == (
            "map_value(source('_nic.linkSpeed.speedMb'), {1000: '1000base-t', 10000: '10gbase-x-sfpp', 25000: '25gbase-x-sfp28', 40000: '40gbase-x-qsfpp', 100000: '100gbase-x-qsfp28'}, '1000base-t')"
        )
        assert fields["speed"].value == "source('_nic.linkSpeed.speedMb')"
        assert fields["duplex"].value == (
            "map_value(source('_nic.linkSpeed.duplex'), {True: 'full', False: 'half'}, None)"
        )
        assert fields["description"].value == "join(' ', [source('_host_name'), source('_nic.device')])"
