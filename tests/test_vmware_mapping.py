"""Targeted tests for the shipped VMware example mapping."""

from collector.config import load_config


class TestVmwareExamplePlatformPrereqs:
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
        assert vm_platform_prereqs[0].args["name"] == "coalesce(source('guest.guestFullName'), 'Unknown')"
