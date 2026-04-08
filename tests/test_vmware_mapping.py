"""Targeted tests for the shipped VMware example mapping."""

from collector.config import load_config


class TestVmwareExamplePlatformPrereqs:
    def test_vmware_example_platform_prereqs_do_not_set_manufacturer(self):
        cfg = load_config("mappings/vmware.hcl.example")
        for obj in cfg.objects:
            for prereq in obj.prerequisites:
                if prereq.method != "ensure_platform":
                    continue
                assert "manufacturer" not in prereq.args
                assert "manufacturer_id" not in prereq.args
                assert "manufacturer_name" not in prereq.args
