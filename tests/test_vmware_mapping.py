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


class TestVMwareExampleVmkNicMapping:
    def test_host_vmk_nics_include_short_host_context_for_description(self):
        cfg = load_config("mappings/vmware.hcl.example")
        host_obj = next(o for o in cfg.objects if o.name == "host")
        vmk_nic_block = host_obj.interfaces[1]

        assert (
            vmk_nic_block.source_items
            == "[{'_vnic': nic, '_host_name': replace(source('name'), '.clemson.edu', '')} for nic in (source('_enriched_vnics') or [])]"
        )

        fields = {f.name: f for f in vmk_nic_block.fields}
        assert fields["name"].value == "source('_vnic.device')"
        assert fields["mac_address"].value == "upper(source('_vnic.spec.mac'))"
        assert fields["type"].value == "'virtual'"
        assert fields["description"].value == "join(' ', [source('_host_name'), source('_vnic.device')])"

        assert vmk_nic_block.ip_addresses[0].source_items == "_vnic.spec.ip"
        assert vmk_nic_block.tagged_vlans[0].source_items == "_vnic._vlans"


class TestVMwareExampleHostClusterMapping:
    def test_vmware_example_hosts_include_cluster_assignment(self):
        cfg = load_config("mappings/vmware.hcl.example")
        host_obj = next(o for o in cfg.objects if o.name == "host")

        prereqs = {p.name: p for p in host_obj.prerequisites}
        assert prereqs["cluster_type"].args["name"] == "'VMWare'"
        assert prereqs["cluster"].args == {
            "name": "source('parent.name')",
            "type": "prereq('cluster_type')",
            "site": "prereq('site')",
        }
        assert prereqs["cluster"].optional is False

        fields = {f.name: f for f in host_obj.fields}
        assert fields["cluster"].value == "prereq('cluster')"


class TestVMwareExampleHostTenantMapping:
    def test_vmware_example_hosts_include_tenant_assignment(self):
        cfg = load_config("mappings/vmware.hcl.example")
        host_obj = next(o for o in cfg.objects if o.name == "host")

        prereqs = {p.name: p for p in host_obj.prerequisites}
        assert (
            prereqs["tenant"].args["name"]
            == "tenant_name if (tenant_name := regex_file(source('name'), 'host_to_tenant')) != source('name') else None"
        )

        fields = {f.name: f for f in host_obj.fields}
        assert fields["tenant"].value == "prereq('tenant')"


class TestVMwareExampleVmMetadataMapping:
    def test_vmware_example_vm_fields_include_device_platform_site_role_and_tenant(self):
        cfg = load_config("mappings/vmware.hcl.example")
        vm_obj = next(o for o in cfg.objects if o.name == "vm")

        prereqs = {p.name: p for p in vm_obj.prerequisites}
        assert prereqs["cluster"].args["name"] == "source('runtime.host.parent.name')"
        assert (
            prereqs["site"].args["name"]
            == "site_name if (site_name := regex_file(source('runtime.host.parent.name'), 'cluster_to_site')) != source('runtime.host.parent.name') else None"
        )
        assert prereqs["platform"].args["name"] == "source('guest.guestFullName') or 'Unknown'"
        assert (
            prereqs["role"].args["name"]
            == "role_name if (role_name := regex_file(source('name'), 'vm_to_role')) != source('name') else 'Virtual Machine'"
        )
        assert (
            prereqs["tenant"].args["name"]
            == "tenant_name if (tenant_name := regex_file(source('name'), 'vm_to_tenant')) != source('name') else None"
        )

        fields = {f.name: f for f in vm_obj.fields}
        assert fields["cluster"].value == "prereq('cluster')"
        assert fields["site"].value == "prereq('site')"
        assert fields["platform"].value == "prereq('platform')"
        assert fields["role"].value == "prereq('role')"
        assert fields["tenant"].value == "prereq('tenant')"

        assert fields["device"].type == "fk"
        assert fields["device"].resource == "dcim.devices"
        assert fields["device"].lookup == {
            "name": "replace(source('runtime.host.name'), '.clemson.edu', '')"
        }

    def test_vm_tagged_vlans_reuse_resolved_vm_site(self):
        cfg = load_config("mappings/vmware.hcl.example")
        vm_obj = next(o for o in cfg.objects if o.name == "vm")
        interface = vm_obj.interfaces[0]
        vlan = interface.tagged_vlans[0]

        assert vlan.source_items == "_vlans"
        site_field = next(f for f in vlan.fields if f.name == "site")
        assert site_field.value == "prereq('site')"
        assert site_field.type == "scalar"
        assert site_field.resource is None
        assert site_field.lookup is None
