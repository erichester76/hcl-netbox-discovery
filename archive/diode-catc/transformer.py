import re
import yaml
import logging

class Transformer:

    def transform_name(self,hostname):
        """
        Transforms hostname to name without the domain and converts to lowercase.
        """
        if not hostname:
            return None
        return hostname.lower().split(".clemson.edu")[0]

    # Utility function for regex replacement
    def regex_replace(vself, alue, pattern, replacement):
        """
        Applies a regex pattern replacement to a given string value.
        """
        import re
        return re.sub(pattern, replacement, value)


    def transform_device_type(self, platform_id):
        """
        Transforms platformId to device type with replacements for Cisco Catalyst models.
        """
        if not platform_id:
            return None
        device_type = platform_id
        replacements = [
            (r"^C", "Catalyst "),
            (r"^WS\-C", "Catalyst "),
            (r"^IE\-", "Catalyst IE"),
            (r"^AIR\-AP", "Catalyst "),
            (r"^AIR\-CAP", "Catalyst "),
            (r"\-K9$", ""),
            (r"^([^\,]+)\,.+", r"\1"),
        ]
        for pattern, replacement in replacements:
            device_type = regex_replace(device_type, pattern, replacement)
        return {"model": device_type, "manufacturer": {"name": "Cisco"}}


    def transform_role(self, role):
        """
        Transforms role into title case and looks up the object.
        """
        if not role:
            return None
        return role.title()


    def transform_platform(self, software_type, software_version):
        """
        Combines softwareType and softwareVersion into a single platform string.
        """
        software_type = software_type.upper() if software_type else "IOS"
        return f"{software_type} {software_version}"


    def transform_site(site_hierarchy):
        """
        Extracts the site name from the siteNameHierarchy.
        """
        if not site_hierarchy:
            return None
        return regex_replace(site_hierarchy, r"^[^/]+/[^/]+/([^/]+)/*.*$", r"\1")


    def transform_location(self,site_hierarchy):
        """
        Extracts the location from the siteNameHierarchy.
        """
        if not site_hierarchy:
            return None
        return regex_replace(site_hierarchy, r"^[^/]+/[^/]+/[^/]+/([^/]+)/*.*$", r"\1")


    def transform_status(self,reachability_status):
        """
        Maps reachabilityStatus to device status.
        """
        if not reachability_status:
            return None
        return (
            "active" if "Reachable" in reachability_status else
            "offline" if "Unreachable" in reachability_status else
            None
        )


    