import logging
from dnacentersdk import api


def fetch_device_data(client):
    """
    Fetches data from Catalyst Center including sites and devices with their site associations.
    """
    try:
        devices = []
        sites = []
        offset = 1
        limit = 500
        items = 501

        # Fetch all sites in Catalyst Center
        while items > limit:
            response = client.sites.get_site(offset=offset, limit=limit)
            sites.extend(response.response if hasattr(response, "response") else [])
            items = len(response.response) if hasattr(response, "response") else 0
            logging.info(f"Found {len(sites)} sites in Catalyst Center.")
            offset += limit

        if not sites:
            raise ValueError("No sites found in Cisco Catalyst Center.")

        # Process each site to fetch associated devices
        items = 0
        for site in sites:
            items += 1
            site_name = site.get("siteNameHierarchy")
            logging.info(f"Processing Site #{items}: {site_name}")

            # Get devices associated with the site
            membership = client.sites.get_membership(site_id=site.id)
            if not membership or not hasattr(membership, "device"):
                continue

            for members in (membership.device or []):
                if not members or not hasattr(members, "response"):
                    continue

                for device in members.response:
                    if hasattr(device, "serialNumber"):
                        device["siteNameHierarchy"] = site_name
                        logging.info(f"Found device {device.hostname} in site {site_name}")
                        devices.append(device)

        return devices

    except Exception as e:
        logging.error(f"Error fetching data from Catalyst Center: {e}")
        raise
