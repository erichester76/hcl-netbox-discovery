from netboxlabs.diode.sdk.ingester import Device, Entity
from transformer import Transformer
import logging

def prepare_device_data(devices):
    """
    Transforms Catalyst Center device data into Diode-compatible Device entities.
    """
    transformer = Transformer()
    entities = []

    for device in devices:
        try:
            # Use the Transformer class to handle field transformations
            location = transformer.transform_location(device.get("siteNameHierarchy"))
            if len(location)<1 : location=transformer.transform_site(device.get("siteNameHierarchy"))
            
            transformed_device = Device(
                name=transformer.transform_name(device.get("hostname")),
                device_type=transformer.transform_device_type(device.get("platformId")),
                manufacturer="Cisco",
                role=transformer.transform_role(device.get("role")),
                platform=transformer.transform_platform(device.get("softwareType"), device.get("softwareVersion")),
                serial=device.get("serialNumber").upper() if device.get("serialNumber") else None,
                site=transformer.transform_site(device.get("siteNameHierarchy")),
                #location=location,
                status=transformer.transform_status(device.get("reachabilityStatus")),
                tags=["Diode-CATC-Agent"],
            )
            entities.append(Entity(device=transformed_device))

        except Exception as e:
            logging.error(f"Error transforming device {device.get('hostname', 'unknown')}: {e}")

    return entities
