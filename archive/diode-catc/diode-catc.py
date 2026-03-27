import logging
import argparse
import os
from dotenv import load_dotenv
from catc_connector import connect_to_catc
from catc_fetcher import fetch_device_data#, fetch_interface_data
from data_conversion import prepare_device_data#, prepare_interface_data
from netboxlabs.diode.sdk import DiodeClient
from version import __version__

# Load .env file
load_dotenv()

def parse_arguments():
    """
    Parse command-line arguments with environment variable defaults,
    making all arguments effectively required.
    """
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Catalyst Center to Diode Agent")

    parser.add_argument(
        "--diode-server",
        default=os.getenv("DIODE_SERVER"),
        required=not os.getenv("DIODE_SERVER"),
        help="Diode server address (or set via DIODE_SERVER environment variable)"
    )
    parser.add_argument(
        "--diode-api-key",
        default=os.getenv("DIODE_API_KEY"),
        required=not os.getenv("DIODE_API_KEY"),
        help="Diode API token (or set via DIODE_API_KEY environment variable)"
    )
    parser.add_argument(
        "--catc-host",
        default=os.getenv("CATC_HOST"),
        required=not os.getenv("CATC_HOST"),
        help="Catalyst Center host (or set via CATC_HOST environment variable)"
    )
    parser.add_argument(
        "--catc-user",
        default=os.getenv("CATC_USER"),
        required=not os.getenv("CATC_USER"),
        help="Catalyst Center username (or set via CATC_USER environment variable)"
    )
    parser.add_argument(
        "--catc-password",
        default=os.getenv("CATC_PASSWORD"),
        required=not os.getenv("CATC_PASSWORD"),
        help="Catalyst Center password (or set via CATC_PASSWORD environment variable)"
    )
    parser.add_argument(
        "--catc-verify",
        default=os.getenv("CATC_VERIFY", "true").lower() in ("true", "1", "yes"),
        type=lambda x: x.lower() in ("true", "1", "yes"),
        help="Verify Catalyst Center SSL certificate (default: true, or set via CATC_VERIFY environment variable)"
    )

    return parser.parse_args()


def main():
    # Parse arguments
    args = parse_arguments()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info(f"Running Catalyst Center (CATC) to Diode Agent version {__version__}")

    try:
        # Connect to Catalyst Center
        logging.info(f"Attempting to connect to Catalyst Center at {args.catc_host}...")
        catc = connect_to_catc(args.catc_host, args.catc_user, args.catc_password, args.catc_verify)
        logging.info("Successfully connected to Catalyst Center.")

        # Connect to Diode
        logging.info(f"Attempting to connect to Diode at {args.diode_server}...")
        with DiodeClient(
            target=f"grpc://{args.diode_server}",
            app_name="diode-catc",
            app_version=__version__,
        ) as client:
            logging.info("Successfully connected to Diode.")

            # Fetch data from Catalyst Center
            logging.info("Fetching device data from Catalyst Center...")
            devices = fetch_device_data(catc)
            logging.info(f"Fetched {len(devices)} devices.")

            # logging.info("Fetching interface data for all devices...")
            # interfaces = []
            # for device in devices:
            #     device_interfaces = fetch_interface_data(catc, device["id"])
            #     interfaces.extend(device_interfaces)
            #     logging.info(f"Fetched {len(device_interfaces)} interfaces for device {device['hostname']}.")

            # Prepare data into Diode-compatible entities
            logging.info("Transforming device data into Diode-compatible format...")
            device_entities = prepare_device_data(devices)
            logging.info(f"Transformed {len(device_entities)} devices.")

            # logging.info("Transforming interface data into Diode-compatible format...")
            # interface_entities = [
            #     entity for iface in interfaces
            #     for entity in prepare_interface_data(iface)
            # ]
            # logging.info(f"Transformed {len(interface_entities)} interfaces.")

            # Ingest data into Diode
            logging.info("Ingesting data into Diode...")
            response = client.ingest(entities=device_entities)# + interface_entities)
            if response.errors:
                logging.error(f"Errors during ingestion: {response.errors}")
            else:
                logging.info("Data ingested successfully into Diode.")

    except Exception as e:
        logging.error(f"An error occurred during the process: {e}")
    finally:
        logging.info("Process completed.")

if __name__ == "__main__":
    main()
