from dnacentersdk import api

def connect_to_catc(host, username, password, verify=True):
    """
    Establishes a connection to Cisco Catalyst Center and returns the SDK client.
    """
    try:
        catc = api.DNACenterAPI(
            base_url=f"https://{host}",
            username=username,
            password=password,
            verify=verify
        )
        return catc
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Cisco Catalyst Center: {e}")
