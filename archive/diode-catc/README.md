# Diode catc Agent

This Python project is a catc to NetBox agent built with the [NetBoxLabs Diode SDK](https://github.com/netboxlabs/diode-sdk-python). It fetches data from catc and ingests it into NetBox.

## Features
- Pulls data such as devices, interfaces, ips, locations and sites from catc.
- Pushes data to NetBox using the Diode SDK.
- Supports configuration via `.env` files, environment variables, and command-line arguments.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/erichester76/diode_catc_agent.git
   cd diode_catc_agent
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install the package:
   ```bash
   python setup.py install
   ```

## Usage
1. Configure `.env` file:
   ```plaintext
   DIODE_SERVER=diode.example.com
   DIODE_TOKEN=your_diode_api_token
   CATC_HOST=catc.example.com
   CATC_USER=catc_user
   CATC_PASSWORD=catc_password
   ```

2. Run the agent:
   ```bash
   python diode-catc.py
   ```

3. Or use command-line arguments:
   ```bash
   python diode-catc.py --diode-server diode.local --diode-token abc123 \
       --catc-host catc.local --catc-user admin --catc-password password
   ```

## License
This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
