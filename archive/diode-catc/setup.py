from setuptools import setup, find_packages
from version import __version__

setup(
    name="diode_catc_agent",
    version=__version__,
    description="A Cisco Catslyst Center to NetBox agent using the NetBoxLabs Diode SDK.",
    author="Eric Hester",
    author_email="hester1@clemson.edu",
    url="https://github.com/erichester76/diode_catc_agent",
    packages=find_packages(),
    install_requires=[
        "diode-sdk-python",
        "dnacentersdk",
        "python-dotenv"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "diode-catc-agent=main:main",
        ],
    },
)
