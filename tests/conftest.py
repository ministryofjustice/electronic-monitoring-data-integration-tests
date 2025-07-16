import os
import logging

if os.getenv("CI") is None:
    from dotenv import load_dotenv
    load_dotenv()
    logging.warning("Local Run")


pytest_plugins = [
    "test_helpers.bastion_connection"
]



