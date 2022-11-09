import logging
import sys

from app.framework.injection import database_injector

database_injector.config(host='10.55.57.53', database='fundrates', user='zdj', password='xtKFE8k3ctqbYDOz')

logging.basicConfig(stream=sys.stderr)
logging.getLogger().setLevel(logging.INFO)
