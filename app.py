import sys
import os
sys.path.append('./repository')
if 'JWT_SECRET' not in os.environ:
    os.environ['JWT_SECRET'] = 'foosecret'

from repository.factory import create_api_app

app = create_api_app()