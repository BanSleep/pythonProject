#config.py

import pathlib
from flask import Flask

from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy

basedir = pathlib.Path(__file__).parent.resolve()
app = Flask(__name__)

