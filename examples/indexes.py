#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import logging

from botocore.session import Session
from flask import Blueprint, Flask
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex, LocalSecondaryIndex
from pynamodb.models import Model

from flask_pynamodb_resource import create_resource


# Create a simple model inspired by the PynamoDB example library
class ViewIndex(GlobalSecondaryIndex):
    """
    This class represents a global secondary index
    """
    class Meta:
        # You can override the index name by setting it below
        index_name = "viewIdx"
        read_capacity_units = 1
        write_capacity_units = 1
        # All attributes are projected
        projection = AllProjection()
    # This attribute is the hash key for the index
    # Note that this attribute must also exist
    # in the model
    view = NumberAttribute(default=0, hash_key=True)


class TestModel(Model):
    """
    A test model that uses a global secondary index
    """
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "TestModel"
        region = Session().get_config_variable('region')
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    view_index = ViewIndex()
    view = NumberAttribute(default=0)


class GamePlayerOpponentIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GamePlayerOpponentIndex"
        projection = AllProjection()
    player_id = UnicodeAttribute(hash_key=True)
    winner_id = UnicodeAttribute(range_key=True)


class GameOpponentTimeIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameOpponentTimeIndex"
        projection = AllProjection()
    winner_id = UnicodeAttribute(hash_key=True)
    created_time = UnicodeAttribute(range_key=True)


class GameModel(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameModel"
        region = Session().get_config_variable('region')
    player_id = UnicodeAttribute(hash_key=True)
    created_time = UTCDateTimeAttribute(range_key=True)
    winner_id = UnicodeAttribute()
    loser_id = UnicodeAttribute(null=True)

    player_opponent_index = GamePlayerOpponentIndex()
    opponent_time_index = GameOpponentTimeIndex()


# Set up logging
logging.basicConfig(level='DEBUG')
logging.getLogger('botocore').setLevel('INFO')

# Set up Flask app
app = Flask(__name__, static_folder=None)

# Serve up a simple welcome page that links to SwaggerUI
@app.route('/')
def index():
    return "<!DOCTYPE html><html><body><a href='/api/v1/doc'><tt>Click here for SwaggerUI</tt></a></body></html>"


# Create a callback function to ensure tables exist
def create_table(*args, **kwargs):
    TestModel.create_table(wait=True)
    GameModel.create_table(wait=True)


# Attach APIs to a blueprint to avoid cluttering up the root URL prefix
api_v1 = Blueprint('api_v1', __name__)

# Create the resources and register them with the blueprint
# Also override the endpoint name for each resource; by default it simply uses the model's table name
create_resource(TestModel).register(api_v1, '/threads')
create_resource(GameModel).register(api_v1, '/games')

# Register the blueprint with the app AFTER attaching the resources
app.register_blueprint(api_v1, url_prefix='/api/v1')

# Register table creation callback
app.before_first_request(create_table)

# Print rules to stdout
for rule in app.url_map.iter_rules():
    logging.info('{} => {}'.format(rule.rule, rule.endpoint))
