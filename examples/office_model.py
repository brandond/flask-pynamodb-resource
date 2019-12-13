#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import logging

from botocore.session import Session
from flask import Blueprint, Flask
from pynamodb.attributes import ListAttribute, MapAttribute, NumberAttribute, UnicodeAttribute
from pynamodb.models import Model

from flask_pynamodb_resource import create_resource


# Create a simple model inspired by the PynamoDB example library
class Location(MapAttribute):
    lat = NumberAttribute(attr_name='latitude')
    lng = NumberAttribute(attr_name='longitude')
    name = UnicodeAttribute()


class Person(MapAttribute):
    fname = UnicodeAttribute(attr_name='firstName')
    lname = UnicodeAttribute()
    age = NumberAttribute()


class OfficeEmployeeMap(MapAttribute):
    office_employee_id = NumberAttribute()
    person = Person()
    office_location = Location()


class Office(Model):
    class Meta:
        table_name = 'OfficeModel'
        region = Session().get_config_variable('region')
        write_capacity_units = 1
        read_capacity_units = 1

    office_id = NumberAttribute(hash_key=True)
    address = Location()
    employees = ListAttribute(of=OfficeEmployeeMap)


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
    Office.create_table(wait=True)


# Attach APIs to a blueprint to avoid cluttering up the root URL prefix
api_v1 = Blueprint('api_v1', __name__)

# Create the resources and register them with the blueprint
# Also override the endpoint name for each resource; by default it simply uses the model's table name
create_resource(Office).register(api_v1, '/offices')

# Register the blueprint with the app AFTER attaching the resources
app.register_blueprint(api_v1, url_prefix='/api/v1')

# Register table creation callback
app.before_first_request(create_table)

# Print rules to stdout
for rule in app.url_map.iter_rules():
    logging.info('{} => {}'.format(rule.rule, rule.endpoint))
