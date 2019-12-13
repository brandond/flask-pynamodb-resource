Flask PynamoDB Resource
=======================
[![PyPI version](https://badge.fury.io/py/flask-pynamodb-resource.svg)](https://badge.fury.io/py/flask-pynamodb-resource)
[![Build Status](https://travis-ci.com/brandond/flask-pynamodb-resource.svg?branch=master)](https://travis-ci.com/brandond/flask-pynamodb-resource)

Presents PynamoDB models (DynamoDB tables) as Flask-RESTPlus resources.

Release Notes
-------------

As of release 1.0.0, this module creates resources using Flask-RESTPlus instead of Flask-RESTful. If you register your models with a Flask App or Blueprint,
you will find a Swagger UI and definition available at '/docs' and '/swagger.json'. If you register them with a Flask-RESTPlus API, they will simply be added
to your existing API definition.

This brings with it some changes to the serialization of a few contentious types such a date/datetime. Previously these were
either a string or number, depending on what type you used in PynamoDB. Now they should be reliably serialized as a ISO8601 datetime string.

Usage
-----

`flask_pynamodb_resource` provides factory methods to generate Flask-RESTPlus resources for PynamoDB Models and Indexes:

    modelresource_factory(model)
        Create a resource class for the given model.

    indexresource_factory(index, name=None)
        Create a resource class for the given index.

Examples
-------

Several simple REST APIs based on PynamoDB example are available in the [examples](examples/) directory.
