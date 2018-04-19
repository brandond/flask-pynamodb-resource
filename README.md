Flask PynamoDB Resource
=======================

Presents PynamoDB models (DynamoDB tables) as Flask-RESTful resources

Usage
-----

`flask_pynamodb_resource` provides factory methods to generate Flask-RESTful resources for PynamoDB Models and Indexes:

    modelresource_factory(model)
        Create a resource class for the given model.

    indexresource_factory(index, name=None)
        Create a resource class for the given index.

Example
-------

```python
from __future__ import print_function
from flask import Flask
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from flask_pynamodb_resource import modelresource_factory

# Create a sample model
class Thread(Model):
    class Meta:
        table_name = 'Thread'

    forum_name = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(default=0)

# Ensure tables exist before serving content
def create_table(*args, **kwargs):
    Thread.create_table(wait=True)

# Set up Flask app
app = Flask(__name__)
app.before_first_request(create_table)

# Register auto-generated routes for the model, under the '/api/v1/thread' prefix
modelresource_factory(Thread).register(app, '/api/v1/thread')

# Print rules
for rule in app.url_map.iter_rules():
    print('{} => {}'.format(rule.rule, rule.endpoint))
```
