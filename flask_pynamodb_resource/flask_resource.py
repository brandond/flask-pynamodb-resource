import logging
from ujson import dumps

from flask import request
from flask_restful import Api, Resource
from flask_restful.representations import json as restful_json
from pynamodb.attributes import Attribute
from pynamodb.indexes import Index
from pynamodb.models import Model
from pynamodb.exceptions import PutError

from six import string_types

logger = logging.getLogger(__name__)
restful_json.dumps = dumps


class IndexResource(Resource):
    """Presents a PynamoDB index as a flask-RESTful resource"""

    name = None
    index = None
    hash_keyname = None
    range_keyname = None
    route_prefix = None

    @classmethod
    def build_routes(cls):
        if not cls.route_prefix:
            cls.route_prefix = '/{0}'.format(cls.index.Meta.model.Meta.table_name)

        if not cls.name:
            cls.name = cls.index.Meta.index_name

        resource_routes = []

        resource_routes.append('{0}/{1}/<{2}>'.format(cls.route_prefix, cls.name, cls.hash_keyname))
        if cls.range_keyname:
            resource_routes.append('{0}/{1}/<{2}>/<{3}>'.format(cls.route_prefix, cls.name, cls.hash_keyname, cls.range_keyname))

        return resource_routes

    def get(self, *args, **kwargs):
        try:
            hash_key = self._get_hash(kwargs)
            if self.range_keyname and self.range_keyname in kwargs:
                range_key = self._get_range(kwargs)
                return [o.to_dict() for o in self.index.query(hash_key, **range_key)]
            else:
                return [o.to_dict() for o in self.index.query(hash_key)]
        except self.index.Meta.model.DoesNotExist:
            return ({'error': 'Record not found'}, 404)
        except Exception as e:
            logger.exception('Failed to get record')
            return ({'error': e.message}, 500)

    def _get_hash(self, kwargs):
        """
        Extract hash key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.hash_keyname)
        attr = self.index._get_attributes()[self.hash_keyname]
        return attr.deserialize(value)

    def _get_range(self, kwargs):
        """
        Extract range key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.range_keyname)
        attr = self.index._get_attributes()[self.range_keyname]
        return {self.range_keyname+'__eq': attr.deserialize(value)}


class ModelResource(Resource):
    """
    Presents a PynamoDB model as a Flask-RESTful resource.
    """

    model = None
    hash_keyname = None
    range_keyname = None
    route_prefix = None

    @classmethod
    def register(cls, app, route_prefix=None):
        api = Api(app)

        if route_prefix:
            cls.route_prefix = route_prefix

        for item in dir(cls.model):
            item_cls = getattr(getattr(cls.model, item), "__class__", None)
            if item_cls is None:
                continue
            if issubclass(item_cls, (Index, )):
                item_obj = getattr(cls.model, item)
                index_cls = indexresource_factory(item_obj, item)
                index_cls.route_prefix = cls.route_prefix
                api.add_resource(index_cls, *index_cls.build_routes())

        api.add_resource(cls, *cls.build_routes())

    @classmethod
    def build_routes(cls):
        if not cls.route_prefix:
            cls.route_prefix = '/{0}'.format(cls.model.Meta.table_name)

        resource_routes = []
        resource_routes.append(cls.route_prefix)
        resource_routes.append('{0}/<{1}>'.format(cls.route_prefix, cls.hash_keyname))

        if cls.range_keyname:
            resource_routes.append('{0}/<{1}>/<{2}>'.format(cls.route_prefix, cls.hash_keyname, cls.range_keyname))

        return resource_routes

    def get(self, *args, **kwargs):
        """
        Get existing records. Get, query, or scan will be called depending on parameters.
        """
        filters = None
        for k, v in request.args.items():
            condition = self._get_filter(k, v)
            if condition is not None:
                filters = filters & condition if filters is not None else condition

        try:
            if self.hash_keyname in kwargs:
                hash_key = self._get_hash(kwargs)
                if self.range_keyname:
                    if self.range_keyname in kwargs:
                        range_key = self._get_range(kwargs)
                        return self.model.get(hash_key, range_key).to_dict()
                    else:
                        return [o.to_dict() for o in self.model.query(hash_key, filter_condition=filters)]
                else:
                    return self.model.get(hash_key).to_dict()
            else:
                return [o.to_dict() for o in self.model.scan(filter_condition=filters)]
        except self.model.DoesNotExist:
            return ({'error': 'Record not found'}, 404)
        except Exception as e:
            logger.exception('Failed to get record')
            return ({'error': e.message}, 500)

    def delete(self, *args, **kwargs):
        """
        Delete a record. Keys must be passed as parameters.
        """
        try:
            if self.hash_keyname in kwargs:
                hash_key = self._get_hash(kwargs)
                if self.range_keyname:
                    if self.range_keyname in kwargs:
                        range_key = self._get_range(kwargs)
                        self.model.get(hash_key, range_key).delete()
                        return ('', 204)
                else:
                    self.model.get(hash_key).delete()
                    return ('', 204)
        except self.model.DoesNotExist:
            pass
        except Exception as e:
            logger.exception('Failed to delete record')
            return ({'error': e.message}, 500)

        return ({'error': 'Record not found'}, 404)

    def post(self, *args, **kwargs):
        """
        Create a new record. Must be sent to the base resource with no parameters.
        """
        if kwargs:
            return ({'error': 'Cannot POST with URL parameters'}, 400)

        try:
            data = self._request_data()
            for (k, v) in data.items():
                if isinstance(v, string_types):
                    data[k] = getattr(self.model, k).deserialize(v)
            data['hash_key'] = self._get_hash(data)
            if self.range_keyname:
                data['range_key'] = self._get_range(data)
            self.model(**data).save()
            return ('', 204)
        except (AttributeError, PutError) as e:
            logger.exception('Failed to save record')
            return ({'error': e.message}, 400)
        except Exception as e:
            logger.exception('Failed to create record')
            return ({'error': e.message}, 500)

    def put(self, *args, **kwargs):
        """
        Update an existing record. Handled same as POST, except parameters are required.
        """
        if not kwargs:
            return ({'error': 'Record not found'}, 404)

        return self.post()

    def _get_hash(self, kwargs):
        """
        Extract hash key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.hash_keyname)
        attr = getattr(self.model, self.hash_keyname)
        return attr.deserialize(value)

    def _get_range(self, kwargs):
        """
        Extract range key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.range_keyname)
        attr = getattr(self.model, self.range_keyname)
        return attr.deserialize(value)

    def _get_filter(self, param, value):
        attr = getattr(self.model, param, None)
        if attr is None or attr.is_hash_key or attr.is_range_key:
            return None
        else:
            return (attr == value)

    def _request_data(self):
        """
        Get request data, either from JSON, or form fields
        """
        data = request.get_json()
        if data is None:
            data = request.values.to_dict()
        return data


def modelresource_factory(model):
    """
    Create a resource class for the given model.
    """
    cls = type('{0}Resource'.format(model.__name__), (ModelResource,), {})

    cls.model = model
    get_attributes = getattr(model, 'get_attributes', model._get_attributes)
    for name, attr in get_attributes().items():
        if attr.is_hash_key:
            cls.hash_keyname = name
        elif attr.is_range_key:
            cls.range_keyname = name

    return cls


def indexresource_factory(index, name=None):
    """
    Create a resource class for the given index.
    """
    cls = type('{0}Resource'.format(index.__class__.__name__), (IndexResource,), {})

    cls.index = index
    get_attributes = getattr(index, 'get_attributes', index._get_attributes)
    if name:
        cls.name = name
    for name, attr in get_attributes().items():
        if attr.is_hash_key:
            cls.hash_keyname = name
        elif attr.is_range_key:
            cls.range_keyname = name

    return cls


def _to_dict(self):
    ret = dict()
    for k, v in self.attribute_values.items():
        if isinstance(v, dict):
            ret[k] = dict([(sk, sv.to_dict() if isinstance(sv, Attribute) else sv) for sk, sv in v.items()])
        if isinstance(v, list):
            ret[k] = [l.to_dict() if isinstance(l, Attribute) else l for l in v]
        elif isinstance(v, Attribute):
            ret[k] = v.to_dict()
        else:
            ret[k] = v
    return ret


Model.to_dict = _to_dict
Attribute.to_dict = _to_dict
