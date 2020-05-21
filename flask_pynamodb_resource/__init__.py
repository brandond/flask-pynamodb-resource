import logging
from collections import MutableMapping
from inspect import isclass

from flask import request
from flask_restx import Api, Namespace, Resource, fields, marshal
from flask_restx.model import ModelBase
from pynamodb import attributes, indexes
from pynamodb.exceptions import PutError
from six import string_types

logger = logging.getLogger(__name__)


class PynamoNumber(fields.Arbitrary):
    """An adaptive number type that maintain numeric serialization for either int or float types"""
    # PynamoDB stores both ints and floats in a generic 'Number' type that doesn't map well
    # to Integer, Float, or Fixed. Arbitrary would work but it serializes as a string.
    # We need something that will take either an int or a float and output an actual numeric type.
    __schema_type__ = 'number'

    def format(self, value):
        if value is None:
            return None
        elif isinstance(value, string_types):
            if '.' in value:
                return float(value)
            else:
                return int(value)
        elif isinstance(value, (int, float)):
            return value
        else:
            raise ValueError('Unsupported number format')


class PynamoMapAttribute(fields.Raw):
    """A wrapper to expose MapAttributes with no required properties as a Raw object"""
    _name = ''

    def __init__(self, name, **kwargs):
        super(PynamoMapAttribute, self).__init__(cls_or_instance=fields.String, **kwargs)
        self._name == name

    def output(self, key, obj, ordered=False):
        value = getattr(obj, key, None)
        if isinstance(value, attributes.MapAttribute):
            return value.attribute_values

    def schema(self):
        schema = super(PynamoMapAttribute, self).schema()
        schema['type'] = 'object'
        schema['additionalProperties'] = fields.String().__schema__
        return schema


class PynamoModel(ModelBase, dict, MutableMapping):
    """Abstraction layer to map PynamoDB model attributes to Flask-RESTX model fields"""

    TYPEMAP = {
        attributes.BooleanAttribute: fields.Boolean(),
        attributes.UnicodeAttribute: fields.String(),
        attributes.TTLAttribute: fields.DateTime(),
        attributes.UTCDateTimeAttribute: fields.DateTime(),
        attributes.NumberAttribute: PynamoNumber(),
        attributes.NumberSetAttribute: fields.List(PynamoNumber()),
        attributes.UnicodeSetAttribute: fields.List(fields.String()),
    }

    if hasattr(attributes, 'LegacyBooleanAttribute'):
        TYPEMAP[attributes.LegacyBooleanAttribute] = fields.Boolean()

    if hasattr(attributes, 'MapAttributeMeta'):
        MAPMETA = attributes.MapAttributeMeta
    else:
        MAPMETA = attributes.AttributeContainerMeta

    def __init__(self, name, base, namespace, *args, **kwargs):
        super(PynamoModel, self).__init__(name=name, *args, **kwargs)
        self.name = name
        self.required = set()
        self.nested_models = {}
        self._translate(base, namespace)

    def _translate(self, base, namespace):
        for name, attr in get_attributes(base).items():
            self[name] = self._translate_attribute(name, attr, namespace)
            if attr.is_hash_key:
                self.required.add(name)
            elif attr.is_range_key:
                self.required.add(name)

        # Add projected attributes for secondary indexes
        if isclass(base) and issubclass(base, indexes.Index):
            if isinstance(base.Meta.projection, indexes.IncludeProjection):
                project_keys = base.Meta.projection.non_key_attributes
            elif isinstance(base.Meta.projection, indexes.AllProjection):
                project_keys = get_attributes(base.Meta.model).keys()
            else:
                return

            for name, attr in get_attributes(base.Meta.model).items():
                if name in project_keys:
                    self[name] = self._translate_attribute(name, attr, namespace)

    def _get_or_create_nested(self, name, attr, namespace):
        # TODO: will this work for recursively nested attributes?
        # I don't even know if that's possible with PynamoDB but we should test.
        nested_name = '{}.{}'.format(self.name, name)
        if nested_name in self.nested_models:
            logger.debug('Using existing nested model {}'.format(nested_name))
        else:
            logger.debug('Creating new nested model {}'.format(nested_name))
            nested_model = PynamoModel(name=nested_name, base=attr, namespace=namespace)
            namespace.add_model(nested_name, nested_model)
            self.nested_models[nested_name] = nested_model
        return fields.Nested(self.nested_models[nested_name])

    def _translate_attribute(self, name, attr, namespace):
        logger.debug('Translating {}.{}'.format(self.name, name))
        field = None

        if isinstance(attr, attributes.MapAttribute):
            if attr.__class__ == attributes.MapAttribute:
                map_name = name.title()
                field = PynamoMapAttribute(map_name)
            else:
                map_name = attr.__class__.__name__
                field = self._get_or_create_nested(map_name, attr, namespace)
        elif isinstance(attr, self.MAPMETA):
            field = self._get_or_create_nested(name, attr, namespace)
        elif isinstance(attr, attributes.ListAttribute):
            if attr.element_type:
                element_type = self._translate_attribute(attr.element_type.__name__, attr.element_type, namespace)
            else:
                element_type = fields.String()
            field = fields.List(element_type)
        else:
            for p_attr, r_field in self.TYPEMAP.items():
                if isinstance(attr, p_attr):
                    field = r_field
                    break
            else:
                logger.warn('Unhandled attribute {} mapped as Raw'.format(attr))
                field = fields.Raw()

        logger.debug('Translated {}.{}={} as {}'.format(self.name, name, attr.__class__, field.__class__))
        return field

    @property
    def _schema(self):
        return {
            'required': list(self.required),
            'properties': dict((n, f.__schema__) for n, f in self.items()),
            'type': 'object',
        }


class PynamoResource(Resource):
    """Base class for presenting PynamoDB models and indexes as a REST resource"""
    name = None
    rest_model = None
    pynamo_model = None
    hash_keyname = None
    range_keyname = None

    @classmethod
    def _register_routes(cls, ns):
        raise NotImplementedError()

    def dispatch_request(self, *args, **kwargs):
        """
        Deserialize path-based arguments to correct type before passing up the stack
        """
        for k, v in kwargs.items():
            kwargs[k] = getattr(self.pynamo_model, k).deserialize(v)
        return super(PynamoResource, self).dispatch_request(*args, **kwargs)


class IndexResource(PynamoResource):
    """Presents a PynamoDB index as a REST resource"""

    @classmethod
    def _register_routes(cls, ns):
        cls.rest_model = PynamoModel(name=cls.__name__,
                                     base=cls.pynamo_model,
                                     namespace=ns)
        ns.add_model(cls.rest_model.name, cls.rest_model)

        hash_param = {'name': cls.hash_keyname,
                      'in': 'path',
                      'required': True,
                      'type': cls.rest_model[cls.hash_keyname].__schema_type__}

        get_multi_doc = {'responses': {200: ('Success', [cls.rest_model]),
                                       500: 'Failed to get records'},
                         'description': 'Returns a list of records'}

        ns.add_resource(cls, '/{0}/'.format(cls.name),
                        route_doc={'description': '',
                                   'get': get_multi_doc,
                                   })
        ns.add_resource(cls, '/{0}/<{1}>'.format(cls.name, cls.hash_keyname),
                        route_doc={'description': '',
                                   'params': {cls.hash_keyname: hash_param},
                                   'get': get_multi_doc
                                   })
        if cls.range_keyname:
            range_param = {'name': cls.range_keyname,
                           'in': 'path',
                           'required': True,
                           'type': cls.rest_model[cls.range_keyname].__schema_type__}

            ns.add_resource(cls, '/{0}/<{1}>/<{2}>'.format(cls.name, cls.hash_keyname, cls.range_keyname),
                            route_doc={'description': '',
                                       'params': {cls.hash_keyname: hash_param, cls.range_keyname: range_param},
                                       'get': get_multi_doc,
                                       })

    def get(self, *args, **kwargs):
        """
        Get a list of records from a secondary index.
        Attribute availability may differ from the parent model, depending on the index's projection.
        """
        try:
            if self.hash_keyname in kwargs:
                hash_key = self._get_hash(kwargs)
                if self.range_keyname and self.range_keyname in kwargs:
                    range_key = self._get_range(kwargs)
                    return [marshal(o, self.rest_model) for o in self.pynamo_model.query(hash_key, range_key)]
                else:
                    return [marshal(o, self.rest_model) for o in self.pynamo_model.query(hash_key)]
            else:
                return [marshal(o, self.rest_model) for o in self.pynamo_model.scan()]
        except Exception as e:
            logger.exception('Failed to get record')
            return ({'message': str(e)}, 500)

    def _get_hash(self, kwargs):
        """
        Extract hash key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.hash_keyname)
        return value

    def _get_range(self, kwargs):
        """
        Extract range key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.range_keyname)
        attr = getattr(self.pynamo_model, self.range_keyname)
        return (attr == value)


class ModelResource(PynamoResource):
    """
    Presents a PynamoDB model as a Flask-RESTX resource.
    """
    @classmethod
    def register(cls, app, url_prefix=None):
        if not url_prefix:
            url_prefix = '/{0}'.format(cls.pynamo_model.Meta.table_name)

        if isinstance(app, Api):
            logger.debug('Using existing Api')
            api = app
        elif hasattr(app, '__api__'):
            logger.debug('Using App Api')
            api = app.__api__
        else:
            logger.debug('Creating new Api for App')
            api = Api(app, doc='/doc')
            app.__api__ = api

        ns = Namespace(cls.pynamo_model.__name__,
                       'PynamoDB model {}.{}'.format(cls.pynamo_model.__module__, cls.pynamo_model.__name__),
                       url_prefix)
        cls._register_routes(ns)

        for item in dir(cls.pynamo_model):
            item_cls = getattr(getattr(cls.pynamo_model, item), "__class__", None)
            if item_cls is None:
                continue
            if issubclass(item_cls, indexes.Index):
                index_cls = create_resource(item_cls, item)
                index_cls._register_routes(ns)

        api.add_namespace(ns)

    @classmethod
    def _register_routes(cls, ns):
        cls.rest_model = PynamoModel(name=cls.__name__,
                                     base=cls.pynamo_model,
                                     namespace=ns)
        ns.add_model(cls.rest_model.name, cls.rest_model)

        hash_param = {'name': cls.hash_keyname,
                      'in': 'path',
                      'required': True,
                      'type': cls.rest_model[cls.hash_keyname].__schema_type__}

        delete_doc = {'responses': {204: 'Success',
                                    404: 'Record not found',
                                    500: 'Failed to get record'},
                      'description': 'Deletes a single record'}
        get_multi_doc = {'responses': {200: ('Success', [cls.rest_model]),
                                       404: 'Records not found',
                                       500: 'Failed to get records'},
                         'description': 'Returns a list of records'}
        get_single_doc = {'responses': {200: ('Success', cls.rest_model),
                                        404: 'Record not found',
                                        500: 'Failed to get record'},
                          'description': 'Returns a single record'}
        post_doc = {'responses': {201: ('Success', cls.rest_model, {'headers': {'Location': 'The URL of the created resource'}}),
                                  400: 'Invalid record',
                                  409: 'Record already exists',
                                  500: 'Failed to store record'},
                    'description': 'Creates a new record. Attempts to create a duplicate record will result in an error.',
                    'expect': [cls.rest_model]}
        put_doc = {'responses': {200: ('Success', cls.rest_model),
                                 400: 'Invalid record',
                                 404: 'Record not found',
                                 500: 'Failed to store record'},
                   'description': 'Updates an existing record. Hash and range key may not be changed; '
                                  'if you wish to update these fields the existing record must be deleted '
                                  'and recreated with the correct values.',
                   'expect': [cls.rest_model]}

        ns.add_resource(cls, '/',
                        methods=['get', 'post'],
                        route_doc={'description': '',
                                   'get': get_multi_doc,
                                   'post': post_doc,
                                   })

        ns.add_resource(cls, '/<{0}>'.format(cls.hash_keyname),
                        methods=['get'] if cls.range_keyname else ['delete', 'get', 'put'],
                        route_doc={'description': '',
                                   'params': {cls.hash_keyname: hash_param},
                                   'delete': delete_doc,
                                   'get': get_multi_doc if cls.range_keyname else get_single_doc,
                                   'put': put_doc,
                                   })

        if cls.range_keyname:
            range_param = {'name': cls.range_keyname,
                           'in': 'path',
                           'required': True,
                           'type': cls.rest_model[cls.range_keyname].__schema_type__}

            ns.add_resource(cls, '/<{0}>/<{1}>'.format(cls.hash_keyname, cls.range_keyname),
                            methods=['delete', 'get', 'put'],
                            route_doc={'description': '',
                                       'params': {cls.hash_keyname: hash_param, cls.range_keyname: range_param},
                                       'delete': delete_doc,
                                       'get': get_single_doc,
                                       'put': put_doc,
                                       })

    def get(self, *args, **kwargs):
        """
        Get a record or list of records.
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
                        return marshal(self.pynamo_model.get(hash_key, range_key), self.rest_model)
                    else:
                        return [marshal(o, self.rest_model) for o in self.pynamo_model.query(hash_key, filter_condition=filters)]
                else:
                    return marshal(self.pynamo_model.get(hash_key), self.rest_model)
            else:
                return [marshal(o, self.rest_model) for o in self.pynamo_model.scan(filter_condition=filters)]
        except self.pynamo_model.DoesNotExist:
            return ({'message': 'Record not found'}, 404)
        except Exception as e:
            logger.exception('Failed to get record')
            return ({'message': str(e)}, 500)

    def delete(self, *args, **kwargs):
        """
        Delete a record.
        """
        try:
            if self.hash_keyname in kwargs:
                hash_key = self._get_hash(kwargs)
                if self.range_keyname:
                    if self.range_keyname in kwargs:
                        range_key = self._get_range(kwargs)
                        self.pynamo_model.get(hash_key, range_key).delete()
                        return ('', 204)
                else:
                    self.pynamo_model.get(hash_key).delete()
                    return ('', 204)
        except self.pynamo_model.DoesNotExist:
            pass
        except Exception as e:
            logger.exception('Failed to delete record')
            return ({'message': str(e)}, 500)

        return ({'message': 'Record not found'}, 404)

    def post(self, *args, **kwargs):
        """
        Create a new record.
        """
        return self._save(create=True, *args, **kwargs)

    def put(self, *args, **kwargs):
        """
        Update an existing record.
        """
        return self._save(create=False, *args, **kwargs)

    def _save(self, create, *args, **kwargs):
        try:
            data = self._request_data()
            if not isinstance(data, dict):
                return ({'message': 'Invalid record type: {}'.format(data.__class__.__name__)}, 400)

            for k, v in kwargs.items():
                if data[k] != v:
                    return ({'message': 'Cannot change hash or range keys with PUT'}, 400)

            self._deserialize_dict(data, self.rest_model)

            if '/' in data[self.hash_keyname]:
                return ({'message': '\'{}\' may not contain forward slashes'.format(self.hash_keyname)}, 400)

            if self.range_keyname and '/' in data[self.range_keyname]:
                return ({'message': '\'{}\' may not contain forward slashes'.format(self.range_keyname)}, 400)

            try:
                keys = [data[self.hash_keyname], data[self.range_keyname]] if self.range_keyname else [data[self.hash_keyname]]
                attrs = [self.hash_keyname, self.range_keyname] if self.range_keyname else [self.hash_keyname]
                old_obj = self.pynamo_model.get(*keys, attributes_to_get=attrs)
            except self.pynamo_model.DoesNotExist:
                old_obj = None

            if create:
                if old_obj:
                    return ({'message': 'Record already exists'}, 409)
                else:
                    new_obj = self.pynamo_model(**data)
                    new_obj.save()
                    location = '{}/{}'.format(data[self.hash_keyname], data[self.range_keyname]) if self.range_keyname else data[self.hash_keyname]
                    return marshal(new_obj, self.rest_model), 201, {'Location': location}
            else:
                if old_obj:
                    new_obj = self.pynamo_model(**data)
                    new_obj.save()
                    return marshal(new_obj, self.rest_model)
                else:
                    return ({'message': 'Record not found'}, 404)
        except (AttributeError, PutError) as e:
            logger.exception('Invalid record')
            return ({'message': str(e)}, 400)
        except Exception as e:
            logger.exception('Failed to store record')
            return ({'message': str(e)}, 500)

    def _deserialize_dict(self, data, model):
        logger.info('Deserializing {} as {}'.format(data, model))
        for k, v in data.items():
            if k in model:
                if hasattr(model[k], 'parse'):
                    data[k] = model[k].parse(v)
                elif isinstance(model[k], fields.Nested):
                    self._deserialize_dict(v, model[k].model)
            else:
                raise AttributeError('Invalid key: {}'.format(k))
        logger.info('Mutated data to {}'.format(data))

    def _get_hash(self, kwargs):
        """
        Extract hash key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.hash_keyname)
        return value

    def _get_range(self, kwargs):
        """
        Extract range key value from request parameters and
        deserialize to correct PynamoDB attribute type.
        """
        value = kwargs.pop(self.range_keyname)
        return value

    def _get_filter(self, param, value):
        attr = getattr(self.pynamo_model, param, None)
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


def create_resource(model_or_index, name=None):
    """
    Create a resource class for a given PynamoDB model or index.
    """
    logger.debug('Creating resource for {}'.format(model_or_index))
    if issubclass(model_or_index, indexes.Index):
        name = name or model_or_index.Meta.index_name
        resource_class = IndexResource
    else:
        name = name or model_or_index.Meta.table_name
        resource_class = ModelResource

    cls = type('{0}Resource'.format(model_or_index.__name__), (resource_class,), {'pynamo_model': model_or_index, 'name': name})

    for name, attr in get_attributes(model_or_index).items():
        if attr.is_hash_key:
            cls.hash_keyname = name
        elif attr.is_range_key:
            cls.range_keyname = name

    return cls


def modelresource_factory(*args, **kwargs):
    """
    Legacy compatibility wrapper for create_resource()
    """
    return create_resource(*args, **kwargs)


def monkeypatch_swagger():
    import flask_restx.model
    import flask_restx.api
    import flask_restx.swagger
    # Swagger.register_model only registers fields for subclasses of Model,
    # but we need to get it to register fields for PynamoModel which
    # subclasses ModelBase directly. Hack that by replacing the Model reference
    # in the Swagger module with a tuple that also contains our class so that
    # isinstance(spec, Model) returns true for instances of our class.
    flask_restx.swagger.Model = (flask_restx.model.Model, PynamoModel)
    # The swagger generator replaces path parameter documentation specified
    # in the resource-level doc with implied types from the flask routes. This makes
    # it impossible to specify parameter info via route_doc. Hack around this
    # by turning path param extraction into a no-op.
    flask_restx.swagger.extract_path_params = lambda path: {}
    flask_restx.api.Swagger = flask_restx.swagger.Swagger


def get_attributes(model_or_index):
    """
    Legacy compatibility wrapper for Model.get_attributes() which was original a hidden method.
    """
    func = getattr(model_or_index, 'get_attributes', model_or_index._get_attributes)
    return func()


__all__ = ['ModelResource', 'IndexResource', 'create_resource', 'modelresource_factory']
monkeypatch_swagger()
