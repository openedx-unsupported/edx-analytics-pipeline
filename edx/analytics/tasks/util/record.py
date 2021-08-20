"""Tools for working with typed records."""

import datetime
import itertools
import logging
import re
from collections import OrderedDict

import ciso8601
import pytz

from edx.analytics.tasks.util.obfuscate_util import backslash_encode_value

DEFAULT_NULL_VALUE = '\\N'  # This is the default string used by Hive to represent a NULL value.

log = logging.getLogger(__name__)


class Record(object):
    """
    Represents a strongly typed record that can be stored in various storage engines and processed by Map Reduce jobs.

    The goal is to represent the schema in a way that allows us to generate the schemas for various systems (Hive,
    MySQL etc) as well as use that schema to serialize and deserialize the record in a variety of formats for Map
    Reduce purposes.

    Once the record is deserialized by the python code it can be used much like a namedtuple or other similar simple
    structure. It is intended to be immutable after initialization, however that can be bypassed relatively simply if
    someone wants to.

    We could have used a more complex ORM, but decided that they weren't worth the complexity, particularly given the
    fact that we would have to implement a bunch of customization and mapping logic. We are also concerned about
    performance since map functions often run over very large data sets, so small performance degredations can cause
    very significant job level performance degradation.

    When subclassing, the order of fields is very important!

    This class ->

        class MyRecord(Record):
            first_field = StringField()
            second_field = IntegerField()

    Will exhibit different behavior than this class ->

        class MyRecord(Record):
            second_field = IntegerField()
            first_field = StringField()

    Note that records are considered equal if they have the same field values. This is similar to the behavior of tuples
    and namedtuples.

    Instances of the following classes can be considered equal ->

        class A(Record):
            name = StringField()

        class B(Record):
            name = StringField()

        class C(A):
            pass

        class D(Record):
            name = StringField(nullable=False)
            SOME_CONSTANT = 'foobar'  # non-fields are ignored for comparison purposes!

    Instances of any of the following classes will never be equivalent to the ones listed above ->

        class E(Record):
            foo = StringField()

        class F(Record):
            name = IntegerField()

        class G(A):
            zipcode = StringField()
    """
    # For the base record class, we want the values of all fields to
    # be explicitly set.  An error is returned if any field is not
    # set.  However, we provide a flag here so that subclasses can
    # change this behavior.  In particular, we want to support a
    # record with sparse entries. In this case, fields not explicitly
    # set are given a value of None.
    set_missing_fields_to_none = False

    def __init__(self, *args, **kwargs):
        fields = self.get_fields()

        # First process all of the positional arguments, and map them to the fields in order of field declaration.

        # Create a one-off object that will represent a NULL value. Don't use None since that may be a real value for
        # a field.
        sentinel = object()
        # Args that aren't assigned to fields will be stored here and included in the exception later.
        extra_args = []
        # Fields that had no argument mapped to them
        remaining_fields = []
        # Use izip_longest instead of zip since it allows us to detect the case when more values have been provided than
        # there are fields in the object. This case should raise a TypeError, so we need to detect it here.
        for val, field_name in itertools.izip_longest(args, fields.keys(), fillvalue=sentinel):
            if val is sentinel:
                remaining_fields.append(field_name)
            elif field_name is sentinel:
                # We have exhausted the fields and there are unconsumed arguments, raise an error later.
                extra_args.append(val)
            else:
                self.initialize_field(field_name, val)
                if field_name in kwargs:
                    raise TypeError(
                        'Multiple values provided for the same field "{0}": {1} and {2}'.format(
                            field_name, repr(val), repr(kwargs[field_name])
                        )
                    )

        if len(extra_args) > 0:
            raise TypeError(
                'Too many positional arguments. Unused args: {0}'.format(', '.join(repr(a) for a in extra_args))
            )

        # Now iterate through any remaining fields and try to find them in the keyword arguments.
        missing_fields = []
        for field_name in remaining_fields:
            try:
                val = kwargs.pop(field_name)
                self.initialize_field(field_name, val)
            except KeyError:
                if self.set_missing_fields_to_none:
                    self.initialize_field(field_name, None)
                else:
                    missing_fields.append(field_name)

        if len(missing_fields) > 0:
            raise TypeError('Required fields not specified: {0}'.format(', '.join(missing_fields)))

        # Raise an error if we found any keyword arguments that weren't mapped to a field.
        if len(kwargs) > 0:
            raise TypeError('Unknown fields specified: {0}'.format(', '.join(kwargs.keys())))

        self._initialized = True

    def initialize_field(self, field_name, value):
        """
        Make sure the value is compatible with the field and assign that value to the field.

        Arguments:
            field_name (string): The name of the field that is being set.
            value (object): The value to assign to the field.

        """
        field_obj = self.get_fields()[field_name]
        validation_errors = field_obj.validate(value)
        if len(validation_errors) > 0:
            raise ValueError('Unable to assign the value {value} to the field named "{name}": {errors}'.format(
                value=repr(value),
                name=field_name,
                errors=', '.join(validation_errors)
            ))
        else:
            setattr(self, field_name, value)

    def __setattr__(self, key, value):
        if hasattr(self, '_initialized'):
            raise TypeError('Records are intended to be immutable')
        else:
            super(Record, self).__setattr__(key, value)

    def __delattr__(self, item):
        if hasattr(self, '_initialized'):
            raise TypeError('Records are intended to be immutable')
        else:
            super(Record, self).__delattr__(item)

    def __repr__(self):
        arg_strs = []
        for field_name in self.get_fields():
            val = getattr(self, field_name)
            arg_strs.append('{name}={value}'.format(name=field_name, value=repr(val)))

        return '{cls_name}({args})'.format(
            cls_name=self.__class__.__name__,
            args=', '.join(arg_strs)
        )

    def __eq__(self, other):
        if not isinstance(other, Record):
            # Note that we deliberately return NotImplemented here since it is semantically different from returning
            # False or raising an error. Instead it tells python that we cannot perform the comparison. Python will
            # often flip the order of the equality, calling b.__eq__(a) after a.__eq__(b) if the first returns
            # NotImplemented.
            return NotImplemented
        else:
            return self.to_ordered_dict() == other.to_ordered_dict()

    def __ne__(self, other):
        result = self.__eq__(other)
        # See discussion of NotImplemented above
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self):
        return hash(tuple(self.to_ordered_dict().items()))

    @classmethod
    def get_fields(cls):
        """
        Get all of the fields in this record in order of declaration.

        Returns: An OrderedDict mapping field names to the field objects in the order they are declared in the record.
        """
        # We don't want to read the fields from the parent class, so we use a variable that is private to this
        # class. This format ensures that the scope is constrained to only cls, not its parents or children.
        class_private_var_name = '_{0}__fields'.format(cls.__name__)
        field_dict = getattr(cls, class_private_var_name, None)
        if field_dict is None:
            fields = []
            for field_name in dir(cls):
                field_obj = getattr(cls, field_name)
                if not isinstance(field_obj, Field):
                    continue

                fields.append((field_name, field_obj))

                # Field ordering matters! Note that parent classes must be processed before any subclasses, so their
                # fields will appear first in the list. Also note that the ordering is very difficult to predict in
                # cases of complex multiple-inheritance. The fields will appear in the order they are declared in the
                # source file, which may be rather unintuitive.
                fields.sort(key=lambda t: t[1].counter)

            field_dict = OrderedDict(fields)
            setattr(cls, class_private_var_name, field_dict)

        return field_dict

    def replace(self, **kwargs):
        """
        Returns: a new Record with identical values except for those specified in the kwargs, which override any
            existing values for those fields.
        """
        new_attribute_values = self.to_ordered_dict()
        new_attribute_values.update(kwargs)
        return self.__class__(**new_attribute_values)

    def to_string_tuple(self, string_encoder=None):
        """
        Convert the record into a tuple of UTF-8 encoded byte strings.

        This format is convenient for use with Luigi since it expects tuples of strings as output from reduce functions.

        Arguments:
            string_encoder : The string encoder to encode the record fields with.

        """
        if string_encoder is None:
            string_encoder = HiveTsvEncoder()

        field_values = []
        for field_name, field_obj in self.get_fields().items():
            val = getattr(self, field_name)
            if val is not None:
                val = field_obj.serialize_to_string(val)

            field_values.append(string_encoder.encode(val, field_obj))

        return tuple(field_values)

    def to_ordered_dict(self):
        """
        Convert this record into a simple python dictionary.

        Returns: a dictionary mapping field names to their respective values.
        """
        field_values = OrderedDict()
        for field_name in self.get_fields():
            val = getattr(self, field_name)
            field_values[field_name] = val

        return field_values

    def to_separated_values(self, sep=u'\t', string_encoder=None):
        """
        Convert this record to a string with fields delimited by `sep`.

        Arguments:
            sep (unicode): The unicode string to inject between fields in the record. It will be encoded in UTF-8.
            string_encoder: The string encoder to encode the record fields with.

        Returns: a UTF8 string representation of the record.
        """
        utf8sep = sep.encode('utf-8')
        return utf8sep.join(self.to_string_tuple(string_encoder=string_encoder))

    @classmethod
    def from_string_tuple(cls, string_tuple, string_decoder=None):
        """
        Construct a record from an iterable of strings.

        The number of strings in the iterable must match the number of fields in the Record. Each string will be
        interpreted by the field and coerced into the appropriate type.

        Arguments:
            string_tuple (iterable): The values for the fields as strings.
            string_decoder : The string encoder to decode the strings with.

        """
        if string_decoder is None:
            string_decoder = HiveTsvEncoder()

        fields = cls.get_fields()
        if len(string_tuple) != len(fields):
            raise ValueError('The length of the tuple of strings must exactly match the number of fields in the Record')
        typed_field_values = []
        for str_value, field_name in zip(string_tuple, fields):
            field_obj = fields[field_name]
            value = string_decoder.decode(str_value, field_obj)
            if value is not None:
                value = field_obj.deserialize_from_string(value)

            typed_field_values.append(value)

        return cls(*typed_field_values)

    @classmethod
    def from_tsv(cls, tsv_str):
        """
        Construct a record from a tab-separated string.

        Arguments:
            tsv_str (string): The TSV formatted string that represents the record.
        """
        return cls.from_string_tuple(tuple(tsv_str.rstrip('\r\n').split('\t')))

    @classmethod
    def get_sql_schema(cls):
        """
        A skeleton schema of the SQL table that could store this data.

        Returns: A list of tuples whose first element is the column name, and the second element is the type to assign
            to the column. Note that this returns SQL92 compliant types including some modifiers (NOT NULL) etc. It
            does not include any index definitions, constraints or relationship declarations.
        """
        schema = []
        for field_name, field_obj in cls.get_fields().items():
            schema.append((field_name, field_obj.sql_type))
        return schema

    @classmethod
    def get_hive_schema(cls):
        """
        A skeleton schema of the Hive table that could store this data.

        Returns: A list of tuples whose first element is the column name, and the second element is the type to assign
            to the column. Note that Hive data types often are quite different than other SQL databases.
        """
        schema = []
        for field_name, field_obj in cls.get_fields().items():
            schema.append((field_name, field_obj.hive_type))
        return schema

    @classmethod
    def get_elasticsearch_properties(cls):
        """
        An elasticsearch mapping that could store this data.

        Fields with `elasticsearch_type` as `keyword` are not analyzed.

        Returns: A dictionary of property definitions.
        """
        properties = {}
        for field_name, field_obj in cls.get_fields().items():
            properties[field_name] = {
                'type': field_obj.elasticsearch_type
            }

            elasticsearch_format = getattr(field_obj, 'elasticsearch_format', None)
            if elasticsearch_format:
                properties[field_name]['format'] = elasticsearch_format

        return properties

    @classmethod
    def get_restructured_text(cls, indent='    '):
        """
        Generates a string that can be injected into docstrings to document the record schema.

        This schema type recognizes the "description" kwarg that can be passed into the Field definition.

            foo = StringField(description='this will appear in the docs')

        Arguments:
            indent (str): This string will be prepended in front of each field.

        Returns: A reStructuredText formatted string describing the fields in this record.
        """
        field_doc = ['\n']
        for field_name, field_obj in cls.get_fields().items():
            field_doc.append(
                '{indent}{name} : {type}\n  {indent}{desc}'.format(
                    indent=indent,
                    name=field_name,
                    type=field_obj.__class__.__name__,
                    desc=getattr(field_obj, 'description', 'Unspecified')
                )
            )
        field_doc.append('')
        return '\n'.join(field_doc)


class SparseRecord(Record):
    """
    Represents a Record that can be initialized with a subset of values being defined.

    Fields in the record that are not explicitly specified will default to None.
    """
    # For the base record class, we wanted the values of all fields to
    # be explicitly set, and an error to be returned if any field is
    # not set.  We set this flag here to support records with sparse
    # entries.  In this case, fields not explicitly set are given a
    # value of None.
    set_missing_fields_to_none = True


class HiveTsvEncoder(object):

    def __init__(self, normalize_whitespace=False, **kwargs):
        self.null_value = kwargs.get('null_value', DEFAULT_NULL_VALUE)
        self.normalize_whitespace = normalize_whitespace

    def encode(self, decoded_string, field_obj):
        if decoded_string is None:
            return self.null_value
        else:
            if self.normalize_whitespace or getattr(field_obj, 'normalize_whitespace', False):
                decoded_string = re.sub(r'\s+', ' ', decoded_string)

        return decoded_string.encode('utf8')

    def decode(self, encoded_string, _field_obj):
        if encoded_string == self.null_value:
            return None
        else:
            return encoded_string.decode('utf8')


class Field(object):
    """
    Represents a field within a record.

    The field is an abstract representation of a type. It can be used to generate schemas for various data manipulation
    systems as well as interpret data. It is intended to provide structure so that downstream code does not have to
    handle edge cases related to dynamic typing. It enforces the type and ensures that the data conforms to the
    declared schema.
    """
    counter = 0

    def __init__(self, **kwargs):
        self.nullable = kwargs.pop('nullable', True)

        for key, value in kwargs.items():
            setattr(self, key, value)

        # This counter lets us "see" the order in which the class member variables appear in the class they are declared
        # in. Sorting by this counter will allow us to order them appropriately. Note that this isn't atomic and has
        # all kinds of issues, but is functional and doesn't require parsing the AST or anything *more* hacky.
        self.counter = Field.counter
        Field.counter += 1

        self.validate_parameters()

    def validate_parameters(self):
        """Once all kwargs have been assigned to attributes, validate them and set any defaults."""
        pass

    def validate(self, value):
        """
        Determine if this value is an acceptable value for this field.

        The goal of this method is to do some trivial checks to detect problems with the data as early as possible and
        raise an error. This will prevent us from attempting to insert data into the database that will definitely cause
        errors on insertion. It also allows downstream code to make assumptions about the data (e.g. that it isn't
        None) since the Field will enforce those constraints.

        Arguments:
            value (object):

        Returns: A list of validation error strings. If the list is empty, the value is acceptable.
        """
        validation_errors = []
        if value is None and not self.nullable:
            validation_errors.append('The field cannot accept null values')
        return validation_errors

    def serialize_to_string(self, value):
        """Returns a unicode string representation of a value for this field."""
        return unicode(value)

    def deserialize_from_string(self, string_value):
        """Returns a typed representation of the value from its string representation."""
        return string_value

    @property
    def sql_type(self):
        """Returns a SQL-92 compliant declaration that can be used to generate a table that includes this field."""
        base_type = self.sql_base_type
        if not self.nullable:
            base_type += ' NOT NULL'
        return base_type

    @property
    def sql_base_type(self):
        """Returns the core SQL-92 data type without any modifiers (such as NOT NULL)."""
        raise NotImplementedError

    @property
    def hive_type(self):
        """Returns the HiveQL data type for this type of field."""
        raise NotImplementedError

    @property
    def elasticsearch_type(self):
        """Returns the elasticsearch type for this type of field."""
        raise NotImplementedError


class StringField(Field):  # pylint: disable=abstract-method
    """Represents a field that contains a relatively short string."""

    hive_type = 'STRING'
    elasticsearch_type = 'keyword'

    def validate_parameters(self):
        if not hasattr(self, 'length'):
            self.length = None
        if self.length is not None and self.length == 0:
            raise ValueError('Length must be greater than 0')

        if not hasattr(self, 'truncate'):
            self.truncate = False

    def validate(self, value):
        validation_errors = super(StringField, self).validate(value)
        if value is not None:
            if not isinstance(value, basestring):
                validation_errors.append('The value is not a string')
            elif self.length and not self.truncate and len(value) > self.length:
                validation_errors.append('The string length exceeds the maximum allowed')
        return validation_errors

    def serialize_to_string(self, value):
        """Returns a unicode string representation of a value for this field, truncating if required."""
        if self.truncate and len(value) > self.length:
            value = value[:self.length]

        try:
            return unicode(value, encoding=getattr(self, 'encoding', 'utf8'))
        except TypeError:
            # It's already a unicode string
            return value

    @property
    def sql_base_type(self):
        if self.length:
            return 'VARCHAR({length})'.format(length=self.length)
        else:
            return 'VARCHAR'


class DelimitedStringField(Field):
    """Represents a list of strings, stored as a single delimited string."""

    hive_type = 'STRING'
    sql_base_type = 'VARCHAR'
    elasticsearch_type = 'text'
    delimiter = '\0'

    def serialize_to_string(self, value):
        """Flatten array values to a delimited string."""
        return self.delimiter.join(value)

    def deserialize_from_string(self, string_value):
        """Unpack delimited strings into an array."""
        if string_value is None:
            return None
        return tuple(string_value.split(self.delimiter))

    def validate(self, value):
        """Accepts tuple values."""
        validation_errors = super(DelimitedStringField, self).validate(value)
        if not(value is None or isinstance(value, tuple)):
            validation_errors.append('The value is not a tuple')
        return validation_errors


class BooleanField(Field):
    """Represents a field that contains a boolean."""

    hive_type = 'TINYINT'
    sql_base_type = 'BOOLEAN'
    elasticsearch_type = 'boolean'

    def serialize_to_string(self, value):
        """Returns '1' for true values, '0' for false."""
        return '1' if value else '0'

    def deserialize_from_string(self, string_value):
        """Return a bool value from the given string."""
        if string_value is None:
            return None
        elif string_value == '1':
            return True
        return False

    def validate(self, value):
        """Accepts boolean values."""
        validation_errors = super(BooleanField, self).validate(value)
        if not(value is None or isinstance(value, bool)):
            validation_errors.append('The value is not a bool')
        return validation_errors


class IntegerField(Field):  # pylint: disable=abstract-method
    """Represents a field that contains an integer."""

    hive_type = sql_base_type = 'INT'
    elasticsearch_type = 'integer'

    def validate(self, value):
        validation_errors = super(IntegerField, self).validate(value)
        if value is not None and not isinstance(value, int):
            validation_errors.append('The value is not an integer')
        return validation_errors

    def deserialize_from_string(self, string_value):
        return int(string_value)


class DateField(Field):  # pylint: disable=abstract-method
    """Represents a field that contains a date."""

    hive_type = 'STRING'
    sql_base_type = 'DATE'
    elasticsearch_type = 'date'

    def validate(self, value):
        validation_errors = super(DateField, self).validate(value)
        if value is not None and not isinstance(value, datetime.date):
            validation_errors.append('The value is not a date')
        return validation_errors

    def deserialize_from_string(self, string_value):
        return datetime.date(*[int(x) for x in string_value.split('-')])


class DateTimeField(Field):  # pylint: disable=abstract-method
    """Represents a field that contains a date and time."""

    hive_type = 'TIMESTAMP'
    sql_base_type = 'DATETIME'
    elasticsearch_type = 'date'
    elasticsearch_format = 'yyyy-MM-dd HH:mm:ss.SSSSSS'
    string_format = '%Y-%m-%d %H:%M:%S.%f'  # hive timestamp format

    class TzUtc(datetime.tzinfo):
        """
        Tzinfo subclass which represents UTC.

        Borrowed from dateutil.tz, to avoid having to include dateutil in our map/reduce requirements.
        """
        ZERO = datetime.timedelta(0)

        def utcoffset(self, dt):  # pylint: disable=unused-argument
            return self.ZERO

        def dst(self, dt):  # pylint: disable=unused-argument
            return self.ZERO

        def tzname(self, dt):  # pylint: disable=unused-argument
            return "UTC"

    utc_tz = TzUtc()

    def validate(self, value):
        validation_errors = super(DateTimeField, self).validate(value)
        if value is None:
            pass
        elif not isinstance(value, datetime.datetime):
            validation_errors.append('The value is not a datetime')
        elif value.utcoffset() is None:
            validation_errors.append('The value is a naive datetime.')
        elif value.utcoffset().total_seconds() != 0:
            validation_errors.append('The value must use UTC timezone.')
        elif value.year < 1900:
            # https://docs.python.org/2/library/datetime.html?highlight=strftime#strftime-strptime-behavior
            # "The exact range of years for which strftime() works also varies across platforms. Regardless of platform, years before 1900 cannot be used."
            validation_errors.append('The value must be a date after 1900.')

        return validation_errors

    def serialize_to_string(self, value):
        """Returns a string representation of the datetime value."""
        return value.strftime(self.string_format)

    def deserialize_from_string(self, string_value):
        """Returns a datetime instance parsed from the numbers in the given string_value."""
        if string_value is None:
            return None
        # Note: we need to be flexible here, because the datetime format differs between input sources
        # (e.g.  tracking logs, REST API)
        # However, we assume the datetime does not include TZ info, and that it's UTC.
        return datetime.datetime(*[int(x) for x in re.split(r'\D+', string_value) if x], tzinfo=self.utc_tz)


class FloatField(Field):  # pylint: disable=abstract-method
    """Represents a field that contains a floating point number."""

    hive_type = sql_base_type = 'FLOAT'
    elasticsearch_type = 'float'

    def validate(self, value):
        validation_errors = super(FloatField, self).validate(value)
        if value is not None:
            try:
                float(value)
            except (ValueError, TypeError):
                validation_errors.append('The value is not a floating point number')
        return validation_errors

    def deserialize_from_string(self, string_value):
        return float(string_value)


class RecordMapper(object):
    """
    Load a record from a dictionary object, according to a given mapping.

    To implement, derive a class that extends RecordMapper, and declares the class name of the record
    it is designed to create data for.   The implementation should also define add_record_field_mapping,
    which is used to define how each field in a record should be populated from the input dictionary.

    Once the derived mapper class is instantiated, it can be used to make calls to add_info(),
    for some input dictionary and output dictionary.  Additional calls to add_calculated_entry can
    add hardcoded (unmapped) field entries to the output dictionary, but with the benefit of conversion and validation.
    The output dictionary can then be passed to self.record_class(**record_dict) to get the resulting
    record loaded.
    """

    record_mapping = None
    date_time_field_for_validating = DateTimeField()

    @property
    def record_class(self):
        """Specifies the class of the Record being loaded by this mapper."""
        raise NotImplementedError

    def add_record_field_mapping(self, field_key, add_event_mapping_entry):
        """
        For a given field name, add the location in the source dictionary from which to fetch a value.

        Key values for the mapping are represented using dot notation, beginning with the hard-coded string value 'root'.
        So for a dictionary with { 'a': {'b': 'b-value' } }, fetching the 'b-value' into a record field would be
        defined as 'root.a.b'.
        """
        raise NotImplementedError

    def _add_entry(self, record_dict, record_key, record_field, label, obj):
        """
        Add the `obj` to the `record_key` entry of `record_dict`, performing appropriate conversion based on `record_field`.

        For strings, the entry is truncated, if necessary, so we should rarely see truncation errors.  Also, null characters
        are escaped so that they won't fail when being loaded into BigQuery.

        For timestamps, parsing is done using ciso8601.

        Errors are logged, but are not fatal.  In such cases, the value is simply not set.  (It's only fatal, then, if the
        value was required.)
        """
        if isinstance(record_field, StringField):
            if obj is None:
                # TODO: this should really check to see if the record_field is nullable.
                value = None
            else:
                value = backslash_encode_value(unicode(obj))
                if '\x00' in value:
                    value = value.replace('\x00', '\\0')
                # Avoid validation errors later due to length by truncating here.
                field_length = record_field.length
                value_length = len(value)
                # TODO: This implies that field_length is at least 4.
                if value_length > field_length:
                    log.error("Record value length (%d) exceeds max length (%d) for field %s: %r", value_length, field_length, record_key, value)
                    value = u"{}...".format(value[:field_length - 4])
            record_dict[record_key] = value
        elif isinstance(record_field, IntegerField):
            try:
                record_dict[record_key] = int(obj)
            except ValueError:
                log.error('Unable to cast value to int for %s: %r', label, obj)
        elif isinstance(record_field, BooleanField):
            try:
                record_dict[record_key] = bool(obj)
            except ValueError:
                log.error('Unable to cast value to bool for %s: %r', label, obj)
        elif isinstance(record_field, FloatField):
            try:
                record_dict[record_key] = float(obj)
            except ValueError:
                log.error('Unable to cast value to float for %s: %r', label, obj)
        elif isinstance(record_field, DateTimeField):
            datetime_obj = None
            try:
                if obj is not None:
                    datetime_obj = ciso8601.parse_datetime(obj)
                    if datetime_obj.tzinfo:
                        datetime_obj = datetime_obj.astimezone(pytz.utc)
                else:
                    datetime_obj = obj
            except ValueError:
                log.error('Unable to cast value to datetime for %s: %r', label, obj)

            # Because it's not enough just to create a datetime object, also perform
            # validation here.
            if datetime_obj is not None:
                validation_errors = self.date_time_field_for_validating.validate(datetime_obj)
                if len(validation_errors) > 0:
                    log.error('Invalid assigment of value %r to field "%s": %s', datetime_obj, label, ', '.join(validation_errors))
                    datetime_obj = None

            record_dict[record_key] = datetime_obj
        else:
            record_dict[record_key] = obj

    def _add_info_recurse(self, record_dict, record_mapping, obj, label):
        """Recurse through the structure of the input `obj`, and add it if it's a matching leaf."""
        if obj is None:
            pass
        elif isinstance(obj, dict):
            for key in obj.keys():
                new_value = obj.get(key)
                # Normalize labels to be all lower-case, since all field (column) names are lowercased.
                new_label = u"{}.{}".format(label, key.lower())
                self._add_info_recurse(record_dict, record_mapping, new_value, new_label)
        elif isinstance(obj, list):
            # We will not output any values that are stored in lists.
            pass
        else:
            # We assume it's a single object, and look it up now.
            if label in record_mapping:
                record_key, record_field = record_mapping[label]
                self._add_entry(record_dict, record_key, record_field, label, obj)

    def add_info(self, record_dict, input_dict):
        """Populate the record_dict by applying the mapping to the input_dict."""
        self._add_info_recurse(record_dict, self._get_record_mapping(), input_dict, 'root')

    def add_calculated_entry(self, record_dict, record_key, obj):
        """
        Use this to explicitly add calculated entry values.

        Value loaded this way should be excluded from the mapping, so that add_info does not also set them.
        """
        record_field = self.record_class.get_fields()[record_key]
        label = record_key
        self._add_entry(record_dict, record_key, record_field, label, obj)

    def _calculate_record_mapping(self):
        """
        Return dictionary that maps a location in a dictionary to the record field it should be inserted into.

        Key values for the mapping are represented using dot notation, beginning with the hard-coded string value 'root'.
        So for a dictionary with { 'a': {'b': 'b-value' } }, fetching the 'b-value' into a record field would be
        defined as 'root.a.b'.

        Values for the mapping provide information about the field in the record to be written to.   This is a tuple
        of the field name and the actual field object.
        """
        record_mapping = {}
        fields = self.record_class.get_fields()
        field_keys = fields.keys()
        for field_key in field_keys:
            field_tuple = (field_key, fields[field_key])

            def add_event_mapping_entry(source_key):
                record_mapping[source_key] = field_tuple

            # Call method defined in derived class here.
            self.add_record_field_mapping(field_key, add_event_mapping_entry)

        return record_mapping

    def _get_record_mapping(self):
        """Return dictionary of input_dict attributes to the output keys they map to."""
        if self.record_mapping is None:
            self.record_mapping = self._calculate_record_mapping()
        return self.record_mapping
