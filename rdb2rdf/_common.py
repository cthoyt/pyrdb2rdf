# -*- coding: utf-8 -*-
"""RDB2RDF common components"""

from binascii import hexlify as _bytes2hexstr, unhexlify as _hexstr2bytes
from datetime import timedelta as _timedelta
from urllib.parse import quote as _pct_encoded
from rdflib import XSD

import rdflib as _rdf
import sqlalchemy as sqla
import re

from sqlalchemy import dialects
from sqlalchemy import exc
from sqlalchemy.sql import sqltypes, type_api

__copyright__ = "Copyright (C) 2014 Ivan D Vasin"
__docformat__ = "restructuredtext"


def iri_safe(string):
    return _pct_encoded(str(string))


def _rdf_duration_from_timedelta(td):
    if td.days == td.seconds == 0:
        return _rdf.Literal('PT0S', datatype=XSD.dayTimeDuration)
    else:
        sign = 1 if td.days >= 0 else -1
        years, days_rem = divmod(td.days, sign * 365)
        months, days_rem = divmod(days_rem, sign * 30)
        days = sign * days_rem
        hours, seconds_rem = divmod(td.seconds, sign * 3600)
        minutes, seconds_rem = divmod(seconds_rem, sign * 60)
        seconds = sign * seconds_rem

        if years or months:
            if days or hours or minutes or seconds:
                datatype = XSD.duration
            else:
                datatype = XSD.yearMonthDuration
        else:
            datatype = XSD.dayTimeDuration
        if years or months or days:
            if hours or minutes or seconds:
                time_desig = 'T'
            else:
                time_desig = ''
        else:
            time_desig = 'T'

        return _rdf.Literal('{}P{}{}{}{}{}{}{}'
                            .format('' if sign == 1 else '-',
                                    '{}Y'.format(years) if years else '',
                                    '{}M'.format(months) if months else '',
                                    '{}D'.format(days) if days else '',
                                    time_desig,
                                    '{}H'.format(hours) if hours else '',
                                    '{}M'.format(minutes) if minutes else '',
                                    '{}S'.format(seconds) if seconds else ''),
                            datatype=datatype)


ISO8601_DURATION_RE = re.compile(r'(?P<sign_neg>-)?'
                                 r'(?:(?P<years>\d+)Y)?'
                                 r'(?:(?P<months>\d+)M)?'
                                 r'(?:(?P<days>\d+)D)?'
                                 r'(?:T(?:(?P<hours>\d+)H)?'
                                 r'(?:(?P<minutes>\d+)M)?'
                                 r'(?:(?P<seconds>\d+)'
                                 r'(?P<frac_seconds>\.\d+)?S)?)?$')


def _timedelta_from_rdf_duration(literal):
    match = ISO8601_DURATION_RE.match(literal)

    if not match:
        raise ValueError('invalid RDF interval literal {!r}: expecting a'
                         ' literal that matches the format {!r}'
                         .format(literal, ISO8601_DURATION_RE.pattern))

    years = int(match.group('years') or 0)
    months = int(match.group('months') or 0)
    days = int(match.group('days') or 0)
    hours = int(match.group('hours') or 0)
    minutes = int(match.group('minutes') or 0)
    seconds = int(match.group('seconds') or 0)
    microseconds = int(match.group('frac_seconds') or 0) * 1000000

    return _timedelta(years, months, days, hours, minutes, seconds, microseconds)

_CANON_RDF_DATATYPE_BY_SQL_TYPE = {
    sqla.sql.sqltypes.Binary: XSD.hexBinary,
    sqla.Boolean: XSD.boolean,
    sqla.Date: XSD.date,
    sqla.DateTime: XSD.dateTime,
    sqla.Float: XSD.double,
    sqla.Integer: XSD.integer,
    sqla.Interval: XSD.duration,
    sqla.Numeric: XSD.decimal,
    sqla.String: XSD.string,
    sqla.Time: XSD.time,
    sqla.sql.sqltypes.TypeEngine: None
}

_RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE = {
    sqla.sql.sqltypes.Binary: lambda literal: _rdf.Literal(_bytes2hexstr(literal), datatype=XSD.hexBinary),
    sqla.Boolean: lambda literal: _rdf.Literal(literal),
    sqla.Date: lambda literal: _rdf.Literal(literal),
    sqla.DateTime: lambda literal: _rdf.Literal(literal),
    sqla.Float: lambda literal: _rdf.Literal(literal),
    sqla.Integer: lambda literal: _rdf.Literal(literal),
    sqla.Interval: _rdf_duration_from_timedelta,
    sqla.Numeric: lambda literal: _rdf.Literal(literal),
    sqla.String: lambda literal: _rdf.Literal(literal),
    sqla.Time: lambda literal: _rdf.Literal(literal),
    sqla.sql.sqltypes.TypeEngine: lambda literal: _rdf.Literal(str(literal, 'utf-8'))
}

_SQL_LITERAL_FROM_RDF_FUNC_BY_RDF_DATATYPE = {
    XSD.binary: lambda literal: _hexstr2bytes(literal),
    XSD.boolean: lambda literal: literal.toPython(),
    XSD.date: lambda literal: literal.toPython(),
    XSD.dateTime: lambda literal: literal.toPython(),
    XSD.decimal: lambda literal: literal.toPython(),
    XSD.double: lambda literal: literal.toPython(),
    XSD.duration: _timedelta_from_rdf_duration,
    XSD.integer: lambda literal: literal.toPython(),
    XSD.string: lambda literal: literal.toPython(),
    XSD.time: lambda literal: literal.toPython(),
}

_SQL_LITERAL_TYPES_BY_RDF_DATATYPE = {
    None: [sqla.String],
    XSD.boolean: [sqla.Boolean],
    XSD.date: [sqla.Date],
    XSD.dateTime: [sqla.DateTime],
    XSD.dayTimeDuration: [sqla.Interval],
    XSD.decimal: [sqla.Numeric],
    XSD.duration: [sqla.Interval],
    XSD.hexBinary: [sqla.sql.sqltypes.Binary],
    XSD.integer: [sqla.Integer],
    XSD.string: [sqla.String],
    XSD.time: [sqla.Time],
    XSD.yearMonthDuration: [sqla.Interval]
}


_RDF_DATATYPES_BY_SQL_TYPE = {}
for _rdf_datatype, _sql_types in _SQL_LITERAL_TYPES_BY_RDF_DATATYPE.items():
    for _sql_type in _sql_types:
        try:
            rdf_datatypes = _RDF_DATATYPES_BY_SQL_TYPE[_sql_type]
        except KeyError:
            rdf_datatypes = []
            _RDF_DATATYPES_BY_SQL_TYPE[_sql_type] = rdf_datatypes
        rdf_datatypes.append(_rdf_datatype)

_RDF_DATATYPES_BY_SQL_TYPE[sqla.sql.type_api.TypeEngine] = [None]


def canon_rdf_datatype_from_sql(sql_type):
    if not isinstance(sql_type, type):
        return _canon_rdf_datatype_from_sql(sql_type.__class__)
    return _canon_rdf_datatype_from_sql(sql_type)


def _rdf_datatypes_from_sql(sql_type):
    try:
        return _RDF_DATATYPES_BY_SQL_TYPE[sql_type]
    except KeyError:
        datatype = _rdf_datatypes_from_sql(sql_type.__mro__[1])
        _RDF_DATATYPES_BY_SQL_TYPE[sql_type] = datatype
        return datatype


def rdf_datatypes_from_sql(sql_type):
    if not isinstance(sql_type, type):
        return tuple(_rdf_datatypes_from_sql(sql_type.__class__))
    return tuple(_rdf_datatypes_from_sql(sql_type))


def rdf_literal_from_sql(literal, sql_type):
    if not isinstance(sql_type, type):
        return _rdf_literal_from_sql_func(sql_type.__class__)(literal)
    return _rdf_literal_from_sql_func(sql_type)(literal)


def sql_literal_from_rdf(literal):
    try:
        sql_literal_from_rdf_ = \
            _SQL_LITERAL_FROM_RDF_FUNC_BY_RDF_DATATYPE[literal.datatype]
    except KeyError:
        literal_py = literal.toPython()
        if isinstance(literal_py, _rdf.Literal):
            return str(literal, 'utf-8')
        else:
            return literal_py
    else:
        return sql_literal_from_rdf_(literal)


def sql_literal_types_from_rdf(datatype):
    return tuple(_SQL_LITERAL_TYPES_BY_RDF_DATATYPE.get(datatype, (sqla.String,)))


def inspect_rdf(orm_entity):
    try:
        return orm_entity.__rdf_mapper__
    except AttributeError:
        return None


def _canon_rdf_datatype_from_sql(sql_type):
    try:
        return _CANON_RDF_DATATYPE_BY_SQL_TYPE[sql_type]
    except KeyError:
        datatype = _canon_rdf_datatype_from_sql(sql_type.__mro__[1])
        _CANON_RDF_DATATYPE_BY_SQL_TYPE[sql_type] = datatype
        return datatype


def _rdf_literal_from_sql_func(sql_type):
    try:
        return _RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE[sql_type]
    except KeyError:
        rdf_literal_from_sql_val = _rdf_literal_from_sql_func(sql_type.__mro__[1])
        _RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE[sql_type] = rdf_literal_from_sql_val
        return rdf_literal_from_sql_val

try:
    sqla.dialects.registry.load('oracle')
except sqla.exc.NoSuchModuleError:
    pass
else:
    _RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE[sqla.dialects.oracle.INTERVAL] = _rdf_duration_from_timedelta

try:
    sqla.dialects.registry.load('postgresql')
except sqla.exc.NoSuchModuleError:
    pass
else:
    _RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE[sqla.dialects.postgresql.INTERVAL] = _rdf_duration_from_timedelta

try:
    sqla.dialects.registry.load('oracle')
except sqla.exc.NoSuchModuleError:
    pass
else:
    for rdf_datatype in (XSD.dayTimeDuration, XSD.duration, XSD.yearMonthDuration):
        _SQL_LITERAL_TYPES_BY_RDF_DATATYPE[rdf_datatype].append(sqla.dialects.oracle.INTERVAL)

try:
    sqla.dialects.registry.load('postgresql')
except sqla.exc.NoSuchModuleError:
    pass
else:
    for rdf_datatype in (XSD.dayTimeDuration, XSD.duration, XSD.yearMonthDuration):
        _SQL_LITERAL_TYPES_BY_RDF_DATATYPE[rdf_datatype].append(sqla.dialects.postgresql.INTERVAL)

try:
    sqla.dialects.registry.load('oracle')
except sqla.exc.NoSuchModuleError:
    pass
else:
    _SQL_LITERAL_FROM_RDF_FUNC_BY_RDF_DATATYPE[sqla.dialects.oracle.INTERVAL] = _timedelta_from_rdf_duration

try:
    sqla.dialects.registry.load('postgresql')
except sqla.exc.NoSuchModuleError:
    pass
else:
    _SQL_LITERAL_FROM_RDF_FUNC_BY_RDF_DATATYPE[sqla.dialects.postgresql.INTERVAL] = _timedelta_from_rdf_duration
