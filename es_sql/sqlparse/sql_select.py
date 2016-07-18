import datetime

from es_sql import sqlparse  # TODO: should not reference parent
from . import datetime_evaluator
from . import sql as stypes
from . import tokens as ttypes
from .ordereddict import OrderedDict


# make the result of sqlparse more usable
class SqlSelect(object):
    def __init__(self, tokens, joinable_results, joinable_queries):
        self.from_table = None
        self.from_indices = None
        self.projections = {}
        self.projection_mapping = {}
        self.group_by = OrderedDict()
        self.order_by = []
        self.limit = None
        self.having = []
        self.where = None
        self.join_table = None
        self.join_conditions = []

        self.buckets_names = {}
        self.joinable_results = joinable_results or {}
        self.joinable_queries = joinable_queries or {}

        self.is_select_inside = False
        self.on_SELECT(tokens)
        if isinstance(self.from_table, basestring):
            if self.group_by or self.has_function_projection():
                self.is_select_inside = True

    def generate_url(self, es_url):
        if self.join_table in self.joinable_queries:
            return '%s/%s/_coordinate_search' % (es_url, self.from_indices)
        else:
            return '%s/%s/_search' % (es_url, self.from_indices)

    @classmethod
    def parse(cls, sql_select, joinable_results=None, joinable_queries=None):
        statement = sqlparse.parse(sql_select)[0]
        sql_select = SqlSelect(statement.tokens, joinable_results, joinable_queries)
        return sql_select

    def tables(self):
        map = {}
        map[self.from_table] = True
        for t in self.joinable_results.keys():
            map[t] = False
        for t in self.joinable_queries.keys():
            map[t] = False
        return map

    def on_SELECT(self, tokens):
        if not (ttypes.DML == tokens[0].ttype and 'SELECT' == tokens[0].value.upper()):
            raise Exception('it is not SELECT: %s' % tokens)
        idx = 1
        from_table_found = False
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if ttypes.Keyword == token.ttype:
                if token.value.upper() in ('FROM', 'INSIDE'):
                    self.is_select_inside = 'INSIDE' == token.value.upper()
                    from_table_found = True
                    idx = self.on_FROM(tokens, idx)
                    continue
                elif 'GROUP' == token.value.upper():
                    idx = self.on_GROUP(tokens, idx)
                    continue
                elif 'ORDER' == token.value.upper():
                    idx = self.on_ORDER(tokens, idx)
                    continue
                elif 'LIMIT' == token.value.upper():
                    idx = self.on_LIMIT(tokens, idx)
                    continue
                elif 'HAVING' == token.value.upper():
                    idx = self.on_HAVING(tokens, idx)
                    continue
                elif 'JOIN' == token.value.upper():
                    idx = self.on_JOIN(tokens, idx)
                    continue
                else:
                    raise Exception('unexpected: %s' % token)
            elif isinstance(token, stypes.Where):
                self.on_WHERE(token)
            elif not from_table_found:
                self.set_projections(token)
                continue
            else:
                raise Exception('unexpected: %s' % repr(token))

    def set_projections(self, token):
        if token.ttype == ttypes.Punctuation:
            raise Exception('a.b should use the form "a.b", unexpected: %s' % token)
        if self.projections:
            raise Exception('projections has already been set, unexpected: %s' % token)
        if isinstance(token, stypes.IdentifierList):
            ids = list(token.get_identifiers())
        else:
            ids = [token]
        self.projections = {}
        for id in ids:
            if isinstance(id, stypes.Identifier):
                self.projections[id.get_name()] = id.without_as()
            elif id.is_field():
                self.projections[id.as_field_name()] = id
            else:
                self.projections[str(id)] = id

    def on_FROM(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            alias = None
            if isinstance(token, stypes.Identifier):
                alias = token.get_name()
                token = token.tokens[0]
            if token.is_field():
                table = token.as_field_name()
                self.from_table = alias or table
            else:
                self.from_table = alias or str(token)
            self.from_indices = ','.join(translate_indices(token))
            break
        return idx

    def on_LIMIT(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            self.limit = int(token.value)
            break
        return idx

    def on_HAVING(self, tokens, idx):
        self.having = []
        while idx < len(tokens):
            token = tokens[idx]
            if ttypes.Keyword == token.ttype and token.value.upper() in ('ORDER', 'LIMIT'):
                break
            idx += 1
            self.having.append(token)
        return idx

    def on_JOIN(self, tokens, idx):
        is_on = False
        while idx < len(tokens):
            token = tokens[idx]
            if isinstance(token, stypes.Where):
                break
            if ttypes.Keyword == token.ttype and token.value.upper() in ('ORDER', 'LIMIT', 'GROUP', 'HAVING'):
                break
            idx += 1
            if is_on:
                self.join_conditions.append(token)
            if token.is_whitespace():
                continue
            if token.is_field():
                if self.join_table:
                    raise Exception('can only join one table')
                self.join_table = token.as_field_name()
            elif ttypes.Keyword == token.ttype and 'ON' == token.value.upper():
                is_on = True
        return idx

    def on_GROUP(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_GROUP_BY(tokens, idx)
            else:
                raise Exception('unexpected: %s' % token)

    def on_GROUP_BY(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            self.group_by = OrderedDict()
            if isinstance(token, stypes.IdentifierList):
                ids = list(token.get_identifiers())
            else:
                ids = [token]
            for id in ids:
                if ttypes.Keyword == id.ttype:
                    raise Exception('%s is keyword' % id.value)
                elif id.is_field():
                    group_by_as = id.as_field_name()
                    if group_by_as in self.projections:
                        self.group_by[group_by_as] = self.projections.pop(group_by_as)
                    else:
                        self.group_by[group_by_as] = id
                elif isinstance(id, stypes.Expression):
                    self.group_by[id.value] = id
                elif isinstance(id, stypes.Identifier):
                    if isinstance(id.tokens[0], stypes.Parenthesis):
                        striped = id.tokens[0].strip_parenthesis()
                        if len(striped) > 1:
                            raise Exception('unexpected: %s' % id.tokens[0])
                        self.group_by[id.get_name()] = striped[0]
                    else:
                        self.group_by[id.get_name()] = id.tokens[0]
                else:
                    raise Exception('unexpected: %s' % repr(id))
            return idx

    def on_ORDER(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_ORDER_BY(tokens, idx)
            else:
                raise Exception('unexpected: %s' % token)

    def on_ORDER_BY(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if isinstance(token, stypes.IdentifierList):
                self.order_by = list(token.get_identifiers())
            else:
                self.order_by = [token]
            return idx

    def on_WHERE(self, where):
        self.where = where

    def has_function_projection(self):
        for projection in self.projections.values():
            if isinstance(projection, stypes.Function):
                return True
        return False


def translate_indices(token):
    if token.is_field():
        return ['%s*' % token.as_field_name()]
    elif isinstance(token, stypes.Function):
        functions = {'index': get_indices}
        functions.update(datetime_evaluator.datetime_functions())
        return eval(str(token), {}, functions)
    elif isinstance(token, stypes.Parenthesis):
        return translate_complex_indices(token.tokens[1:-1])
    else:
        raise Exception('unexpected: %s' % token)


def translate_complex_indices(tokens):
    logic_op = None
    indices = []
    for token in tokens:
        if token.is_whitespace():
            continue
        if ttypes.Keyword == token.ttype:
            if 'UNION' == token.value.upper():
                logic_op = token.value.upper()
            elif 'EXCEPT' == token.value.upper():
                logic_op = token.value.upper()
            else:
                raise Exception('unexpected keyword: %s' % token.value)
        else:
            if indices and 'EXCEPT' == logic_op:
                indices.extend(['-%s' % index for index in translate_indices(token)])
            else:
                indices.extend(translate_indices(token))
    return indices


def get_indices(index_pattern, from_datetime=None, to_datetime=None):
    if from_datetime:
        prefix, delimiter, datetime_pattern = index_pattern.partition('%')
        if not delimiter:
            raise Exception('missing datetime pattern in %s' % index_pattern)
        datetime_pattern = '%' + datetime_pattern
        if to_datetime:
            from_datetime = try_strptime(from_datetime, datetime_pattern)
            to_datetime = try_strptime(to_datetime, datetime_pattern)
            step = None
            if '%S' in datetime_pattern:
                step = datetime.timedelta(seconds=1)
            elif '%M' in datetime_pattern:
                step = datetime.timedelta(minutes=1)
            elif '%H' in datetime_pattern:
                step = datetime.timedelta(hours=1)
            elif '%d' in datetime_pattern:
                step = datetime.timedelta(days=1)
            else:
                raise Exception('can not guess the step')
            the_datetime = from_datetime
            indices = []
            while the_datetime <= to_datetime:
                indices.append(the_datetime.strftime(index_pattern))
                the_datetime += step
            return indices
        else:
            the_datetime = try_strptime(from_datetime, datetime_pattern)
            return [the_datetime.strftime(index_pattern)]
    else:
        return [index_pattern]


def try_strptime(date_string, format):
    if isinstance(date_string, datetime.datetime):
        return date_string
    return datetime.datetime.strptime(date_string, format)
