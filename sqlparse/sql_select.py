import sqlparse
from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
from sqlparse.ordereddict import OrderedDict


# make the result of sqlparse more usable
class SqlSelect(object):
    def __init__(self, tokens):
        self.from_table = None
        self.projections = {}
        self.group_by = OrderedDict()
        self.order_by = []
        self.limit = None
        self.having = []
        self.where = None
        self.join_table = None
        self.join_conditions = []

        self.buckets_names = {}
        self.joinable_results = {}
        self.joinable_queries = {}

        self.is_select_inside = False
        self.on_SELECT(tokens)
        if isinstance(self.from_table, basestring):
            if self.group_by or self.has_function_projection():
                self.is_select_inside = True

    @classmethod
    def parse(cls, *args, **kwargs):
        statement = sqlparse.parse(*args, **kwargs)[0]
        sql_select = SqlSelect(statement.tokens)
        return sql_select

    def tables(self):
        map = {self.from_table: True}
        for t in self.joinable_results.keys():
            map[t] = False
        for t in self.joinable_queries.keys():
            map[t] = False
        return map

    def on_SELECT(self, tokens):
        if not (ttypes.DML == tokens[0].ttype and 'SELECT' == tokens[0].value.upper()):
            raise Exception('it is not SELECT: %s' % tokens[0])
        idx = 1
        source_found = False
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if ttypes.Keyword == token.ttype:
                if token.value.upper() in ('FROM', 'INSIDE'):
                    self.is_select_inside = 'INSIDE' == token.value.upper()
                    source_found = True
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
                    print(tokens)
                    raise Exception('unexpected: %s' % token)
            elif isinstance(token, stypes.Where):
                self.on_WHERE(token)
            elif not source_found:
                self.set_projections(token)
                continue
            else:
                raise Exception('unexpected: %s' % token)

    def set_projections(self, token):
        if token.ttype == ttypes.Punctuation:
            raise Exception('a.b should use the form "a.b"')
        if self.projections:
            raise Exception('projections has already been set')
        if isinstance(token, stypes.IdentifierList):
            ids = list(token.get_identifiers())
        else:
            ids = [token]
        self.projections = {}
        for id in ids:
            if isinstance(id, stypes.Identifier):
                self.projections[id.get_name() or str(id)] = id.without_as()
            elif ttypes.String.Symbol == id.ttype:
                self.projections[id.value[1:-1]] = id
            else:
                self.projections[str(id)] = id

    def on_FROM(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if token.is_field():
                self.from_table = token.as_field_name()
                break
            elif isinstance(token, stypes.Identifier):
                if len(token.tokens) > 1:
                    raise Exception('unexpected: %s' % token)
                self.from_table = token.get_name()
                break
            elif isinstance(token, stypes.Function):
                self.from_table = token
                break
            else:
                raise Exception('unexpected: %s' % token)
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
            if ttypes.Keyword == token.ttype and token.value.upper() in ('ORDER', 'LIMIT', 'GROUP', 'WHERE', 'HAVING'):
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
                elif ttypes.Name == id.ttype:
                    self.group_by[id.value] = id
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
