import sqlparse
from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
from sqlparse.ordereddict import OrderedDict

# make the result of sqlparse more usable
class SqlSelect(object):
    def __init__(self):
        self.select_from = None
        self.projections = {}
        self.group_by = OrderedDict()
        self.order_by = []
        self.limit = None
        self.having = []
        self.where = None
        self.is_select_inside = False

    def get_inside_projection(self, projection_name):
        projection = self.projections.get(projection_name)
        if projection:
            return self.get_bucket_keys(), projection
        else:
            if isinstance(self.select_from, SqlSelect):
                return self.select_from.get_inside_projection(projection_name)
            else:
                raise Exception('projection not found: %s' % projection_name)

    def get_bucket_keys(self):
        if isinstance(self.select_from, basestring):
            return [self.group_by.keys()]
        else:
            return [self.group_by.keys()] + self.select_from.get_bucket_keys()

    def on_SELECT(self, tokens):
        if not (ttypes.DML == tokens[0].ttype and 'SELECT' == tokens[0].value.upper()):
            raise Exception('it is not SELECT: %s' % tokens[0])
        idx = 1
        from_found = False
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if token.value.upper() in ('FROM', 'INSIDE'):
                    self.is_select_inside = 'INSIDE' == token.value.upper()
                    from_found = True
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
                else:
                    raise Exception('unexpected: %s' % repr(token))
            elif isinstance(token, stypes.Where):
                self.on_WHERE(token)
            elif not from_found:
                self.set_projections(token)
                continue
            else:
                raise Exception('unexpected: %s' % repr(token))

    def set_projections(self, token):
        if isinstance(token, stypes.IdentifierList):
            ids = list(token.get_identifiers())
        else:
            ids = [token]
        self.projections = {}
        for id in ids:
            if isinstance(id, stypes.Identifier):
                self.projections[id.get_name()] = id
            else:
                self.projections[id.value] = id

    def on_FROM(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if isinstance(token, stypes.Identifier):
                self.select_from = token.get_name()
                break
            elif isinstance(token, stypes.Parenthesis):
                self.select_from = SqlSelect()
                select_from = sqlparse.parse(token.value[1:-1].strip())[0]
                self.select_from.on_SELECT(select_from.tokens)
                break
            else:
                raise Exception('unexpected: %s' % repr(token))
        return idx

    def on_LIMIT(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
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
            else:
                idx += 1
                self.having.append(token)
        return idx

    def on_GROUP(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_GROUP_BY(tokens, idx)
            else:
                raise Exception('unexpected: %s' % repr(token))

    def on_GROUP_BY(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            self.group_by = OrderedDict()
            if isinstance(token, stypes.IdentifierList):
                for id in token.get_identifiers():
                    if ttypes.Keyword == id.ttype:
                        raise Exception('%s is keyword' % id.value)
                    elif isinstance(id, stypes.Identifier):
                        self.group_by[id.get_name()] = id
                    else:
                        raise Exception('unexpected: %s' % repr(id))
            elif isinstance(token, stypes.Identifier):
                self.group_by[token.get_name()] = token
            else:
                raise Exception('unexpected: %s' % repr(token))
            return idx

    def on_ORDER(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_ORDER_BY(tokens, idx)
            else:
                raise Exception('unexpected: %s' % repr(token))

    def on_ORDER_BY(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if isinstance(token, stypes.IdentifierList):
                self.order_by = token.get_identifiers()
            else:
                self.order_by = [token]
            return idx

    def on_WHERE(self, where):
        self.where = where
