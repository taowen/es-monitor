from es_sql.sqlparse import sql as stypes
from es_sql.sqlparse import tokens as ttypes
from . import filter_translator


def translate_case_when(case_when):
    case_when_translator = CaseWhenNumericRangeTranslator()
    try:
        case_when_aggs = case_when_translator.on_CASE(case_when.tokens[1:])
    except:
        case_when_translator = CaseWhenFiltersTranslator()
        case_when_aggs = case_when_translator.on_CASE(case_when.tokens[1:])
    return case_when_aggs


class CaseWhenNumericRangeTranslator(object):
    def __init__(self):
        self.ranges = []
        self.field = None

    def on_CASE(self, tokens):
        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if 'WHEN' == token.value.upper():
                idx = self.on_WHEN(tokens, idx)
            elif 'ELSE' == token.value.upper():
                idx = self.on_ELSE(tokens, idx)
            elif 'END' == token.value.upper():
                break
            else:
                raise Exception('unexpected: %s' % token)
        return self.build()

    def on_WHEN(self, tokens, idx):
        current_range = {}
        idx = skip_whitespace(tokens, idx)
        token = tokens[idx]
        self.parse_comparison(current_range, token)
        idx = skip_whitespace(tokens, idx + 1)
        token = tokens[idx]
        if 'AND' == token.value.upper():
            idx = skip_whitespace(tokens, idx + 1)
            token = tokens[idx]
            self.parse_comparison(current_range, token)
            idx = skip_whitespace(tokens, idx + 1)
            token = tokens[idx]
        if 'THEN' != token.value.upper():
            raise Exception('unexpected: %s' % token)
        idx = skip_whitespace(tokens, idx + 1)
        token = tokens[idx]
        idx += 1
        current_range['key'] = eval(token.value)
        self.ranges.append(current_range)
        return idx

    def parse_comparison(self, current_range, token):
        if isinstance(token, stypes.Comparison):
            operator = str(token.token_next_by_type(0, ttypes.Comparison))
            if '>=' == operator:
                current_range['from'] = filter_translator.eval_value(token.right)
            elif '<' == operator:
                current_range['to'] = filter_translator.eval_value(token.right)
            else:
                raise Exception('unexpected: %s' % token)
            self.set_field(token.left.as_field_name())
        else:
            raise Exception('unexpected: %s' % token)

    def on_ELSE(self, tokens, idx):
        raise Exception('else is not supported')

    def set_field(self, field):
        if self.field is None:
            self.field = field
        elif self.field != field:
            raise Exception('can only case when on single field: %s %s' % (self.field, field))
        else:
            self.field = field

    def build(self):
        if not self.field or not self.ranges:
            raise Exception('internal error')
        return {
            'range': {
                'field': self.field,
                'ranges': self.ranges
            }
        }


class CaseWhenFiltersTranslator(object):
    def __init__(self):
        self.filters = {}
        self.other_bucket_key = None

    def on_CASE(self, tokens):
        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if 'WHEN' == token.value.upper():
                idx = self.on_WHEN(tokens, idx)
            elif 'ELSE' == token.value.upper():
                idx = self.on_ELSE(tokens, idx)
            elif 'END' == token.value.upper():
                break
            else:
                raise Exception('unexpected: %s' % token)
        return self.build()

    def on_WHEN(self, tokens, idx):
        filter_tokens = []
        bucket_key = None
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.is_whitespace():
                continue
            if ttypes.Keyword == token.ttype and 'THEN' == token.value.upper():
                idx = skip_whitespace(tokens, idx + 1)
                bucket_key = eval(tokens[idx].value)
                idx += 1
                break
            filter_tokens.append(token)
        if not filter_tokens:
            raise Exception('case when can not have empty filter')
        self.filters[bucket_key] = filter_translator.create_compound_filter(filter_tokens)
        return idx

    def on_ELSE(self, tokens, idx):
        idx = skip_whitespace(tokens, idx + 1)
        self.other_bucket_key = eval(tokens[idx].value)
        idx += 1
        return idx

    def set_field(self, field):
        if self.field is None:
            self.field = field
        elif self.field != field:
            raise Exception('can only case when on single field: %s %s' % (self.field, field))
        else:
            self.field = field

    def build(self):
        if not self.filters:
            raise Exception('internal error')
        agg = {'filters': {'filters': self.filters}}
        if self.other_bucket_key:
            agg['filters']['other_bucket_key'] = self.other_bucket_key
        return agg


def skip_whitespace(tokens, idx):
    while idx < len(tokens):
        token = tokens[idx]
        if token.is_whitespace():
            idx += 1
            continue
        else:
            break
    return idx
