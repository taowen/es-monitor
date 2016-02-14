import urllib2
import json
import sys
import csv


def main():
    create_index_template()
    delete_index()
    create_index()
    symbols = read_symbols()
    request = urllib2.Request('http://localhost:9200/_bulk', data='\n'.join(bluk_import_lines(symbols)))
    response = urllib2.urlopen(request).read()
    print(response)


def bluk_import_lines(symbols):
    for symbol in symbols:
        yield json.dumps({'index': {'_index': 'symbol', '_type': 'symbol'}})
        yield json.dumps(symbol)


def read_symbols():
    for exchange in ['nasdaq', 'nyse', 'nyse mkt']:
        with open('%s.csv' % exchange) as f:
            f.readline()
            for symbol in csv.DictReader(f,
                                         fieldnames=['symbol', 'name', 'last_sale', 'market_cap', 'ipo_year', 'sector',
                                                     'industry']):
                symbol.pop(None, None)
                symbol['exchange'] = 'nasdaq'
                if 'n/a' == symbol['ipo_year']:
                    symbol['ipo_year'] = None
                else:
                    symbol['ipo_year'] = int(symbol['ipo_year'])
                if 'n/a' == symbol['last_sale']:
                    symbol['last_sale'] = None
                else:
                    symbol['last_sale'] = int(float(symbol['last_sale']) * 100)
                if 'n/a' == symbol['market_cap']:
                    symbol['market_cap'] = None
                elif '0' == symbol['market_cap']:
                    symbol['market_cap'] = 0
                else:
                    try:
                        symbol['market_cap'] = long(float(symbol['market_cap'][1:]))
                    except:
                        unit = symbol['market_cap'][-1]
                        if 'M' == unit:
                            symbol['market_cap'] = long(float(symbol['market_cap'][1:-1]) * 1000L * 1000L)
                        elif 'B' == unit:
                            symbol['market_cap'] = long(float(symbol['market_cap'][1:-1]) * 1000L * 1000L * 1000L)
                        else:
                            raise Exception('unexpected unit: %s' % symbol['market_cap'])
                yield symbol


def delete_index():
    try:
        request = urllib2.Request('http://localhost:9200/symbol/')
        request.get_method = lambda: 'DELETE'
        response = urllib2.urlopen(request).read()
        print(response)
    except:
        pass


def create_index():
    request = urllib2.Request('http://localhost:9200/symbol/')
    request.get_method = lambda: 'PUT'
    try:
        response = urllib2.urlopen(request).read()
        print(response)
    except urllib2.HTTPError as e:
        print(e.read())
        sys.exit(1)


def create_index_template():
    request = urllib2.Request('http://localhost:9200/_template/symbol', data=json.dumps({
        'template': 'symbol',
        'settings': {
            'number_of_shards': 3,
            'number_of_replicas': 0
        },
        'mappings': {
            'symbol': {
                '_source': {'enabled': True},
                'properties': {
                    'symbol': {'type': 'string', 'index': 'not_analyzed'},
                    'name': {'type': 'string', 'index': 'analyzed'},
                    'last_sale': {'type': 'long', 'index': 'not_analyzed'},
                    'market_cap': {'type': 'long', 'index': 'not_analyzed'},
                    'ipo_year': {'type': 'integer', 'index': 'not_analyzed'},
                    'sector': {'type': 'string', 'index': 'not_analyzed'},
                    'industry': {'type': 'string', 'index': 'not_analyzed'},
                    'exchange': {'type': 'string', 'index': 'not_analyzed'}
                }
            }
        }
    }))
    request.get_method = lambda: 'PUT'
    response = urllib2.urlopen(request).read()
    print(response)


main()
