import urllib2
import json
import sys
import os
import zipfile
import csv
import StringIO
import contextlib


def main():
    create_index_template()
    quote_csvs = read_quote_csvs()
    delete_index('quote')
    create_index('quote')
    for symbol, quote_csv in quote_csvs:
        quotes = read_quotes(quote_csv, symbol)
        lines = list(bluk_import_lines('quote', quotes))
        to_import_count = len(lines) / 2
        imported_count = read_index_documents_count(symbol)
        if imported_count >= to_import_count - 5:
            print('skip %s' % symbol)
            continue
        print('import %s' % symbol)
        request = urllib2.Request('http://localhost:9200/_bulk', data='\n'.join(lines))
        response = urllib2.urlopen(request).read()


def read_index_documents_count(symbol):
    try:
        return json.loads(urllib2.urlopen('http://127.0.0.1:9200/quote/_count?q=symbol:%s' % symbol).read())['count']
    except:
        return 0


def create_index_template():
    request = urllib2.Request('http://localhost:9200/_template/quote', data=json.dumps({
        'template': 'quote',
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'index.codec': 'best_compression'
        },
        'mappings': {
            'quote': {
                '_all': {'enabled': False},
                '_source': {'enabled': True},
                'properties': {
                    'symbol': {'type': 'string', 'index': 'not_analyzed'},
                    'adj_close': {'type': 'long', 'index': 'not_analyzed'},
                    'close': {'type': 'long', 'index': 'not_analyzed'},
                    'open': {'type': 'long', 'index': 'not_analyzed'},
                    'high': {'type': 'long', 'index': 'not_analyzed'},
                    'low': {'type': 'long', 'index': 'not_analyzed'},
                    'volume': {'type': 'long', 'index': 'not_analyzed'},
                    'date': {'type': 'date', 'index': 'not_analyzed'}
                }
            }
        }
    }))
    request.get_method = lambda: 'PUT'
    response = urllib2.urlopen(request).read()
    print(response)


def read_quote_csvs():
    with zipfile.ZipFile('quote.zip') as quote_zip:
        for file in quote_zip.namelist():
            if file.endswith('.csv'):
                symbol = os.path.basename(file).replace('.csv', '')
                yield symbol, quote_zip.read(file)


def read_quotes(quote_csv, symbol):
    with contextlib.closing(StringIO.StringIO(quote_csv)) as f:
        quotes = csv.DictReader(f, fieldnames=['date', 'open', 'high', 'low', 'close', 'volume', 'adj_close'])
        quotes.next()
        for quote in quotes:
            try:
                quote['symbol'] = symbol
                quote['open'] = long(float(quote['open']) * 100)
                quote['high'] = long(float(quote['high']) * 100)
                quote['low'] = long(float(quote['low']) * 100)
                quote['close'] = long(float(quote['close']) * 100)
                quote['adj_close'] = long(float(quote['adj_close']) * 100)
                quote['volume'] = long(quote['volume'])
                yield quote
            except:
                pass


def bluk_import_lines(index_name, quotes):
    for quote in quotes:
        yield json.dumps(
            {'index': {'_index': index_name, '_type': 'quote', '_id': '%s-%s' % (quote['symbol'], quote['date'])}})
        yield json.dumps(quote)


def delete_index(index_name):
    try:
        request = urllib2.Request('http://localhost:9200/%s/' % index_name)
        request.get_method = lambda: 'DELETE'
        response = urllib2.urlopen(request).read()
    except:
        pass


def create_index(index_name):
    request = urllib2.Request('http://localhost:9200/%s/' % index_name)
    request.get_method = lambda: 'PUT'
    try:
        response = urllib2.urlopen(request).read()
    except urllib2.HTTPError as e:
        print(e.read())
        sys.exit(1)


main()
