import urllib2
import json
import sys


def main():
    create_index_template()
    delete_index()
    create_index()
    


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
