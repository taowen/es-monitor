import gzip
import urllib2
import json


def main():
    create_index_template()
    for j in range(1, 32):
        for i in range(24):
            filename = 'githubarchive/2015-01/2015-01-%02d-%s.json.gz' % (j, i)
            print('import %s' % filename)
            gzipFile = gzip.GzipFile(filename)
            events = gzipFile.readlines()
            bulk_import_lines = []
            index_name = 'githubarchive-%s' % '2015-01-%02d' % j
            for event in events:
                eventObj = json.loads(event)
                bulk_import_lines.append(json.dumps(
                    {'index': {'_index': index_name, '_type': eventObj['type'], '_id': eventObj['id']}}))
                bulk_import_lines.append(event)
            request = urllib2.Request('http://localhost:9200/_bulk', data='\n'.join(bulk_import_lines))
            urllib2.urlopen(request).read()


def create_index_template():
    mappings = {}
    for type in ['PushEvent', 'CreateEvent', 'DeleteEvent', 'ForkEvent', 'GollumEvent', 'IssueCommentEvent',
                 'IssuesEvent', 'MemberEvent', 'PullRequestEvent', 'WatchEvent', 'CommitCommentEvent', 'PublicEvent',
                 'PullRequestReviewCommentEvent', 'ReleaseEvent']:
        mappings[type] = {
            '_all': {'enabled': False},
            '_source': {'enabled': False},
            'dynamic': False,
            'properties': {
                'created_at': {'type': 'date', 'index': 'not_analyzed'},
                'type': {'type': 'string', 'index': 'not_analyzed'},
                'repo': {
                    'properties': {
                        'name': {'type': 'string', 'index': 'not_analyzed'}
                    }
                },
                'actor': {
                    'properties': {
                        'login': {'type': 'string', 'index': 'not_analyzed'}
                    }
                },
                'org': {
                    'properties': {
                        'login': {'type': 'string', 'index': 'not_analyzed'}
                    }
                }
            }
        }
    request = urllib2.Request('http://localhost:9200/_template/githubarchive', data=json.dumps({
        'template': 'githubarchive-*',
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'index.codec': 'best_compression'
        },
        'mappings': mappings
    }))
    request.get_method = lambda: 'PUT'
    response = urllib2.urlopen(request).read()
    print(response)


main()
