import urllib2
import json

def execute(es_url, sql_select):
    response = None
    if sql_select.from_table.startswith('_cluster_health'):
        response = json.loads(urllib2.urlopen('%s/_cluster/health' % es_url).read())
        response = {'hits': {'hits': [{'_source': response}]}}
    elif sql_select.from_table.startswith('_cluster_state'):
        _, _, metric = sql_select.from_table.partition('.')
        if metric == 'nodes':
            response = json.loads(urllib2.urlopen('%s/_cluster/state/nodes' % es_url).read())
            nodes = []
            for node_id, node in response['nodes'].iteritems():
                node['node_id'] = node_id
                nodes.append({'_source': node})
            response = {'hits': {'hits': nodes}}
        elif metric == 'blocks':
            response = json.loads(urllib2.urlopen('%s/_cluster/state/blocks' % es_url).read())
            blocks = []
            for index_name, index_blocks in response['blocks'].get('indices', {}).iteritems():
                for block_no, block in index_blocks.iteritems():
                    block['block_no'] = block_no
                    block['index_name'] = index_name
                    blocks.append({'_source': block})
            response = {'hits': {'hits': blocks}}
        elif metric == 'routing_table':
            response = json.loads(urllib2.urlopen('%s/_cluster/state/routing_table' % es_url).read())
            routing_tables = []
            for index_name, index_shards in response['routing_table'].get('indices', {}).iteritems():
                for shard_index, shard_tables in index_shards.get('shards', {}).iteritems():
                    for shard_table in shard_tables:
                        shard_table['shard_index'] = shard_index
                        shard_table['index_name'] = index_name
                        routing_tables.append({'_source': shard_table})
            response = {'hits': {'hits': routing_tables}}
        elif metric == 'routing_nodes':
            response = json.loads(urllib2.urlopen('%s/_cluster/state/routing_nodes' % es_url).read())
            routing_nodes = []
            for node_no, node_routing_nodes in response['routing_nodes'].get('nodes', {}).iteritems():
                for routing_node in node_routing_nodes:
                    routing_node['is_assigned'] = True
                    routing_node['node_no'] = node_no
                    routing_nodes.append({'_source': routing_node})
            for routing_node in response['routing_nodes'].get('unassigned', []):
                routing_node['is_assigned'] = False
                routing_node['node_no'] = None
                routing_nodes.append({'_source': routing_node})
            response = {'hits': {'hits': routing_nodes}}
        else:
            response = json.loads(urllib2.urlopen('%s/_cluster/state' % es_url).read())
            response = {'hits': {'hits': [{'_source': response}]}}
    elif sql_select.from_table.startswith('_cluster_stats'):
        response = json.loads(urllib2.urlopen('%s/_cluster/stats' % es_url).read())
        rows = []
        collect_cluster_stats_rows(rows, response, [])
        response = {'hits': {'hits': rows}}
    return response

def collect_cluster_stats_rows(rows, response, path):
    if isinstance(response, dict):
        for k, v in response.iteritems():
            collect_cluster_stats_rows(rows, v, path + [k])
    elif isinstance(response, (tuple, list)):
        for e in response:
            collect_cluster_stats_rows(rows, e, path)
    else:
        rows.append({'_source': {
            '_metric_name': '.'.join(path),
            'value': response
        }})
