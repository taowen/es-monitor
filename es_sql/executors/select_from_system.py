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
        collect_stats_rows(rows, response, ['cluster'])
        response = {'hits': {'hits': rows}}
    elif sql_select.from_table.startswith('_cluster_pending_tasks'):
        response = json.loads(urllib2.urlopen('%s/_cluster/pending_tasks' % es_url).read())
        response = {'hits': {'hits': [{'_source': task} for task in response.get('tasks', [])]}}
    elif sql_select.from_table.startswith('_cluster_reroute'):
        response = json.loads(urllib2.urlopen('%s/_cluster/reroute' % es_url).read())
        commands = []
        for command in response.get('commands', []):
            for command_name, command_args in command.iteritems():
                command_args['command_name'] = command_name
                commands.append({'_source': command_args})
        response = {'hits': {'hits': commands}}
    elif sql_select.from_table.startswith('_nodes_stats'):
        response = json.loads(urllib2.urlopen('%s/_nodes/stats' % es_url).read())
        all_rows = []
        for node_id, node in response.get('nodes', {}).iteritems():
            node_name = node.pop('name', None)
            node_transport_address = node.pop('transport_address', None)
            node_host = node.pop('host', None)
            node.pop('ip', None)
            rows = []
            collect_stats_rows(rows, node, ['nodes'])
            for row in rows:
                row['_source']['node_id'] = node_id
                row['_source']['node_name'] = node_name
                row['_source']['node_transport_address'] = node_transport_address
                row['_source']['node_host'] = node_host
            all_rows.extend(rows)
        response = {'hits': {'hits': all_rows}}
    elif sql_select.from_table.startswith('_nodes_info'):
        response = json.loads(urllib2.urlopen('%s/_nodes' % es_url).read())
        nodes = []
        for node_id, node in response.get('nodes', {}).iteritems():
            node['node_id'] = node_id
            nodes.append({'_source': node})
        response = {'hits': {'hits': nodes}}
    elif sql_select.from_table.startswith('_indices_stats'):
        _, _, target_index_name = sql_select.from_table.partition('.')
        response = json.loads(urllib2.urlopen('%s/_stats' % es_url).read())
        all_rows = []
        if target_index_name:
            for index_name, index_stats in response.get('indices', {}).iteritems():
                if target_index_name != 'all' and target_index_name != index_name:
                    continue
                rows = []
                collect_stats_rows(rows, index_stats, ['indices', 'per_index'])
                for row in rows:
                    row['_source']['index_name'] = index_name
                all_rows.extend(rows)
        else:
            rows = []
            collect_stats_rows(rows, response.get('_shards', {}), ['indices', 'shards'])
            all_rows.extend(rows)
            rows = []
            collect_stats_rows(rows, response.get('_all', {}), ['indices', 'all'])
            all_rows.extend(rows)
        response = {'hits': {'hits': all_rows}}
    if sql_select.where and response and response['hits']['hits']:
        columns = sorted(response['hits']['hits'][0]['_source'].keys())
        import sqlite3
        with sqlite3.connect(':memory:') as conn:
            conn.execute('CREATE TABLE temp(%s)' % (', '.join(columns)))
            rows = [[hit['_source'][column] for column in columns] for hit in response['hits']['hits']]
            conn.executemany('INSERT INTO temp VALUES (%s)' % (', '.join(['?'] * len(columns))), rows)
            filtered_rows = []
            for row in conn.execute('SELECT * FROM temp %s' % sql_select.where):
                filtered_rows.append({'_source': dict(zip(columns, row))})
            return {'hits': {'hits': filtered_rows}}
    return response

def collect_stats_rows(rows, response, path):
    if isinstance(response, dict):
        for k, v in response.iteritems():
            collect_stats_rows(rows, v, path + [k])
    elif isinstance(response, (tuple, list)):
        for e in response:
            collect_stats_rows(rows, e, path)
    else:
        rows.append({'_source': {
            '_metric_name': '.'.join(path),
            'value': response
        }})
