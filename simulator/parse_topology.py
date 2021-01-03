import pandas as pd
import datetime
import sys
import os
import math

topo_fname = "location_info_of_ssd.csv"
fail_fname = "ssd_failure_tag.csv"

def clustering(prefix_dir):
    df = pd.read_csv(prefix_dir + topo_fname)
    df_failed = pd.read_csv(prefix_dir + fail_fname)
    # note: disk_id is only unique within the same drive model
    df = df.merge(df_failed[['model', 'disk_id', 'failure_time'
                            ]], how='left',
                 on=['model', 'disk_id'])

    # for the nodes with the entry without slot_id
    node_ids = df[df['slot_id'].isnull()]['node_id']
    df1 = df[df['node_id'].isin(node_ids)]
    df1_node = df1[['node_id', 'disk_id']].groupby([
        'node_id'
    ]).count().rename(columns={'disk_id': '#disks/node'})

    # count the number of slots for each node
    df2 = df[~(df['node_id'].isin(node_ids))]
    # drop duplicate slots in a node due to SSD replacements
    df2_node = df2[['node_id', 'slot_id']].drop_duplicates().groupby(
        ['node_id']).count().rename(columns={'slot_id': '#disks/node'})
    df_node = pd.concat([df1_node, df2_node], axis=0)

    df = df.join(df_node, on='node_id')
    df_rack = df[['rack_id', 'node_id',
                  '#disks/node']].drop_duplicates().groupby([
                      'rack_id', '#disks/node'
                  ]).count().rename(columns={'node_id': '#nodes/rack'})
    df = df.merge(df_rack, on=['rack_id', '#disks/node'])
    df_total_disks = df[[
        'disk_id', '#nodes/rack', '#disks/node', 'failure_time'
    ]].groupby(['#disks/node',
                '#nodes/rack']).count().rename(columns={
                    'disk_id': '#total disks',
                    'failure_time': '#failures'
                })

    df_meta = df[['rack_id', '#nodes/rack',
                  '#disks/node']].drop_duplicates().groupby([
                      '#disks/node', '#nodes/rack'
                  ]).count().rename(columns={'rack_id': '#racks'})
    df_meta = df_meta.merge(df_total_disks, on=['#disks/node', '#nodes/rack'])
    df_meta = df_meta[df_meta['#racks'] >= 16] # the maximum number of chunks in a coding group
    df_all = df.merge(df_meta, on=['#disks/node', '#nodes/rack'])
    df_meta.to_csv(prefix_dir + "meta.csv")
    df_all.to_csv(prefix_dir + "topo_all.csv", index=False)
    print("Obtained all clusters topology.")
    return (df_meta, df_all)


def give_disk_id(df):
    """
    df includes all disks on one node
    """
    # for the disks with slot numbers
    df1 = df.dropna(subset=['slot_id'])
    # for the disks with unknown and empty slots
    df2 = df[df['slot_id'].isnull()]

    slots = list(df1['slot_id'].unique())
    if len(slots) > 0:
        df1['disk_id_local'] = df1.apply(lambda x: slots.index(x['slot_id']),
                                   axis=1)
    if len(df2.index) > 0:
        df2 = df2.reset_index(drop=True)
        df2['disk_id_local'] = df2.index + len(slots)
    return pd.concat([df1, df2], axis=0)


def parse_each_cluster(prefix_dir):
    print("Start to get failure timestamps of SSDs for each cluster.")
    df_meta = pd.read_csv(prefix_dir + "meta.csv")
    df_meta = df_meta[df_meta['#failures'] > 0]
    df_all = pd.read_csv(prefix_dir + "topo_all.csv")
    for index, row in df_meta.iterrows():
        disks_per_node = row['#disks/node']
        nodes_per_rack = row['#nodes/rack']
        num_racks = row['#racks']
        df = df_all[(df_all['#disks/node'] == disks_per_node)
                    & (df_all['#nodes/rack'] == nodes_per_rack)]
        # give rack id
        rack_ids = list(df.rack_id.unique())
        if len(rack_ids) != num_racks:
            print("rack_ids = %d, num_racks = %d" % (len(rack_ids), num_racks))
            sys.exit(2)
        df_racks = df[['rack_id']].drop_duplicates()
        df_racks['rack_id_local'] = df_racks.apply(
            lambda x: rack_ids.index(x['rack_id']), axis=1)
        df = df.join(df_racks.set_index('rack_id'), on='rack_id')
        # give node id
        grouped_rack = df.groupby(['rack_id'])
        df_res = pd.DataFrame()
        for rack_id, group in grouped_rack:
            node_ids = list(group.node_id.unique())
            if len(node_ids) != nodes_per_rack:
                print("node_ids = %d, nodes_per_rack = %d" %
                      (len(node_ids), nodes_per_rack))
                sys.exit(2)
            df_nodes = group[['node_id']].drop_duplicates()
            df_nodes['node_id_local'] = df_nodes.apply(
                lambda x: node_ids.index(x['node_id']), axis=1)
            group = group.join(df_nodes.set_index('node_id'), on='node_id')
            df_res = pd.concat([df_res, group], axis=0)
        # give disk id
        grouped_node = df_res.groupby(['node_id'])
        df_res = pd.DataFrame()
        for node_id, group in grouped_node:
            group = give_disk_id(group)
            df_res = pd.concat([df_res, group], axis=0)
        df_res['disk_total_id'] = df_res[
            'rack_id_local'] * nodes_per_rack * disks_per_node + df_res[
                'node_id_local'] * disks_per_node + df_res['disk_id_local']

        # only output failure entries for simulator
        df_res = filter_failure_entries(df_res)

        df_res.to_csv(prefix_dir + "clusters/d%dn%d.csv" %
                      (disks_per_node, nodes_per_rack),
                      index=False)

def filter_failure_entries(df):
    df = df.dropna(subset=['failure_time'])
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    # convert to relative failure time to 2018-01-01 (in hours)
    df['fail_time'] = (df['failure_time'] -
                                datetime.datetime.strptime(
                                    "2018-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                                ).dt.total_seconds() / 3600.0
    df['disk_total_id'] = df['disk_total_id'].astype('int')
    return df[['disk_total_id', 'fail_time']]

def parse_topology_loc(prefix_dir):
    try:
        os.mkdir(prefix_dir + "clusters/")
    except Exception as e:
        print(e)
    clustering(prefix_dir)
    parse_each_cluster(prefix_dir)

def usage():
    print("python parse_topology [data dir]")

if __name__ == '__main__':
    try:
        prefix_dir = sys.argv[1]
        parse_topology_loc(prefix_dir)
    except Exception as e:
        usage()
        print(e)
