import os
import sys
import pandas as pd
import numpy as np
from smart_converter import SMARTConverter

def compute_time_diff(df, col_name=None, group=False):
    if group is True:
        if type(col_name) == list:
            df['diff'] = df.groupby(col_name)['failure_time'].diff().dt.total_seconds()
        else:
            df['diff'] = df.groupby([col_name])['failure_time'].diff().dt.total_seconds()
    else:
        df['diff'] = df['failure_time'].diff().dt.total_seconds()
    return df

def burst_global_group(df, col_name, time=1800):
    grouped = df.groupby(col_name)
    res_df = pd.DataFrame()
    burst_id = -1
    for col, group in grouped:
        for index, row in group.iterrows():
            if row['diff'] == -1:
                burst_id += 1
                group.loc[index, 'burst_glob_id'] = burst_id
            elif row['diff'] > time:  # 30mins
                burst_id += 1
                group.loc[index, 'burst_glob_id'] = burst_id
            else:
                group.loc[index, 'burst_glob_id'] = burst_id
        res_df = pd.concat([res_df, group], axis=0)
    return res_df

def find_failure_group(df, col_name, time):
    # col_name is node_id or rack_id
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(by=['failure_time'])
    df = compute_time_diff(df, col_name, True)
    df['diff'] = df['diff'].fillna(value=-1)
    df = df.sort_values([col_name, 'failure_time'])
    df['burst_glob_id'] = -1
    df = burst_global_group(df, col_name, time)
    return df

def separate_failures(df):
    df1 = df.groupby(['burst_glob_id'])['disk_id'].count().reset_index()
    intra_failures = df[df['burst_glob_id'].isin(df1[df1['disk_id'] > 1]['burst_glob_id'].values)]
    non_intra_failures = df[~df['disk_id'].isin(intra_failures['disk_id'])]
    return (intra_failures, non_intra_failures)

def get_intra_failures(df):
    # Section 4.1 - Finding 1
    time = 30*60 # 30 mins
    res_df = find_failure_group(df, 'node_id', time)
    (intra_df, non_intra_df) = separate_failures(res_df)
    intra_df.to_csv("results/intra_node_failures_30mins.csv", index=False)
    intra_node = intra_df.groupby(['burst_glob_id'])['disk_id'].count().reset_index().rename(
            columns={'disk_id': 'group_size'}).groupby(['group_size']).count().reset_index().rename(
                    columns={'burst_glob_id': 'count'})
    intra_node['percent (%)'] = intra_node['group_size'] * intra_node['count'] / df.shape[0] * 100
    intra_node.to_csv("results/finding_1_node.csv", index=False)

    res_df = find_failure_group(df, 'rack_id', time)
    (intra_df, non_intra_df) = separate_failures(res_df)
    intra_df.to_csv("results/intra_rack_failures_30mins.csv", index=False)
    intra_rack = intra_df.groupby(['burst_glob_id'])['disk_id'].count().reset_index().rename(
            columns={'disk_id': 'group_size'}).groupby(['group_size']).count().reset_index().rename(
                    columns={'burst_glob_id': 'count'})
    intra_rack['percent (%)'] = intra_rack['group_size'] * intra_rack['count'] / df.shape[0] * 100
    intra_rack.to_csv("results/finding_1_rack.csv", index=False)

def get_intra_failures_diff_thresholds(df):
    # Section 4.1 - Finding 3
    time = [60, 1800, 3600, 3600*24, 3600*24*7, 3600*24*30, 3600*24*365]
    name = ['1min', '30mins', '1hour', '1day', '1week', '1month', '1year']
    res_df = pd.DataFrame()
    for i in range(len(time)):
        res_df = find_failure_group(df, 'node_id', time[i])
        (intra_df, non_intra_df) = separate_failures(res_df)
        intra_node = (intra_df.shape[0] / df.shape[0])

        res_df = find_failure_group(df, 'rack_id', time[i])
        (intra_df, non_intra_df) = separate_failures(res_df)
        intra_rack = (intra_df.shape[0] / df.shape[0])

        res_df = res_df.append({'threshold': time[i], 'intra_node': intra_node, 'intra_rack': intra_rack}, ignore_index=True)
    res_df.to_csv("results/finding_3.csv", index=False)

def label_intra_failures_for_factors(col_name, sub_col_name, df, time):
    # col_name is model/lithography/capacity/age
    # sub_col_name is node_id/rack_id
    grouped = df.groupby([col_name])
    res_df = pd.DataFrame()
    for col, group in grouped:
        group = find_failure_group(group, sub_col_name, time)
        res_df = pd.concat([res_df, group], axis=0)
    return res_df

def find_intra_failures_for_factors(col_name, sub_col_name, 
        df, time):
    # col_name is model/lithography/capacity/age
    # sub_col_name is node_id/rack_id
    grouped = df.groupby([col_name])
    res_df = pd.DataFrame()
    for col, group in grouped:
        group = find_failure_group(group, sub_col_name, time)
        (intra_group, non_intra_group) = separate_failures(group) # filter intra-* failures
        print(col, intra_group.shape[0], non_intra_group.shape[0])
        res_df = res_df.append({'factor': col, 'percent': intra_group.shape[0]/group.shape[0]}, ignore_index=True)
    return res_df

def spatial_bursty_percent(col_name, sub_col_name, df):
    # col_name: drive model...
    # sub_col_name: burst_glob_id
    res_df = pd.DataFrame()
    grouped = df.groupby([col_name])
    for col, group in grouped:
        total_len = group.shape[0]
        (df_intra, df_non_intra) = separate_failures(group)
        new_subgroup = df_intra.groupby([sub_col_name])['disk_id'].size().reset_index()
        df_count = new_subgroup.groupby(['disk_id'])[sub_col_name].size().reset_index().rename(
                columns={sub_col_name:'count', 'disk_id': '#failures'})
        df_count['total'] = df_count['count'] * df_count['#failures']
        df_count['percent'] = df_count['total'] / total_len
        df_count[col_name] = col
        tmp_df = pd.DataFrame()
        tmp_df = tmp_df.append({'interval': '(1,10]', 'percent': df_count[(df_count['#failures'] <= 10) & 
            (df_count['#failures'] > 1)]['total'].sum() / total_len}, ignore_index=True)
        tmp_df = tmp_df.append({'interval': '(10,20]', 'percent': df_count[(df_count['#failures'] <= 20) & 
            (df_count['#failures'] > 10)]['percent'].sum()}, ignore_index=True)
        tmp_df = tmp_df.append({'interval': '(20,30]', 'percent': df_count[(df_count['#failures'] <= 30) & 
            (df_count['#failures'] > 20)]['percent'].sum()}, ignore_index=True)
        tmp_df = tmp_df.append({'interval': '(30,89]', 'percent': df_count[(df_count['#failures'] <= 89) & 
            (df_count['#failures'] > 30)]['percent'].sum()}, ignore_index=True)
        tmp_df[col_name] = col
        res_df = pd.concat([res_df, tmp_df], axis=0)
    return res_df

def spatial_correlations(df, granularity):
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(['failure_time'])
    if granularity == 'model':
        sub_col_name = 'node_id'
        col_name = 'model'
        time = 1800
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_node_model.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_rack_model.csv", index=False)

    elif granularity == "lithography":
        dict_ = {'A3': '20nm',
                'A6': '20nm',
                'A4': '16nm',
                'A1': '20nm',
                'A5': '16nm',
                'A2': '20nm',
                'B2': '19nm',
                'B3': '24nm',
                'B1': '21nm',
                'C1': 'V1',
                'C2': 'V1'}
        df['lithography'] = df['model']
        for key, value in dict_.items():
            df['lithography'] = df['lithography'].replace(key, value)
        sub_col_name = 'node_id'
        col_name = "lithography"
        time = 1800
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_node_litho.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
            col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_rack_litho.csv", index=False)

    elif granularity == "age":
        df['age'] = df['r_9']/8760
        df = df.dropna(subset=['age'])
        df['age'] = df['age'].astype('int')
        sub_col_name = 'node_id'
        col_name = "age"
        time = 1800
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_node_age.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_rack_age.csv", index=False)

    elif granularity == "capacity":
        dict_ = {'A3': '480GB',
                'A6': '800GB',
                'A4': '240GB',
                'A1': '480GB',
                'A5': '480GB',
                'A2': '800GB',
                'B2': '1920GB',
                'B3': '1920GB',
                'B1': '480GB',
                'C1': '1920GB',
                'C2': '960GB'}
        df['capacity'] = df['model']
        for key, value in dict_.items():
            df['capacity'] = df['capacity'].replace(key, value)
        sub_col_name = 'node_id'
        col_name = "capacity"
        time = 1800
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_node_capacity.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_rack_capacity.csv", index=False)

    elif granularity == 'app':
        apps = ['WSM', 'RM', 'WPS', 'DB', 'SS', 'DAE', 'NAS', 'WS']
        df = df[df['app'].isin(apps)]
        sub_col_name = 'node_id'
        col_name = "app"
        time = 1800
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_node_app.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        df_intra_failures = label_intra_failures_for_factors(
                col_name, sub_col_name, df, time)
        res_df = spatial_bursty_percent(col_name, "burst_glob_id", df_intra_failures)
        res_df.to_csv("results/spatial_rack_app.csv", index=False)

def temporal_correlations(df, granularity):
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(['failure_time'])
    if granularity == 'model':
        sub_col_name = 'node_id'
        col_name = 'model'
        time = [60, 1800, 3600*24, 3600*30]
        name = ['1min', '30min', '1d', '1m']
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_node_model.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_rack_model.csv", index=False)

    elif granularity == "lithography":
        dict_ = {'A3': '20nm',
                'A6': '20nm',
                'A4': '16nm',
                'A1': '20nm',
                'A5': '16nm',
                'A2': '20nm',
                'B2': '19nm',
                'B3': '24nm',
                'B1': '21nm',
                'C1': 'V1',
                'C2': 'V1'}
        df['lithography'] = df['model']
        for key, value in dict_.items():
            df['lithography'] = df['lithography'].replace(key, value)
        sub_col_name = 'node_id'
        col_name = "lithography"
        time = [60, 1800, 3600*24, 3600*30]
        name = ['1min', '30min', '1d', '1m']
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_node_litho.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_rack_litho.csv", index=False)

    elif granularity == "capacity":
        dict_ = {'A3': '480GB',
                'A6': '800GB',
                'A4': '240GB',
                'A1': '480GB',
                'A5': '480GB',
                'A2': '800GB',
                'B2': '1920GB',
                'B3': '1920GB',
                'B1': '480GB',
                'C1': '1920GB',
                'C2': '960GB'}
        df['capacity'] = df['model']
        for key, value in dict_.items():
            df['capacity'] = df['capacity'].replace(key, value)
        sub_col_name = 'node_id'
        col_name = "capacity"
        time = [60, 1800, 3600*24, 3600*30]
        name = ['1min', '30min', '1d', '1m']
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_node_capacity.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_rack_capacity.csv", index=False)

    elif granularity == "age":
        df['age'] = df['r_9']/8760
        df = df.dropna(subset=['age'])
        df['age'] = df['age'].astype('int')
        sub_col_name = 'node_id'
        col_name = "age"
        time = [60, 1800, 3600*24, 3600*30]
        name = ['1min', '30min', '1d', '1m']
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_node_age.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_rack_age.csv", index=False)

    elif granularity == 'app':
        apps = ['WSM', 'RM', 'WPS', 'DB', 'SS', 'DAE', 'NAS', 'WS']
        df = df[df['app'].isin(apps)]
        sub_col_name = 'node_id'
        col_name = "app"
        time = [60, 1800, 3600*24, 3600*30]
        name = ['1min', '30min', '1d', '1m']
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_node_app.csv", index=False)

        sub_col_name = 'rack_id'
        res_df = pd.DataFrame()
        for i in range(4):
            df_intra_failures = find_intra_failures_for_factors(
                col_name, sub_col_name, df, time[i])
            df_intra_failures['time'] = name[i]
            res_df = pd.concat([res_df, df_intra_failures], axis=0)
        res_df.to_csv("results/temporal_rack_app.csv", index=False)

def srcc(df_healthy, df_failures, fname):
    attrs = [
        'failure', 'r_5', 'r_183', 'r_184', 'r_187', 'r_195', 'n_195', 'r_197',
        'r_199', 'r_program', 'r_erase', 'n_blocks', 'n_wearout', 'r_241',
        'r_242', 'r_9', 'r_12', 'r_174', 'n_175'
    ]
    df = pd.concat([df_healthy, df_failures], axis=0)
    df[attrs].corr(method='spearman')[['failure']].to_csv(fname)

def handle_healthy(prefix_dir):
    # 20191231.csv also includes the failures that occur on 2019-12-31.
    df_all = pd.read_csv(prefix_dir + "20191231.csv")
    df_all = df_all.merge(df[['model', 'disk_id', 'failure_time']], how='left', on=['model', 'disk_id'])
    df_all = SMARTConverter(df_all).df
    df_healthy = df_all[df_all['failure_time'].isnull()]
    return df_healthy

def get_avg_num_of_ssds(df, col, sub_col):
    # col is model or app; sub_col is node_id or rack_id
    df_mean = pd.DataFrame()
    if col == 'app':
        apps = ['WSM', 'RM', 'WPS', 'DB', 'SS', 'DAE', 'NAS', 'WS']
        df = df[df['app'].isin(apps)]

    df_mean = df.groupby([col, sub_col])['disk_id'].count().reset_index().groupby([
        col])['disk_id'].mean().reset_index().rename(columns={'disk_id': 'mean'})
    df_std = df.groupby([col, sub_col])['disk_id'].count().reset_index().groupby([
        col])['disk_id'].std().reset_index().rename(columns={'disk_id': 'std'})
    df_n = df.groupby([col, sub_col])['disk_id'].count().reset_index().groupby([
        col])['disk_id'].count().reset_index().rename(columns={'disk_id': 'n'})
    df_mean = df_mean.merge(df_std, on=[col])
    df_mean = df_mean.merge(df_n, on=[col])
    # compute 95% confidence interval
    df_mean['conf'] = 1.96 * df_mean['std'] / np.sqrt(df_mean['n'])
    df_mean[[col, 'mean', 'conf']].to_csv("results/avg_num_" + col + "_" + sub_col[0:4] + ".csv", index=False)

def check_machine_room_id(df):
    # check machine room id for intra-rack failures of A2 and B3
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(['failure_time'])
    sub_col_name = 'rack_id'
    col_name = 'model'
    time = 1800
    df = df[(df['model'] == 'A2') | (df['model'] == 'B3')]
    df = label_intra_failures_for_factors(col_name, sub_col_name, df, time)
    grouped = df.groupby(['model'])
    for col, group in grouped:
        (intra_group, non_intra_group) = separate_failures(group)
        df1 = intra_group.groupby(['machine_room_id', 'burst_glob_id'])[
                'disk_id'].count().sort_values().reset_index().rename(columns={'disk_id': 'failure_group_size'})
        if col == 'A2':
            room_id = df1[df1['failure_group_size'] > 20]['machine_room_id'].unique()
            assert(len(room_id) == 1)
            percent = df1[df1['failure_group_size'] > 20]['failure_group_size'].sum() / group.shape[0]
            print("The room (%d) has %lf of intra-rack failures for A2 with the failure group sizes larger than 20." % (room_id[0], percent))
        else:
            room_id = df1[df1['failure_group_size'] > 30]['machine_room_id'].unique()
            assert(len(room_id) == 1)
            percent = df1[df1['failure_group_size'] > 30]['failure_group_size'].sum() / group.shape[0]
            print("The room (%d) has %lf of intra-rack failures for B3 with the failure group sizes larger than 30." % (room_id[0], percent))

def check_rated_life_used(df):
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(['failure_time'])
    df['age'] = df['r_9']/8760
    df = df.dropna(subset=['age'])
    df['age'] = df['age'].astype('int')
    sub_col_name = 'node_id'
    col_name = "age"
    time = 1800
    node_df = label_intra_failures_for_factors(
            col_name, sub_col_name, df, time)
    grouped = node_df.groupby([col_name])
    intra_df = pd.DataFrame()
    for col, group in grouped:
        (intra_group, non_intra_group) = separate_failures(group)
        intra_df = pd.concat([intra_df, intra_group], axis=0)
    (100 - intra_df.groupby(['age'])['n_wearout'].mean()).to_csv("results/intra_node_age_rated_life_used.csv")

    sub_col_name = 'rack_id'
    rack_df = label_intra_failures_for_factors(
        col_name, sub_col_name, df, time)
    grouped = rack_df.groupby([col_name])
    intra_df = pd.DataFrame()
    for col, group in grouped:
        (intra_group, non_intra_group) = separate_failures(group)
        intra_df = pd.concat([intra_df, intra_group], axis=0)
    (100 - intra_df.groupby(['age'])['n_wearout'].mean()).to_csv("results/intra_rack_age_rated_life_used.csv")

def check_age_for_app(df):
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(['failure_time'])
    apps = ['WSM', 'RM', 'WPS', 'DB', 'SS', 'DAE', 'NAS', 'WS']
    df = df[df['app'].isin(apps)]
    sub_col_name = 'node_id'
    col_name = "app"
    time = 60
    node_df = label_intra_failures_for_factors(
        col_name, sub_col_name, df, time)
    grouped = node_df.groupby(col_name)
    intra_df = pd.DataFrame()
    for col, group in grouped:
        (intra_group, non_intra_group) = separate_failures(group)
        intra_df = pd.concat([intra_df, intra_group], axis=0)
    (intra_df.groupby([col_name])['r_9'].mean()/8760).to_csv("results/check_node_app_age.csv")

    sub_col_name = 'rack_id'
    rack_df = label_intra_failures_for_factors(
        col_name, sub_col_name, df, time)
    grouped = rack_df.groupby(col_name)
    intra_df = pd.DataFrame()
    for col, group in grouped:
        (intra_group, non_intra_group) = separate_failures(group)
        intra_df = pd.concat([intra_df, intra_group], axis=0)
    (intra_df.groupby([col_name])['r_9'].mean()/8760).to_csv("results/check_rack_app_age.csv")

def check_writes_percentage(df, df_topo):
    apps = ['WSM', 'RM', 'WPS', 'DB', 'SS', 'DAE', 'NAS', 'WS']
    df_topo = df_topo[df_topo['app'].isin(apps)]
    df = df.merge(df_topo[['model', 'disk_id', 'app']], how='left', on=['model', 'disk_id'])
    df = df[df['app'].isin(apps)]
    df = df[['disk_id', 'app', 'r_241', 'r_242']]
    df = df.dropna(subset=['r_241', 'r_242'], how='any')
    df['wr'] = df['r_241'] / (df['r_242']+df['r_241'])
    res_df = df.groupby(['app'])['wr'].mean().reset_index().rename(columns={'wr': 'mean'})
    df_std = df.groupby(['app'])['wr'].std().reset_index().rename(columns={'wr': 'std'})
    df_n = df.groupby(['app'])['wr'].count().reset_index().rename(columns={'wr': 'n'})
    res_df = res_df.merge(df_std, on=['app'])
    res_df = res_df.merge(df_n, on=['app'])
    # compute 95% confidence interval
    res_df['conf'] = 1.96 * res_df['std'] / np.sqrt(res_df['n'])
    res_df[['app', 'mean', 'conf']].to_csv("results/app_writes_percentage.csv", index=False)

def compute_conditional_prob(df):
    # aggregate failure group size
    df1 = df.groupby(['burst_glob_id'])['disk_id'].count().reset_index().rename(
            columns={'disk_id': 'failure_group_size'}) 
    # aggregate num groups
    df2 = df1.groupby(['failure_group_size'])['burst_glob_id'].count().reset_index().rename(
            columns={'burst_glob_id': 'num_groups'})
    # get number of failures for each failure group size
    df2['num_failures'] = df2['failure_group_size'] * df2['num_groups']
    res_df = pd.DataFrame()
    for i in range(2, df2['failure_group_size'].max()):
        # p(x+1)/ (p(x) + p(x+1))
        try:
            prob = (df2[df2['failure_group_size'] == (i+1)]['num_groups'].item() / \
                        (df2[df2['failure_group_size'] == (i+1)]['num_groups'].item() + \
                        df2[df2['failure_group_size'] == i]['num_groups'].item()))
        except Exception as e:
            prob = np.nan
            print(e)
        res_df = res_df.append({'failure_group_size': i, 'cond_prob': prob}, ignore_index=True)
    return res_df

def check_homogeneous_component(df, level):
    if level == 'node':
        col_name = 'node_id'
    elif level == 'rack':
        col_name = 'rack_id'

    df1 = df.groupby([col_name])['disk_id', 'model'].nunique()
    homo = df1[(df1['disk_id'] > 1) & (df1['model'] == 1)]
    only_one_ssd = df1[df1['disk_id'] == 1]

    if level == 'node':
        print("total nodes = %d" % (df1.shape[0]))
        print("homo nodes = %d, percentage of all nodes = %lf, percentage of nodes with more than one SSD = %lf" % 
                (homo.shape[0], homo.shape[0]/df1.shape[0], homo.shape[0] / (df1.shape[0] - only_one_ssd.shape[0])))
        print("only one ssd = %d, percentage = %lf" % (only_one_ssd.shape[0], only_one_ssd.shape[0]/df1.shape[0]))
    elif level == 'rack':
        print("total racks = %d" % (df1.shape[0]))
        print("homo racks = %d, percentage of all racks = %lf, percentage of racks with more than one SSD = %lf" % 
                (homo.shape[0], homo.shape[0]/df1.shape[0], homo.shape[0] / (df1.shape[0] - only_one_ssd.shape[0])))
        print("only one ssd = %d, percentage = %lf" % (only_one_ssd.shape[0], only_one_ssd.shape[0]/df1.shape[0]))


if __name__ == '__main__':
    prefix_dir = sys.argv[1]
    try:
        os.mkdir("results/")
    except Exception as e:
        print(e)
    df = pd.read_csv(prefix_dir + "ssd_failure_tag.csv")
    df_topo = pd.read_csv(prefix_dir + "location_info_of_ssd.csv")
    df_all = pd.read_csv(prefix_dir + "20191231.csv")

    # Section 4.1 - Finding 1
    get_intra_failures(df)

    # Section 4.1 - Finding 2
    df_intra_node = pd.read_csv("results/intra_node_failures_30mins.csv")
    res_df = compute_conditional_prob(df_intra_node)
    res_df.to_csv("results/cond_prob_node.csv", index=False)
    df_intra_rack = pd.read_csv("results/intra_rack_failures_30mins.csv")
    res_df = compute_conditional_prob(df_intra_rack)
    res_df.to_csv("results/cond_prob_rack.csv", index=False)

    # Section 4.1 - Finding 3
    get_intra_failures_diff_thresholds(df)

    # Sections 4.2 & 4.4 
    # For Figures 6, 8, 10, 12, and 17, the following results are cumulative. 
    # That is, the failure time intervals are [0, 1min], [0, 30mins], [0, 1day], [0, 1month]
    # Finding 4, Figures 4 & 6
    spatial_correlations(df, "model") 
    temporal_correlations(df, "model")
    # Finding 6, Figures 7 & 8
    spatial_correlations(df, "lithography") 
    temporal_correlations(df, "lithography") 
    # Finding 7, Figures 9 & 10
    spatial_correlations(df, "age") 
    temporal_correlations(df, "age")
    # Finding 8, Figures 11 & 12
    spatial_correlations(df, "capacity") 
    temporal_correlations(df, "capacity") 
    # Finding 11, Figures 15 & 17
    spatial_correlations(df, "app")
    temporal_correlations(df, "app")

    # Section 4.2 - Finding 4, Figure 5
    get_avg_num_of_ssds(df_topo, 'model', 'node_id')
    get_avg_num_of_ssds(df_topo, 'model', 'rack_id')
    get_avg_num_of_ssds(df_topo, 'app', 'node_id')
    get_avg_num_of_ssds(df_topo, 'app', 'rack_id')

    # Section 4.2 - Finding 4 - check the machine room of intra-rack failures for A2 and B3 
    check_machine_room_id(df)

    # Section 4.2 - Finding 7 - check the rated life used for different age groups of intra-node/intra-rack failures
    check_rated_life_used(df)

    # Section 4.4 - Finding 10 - check the percentage of writes
    check_writes_percentage(df_all, df_topo)

    # Section 4.4 - Finding 12 - check the ages of intra-node/intra-rack failures with a threshold of 1 min.
    check_age_for_app(df)

    # Section 4.3 - Finding 13
    # need to run Finding 1 first
    df_intra_node = pd.read_csv("results/intra_node_failures_30mins.csv")
    df_intra_rack = pd.read_csv("results/intra_rack_failures_30mins.csv")
    df_healthy = handle_healthy(prefix_dir)
    srcc(df_healthy, df_intra_node, "results/srcc_node.csv")
    srcc(df_healthy, df_intra_rack, "results/srcc_rack.csv")

    # Section 2.1 - check homogeneous nodes and racks
    check_homogeneous_component(df_topo, 'node')
    check_homogeneous_component(df_topo, 'rack')
