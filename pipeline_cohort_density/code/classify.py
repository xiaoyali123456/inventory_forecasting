import sys
import pandas as pd

input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v4/'
config_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/ssai_configuration/ssai_configuration.json'
output_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/forecast_information/forecast_information.json'

def classify(rank, threshold1=0.02, threshold2=0.5):
    if rank <= threshold1:
        return 'super_dense'
    elif rank <= threshold1 + threshold2:
        return 'dense'
    else:
        return 'sparse'

forward = {
    'AgeTag': 'age',
    'AffluenceTag': 'device',
    'GenderTag': 'gender',
    'StateTags': 'state',
    'CityTags': 'city',
    'AsnTag': 'asn',
    'PlatformTag': 'platform',
    'NCCSAffluenceTag': 'nccs',
}
backward = {v: k for k,v in forward.items()}

def main(cd):
    df = pd.read_parquet(input_path)
    df = df.loc[df.is_cricket == True]
    config = pd.read_json(config_path)
    config['basic'] = config.tagType.map(lambda x: forward.get(x, x))
    basic = [x for x in config['basic'] if x in df.columns]
    # cohorts that would be delivered, grouped by `key` of config
    df2 = df.groupby(basic).reach.sum().reset_index()
    for i in basic:
        if backward[i] in config.tagType.tolist():
            row = config[config.tagType == backward[i]].iloc[0]
            meta = row.metaData
            if meta['type'] == 'map':
                for x in row.acceptableValues:
                    meta[x] = x
                if i == 'age':
                    df2[i] = df2[i].map(lambda x: meta[x] if x in meta else row.defaultValue)
                else:
                    df2[i] = df2[i].map(lambda x: x if x in meta else row.defaultValue)
            elif meta['type'] == 'gender':
                df2[i] = df2[i].map(lambda x: x if x in ['f', 'm'] else row.defaultValue)
    df2 = df2.groupby(basic).reach.sum().reset_index()
    # simulate how these cohorts are treated in SSAI, grouped by `value` of config, num(`key`) > num(`value`)
    df3 = df2.reset_index()
    for i in basic:
        if backward[i] in config.tagType.tolist():
            row = config[config.tagType == backward[i]].iloc[0]
            meta = row.metaData
            if meta['type'] == 'map':
                for x in row.acceptableValues:
                    meta[x] = x
                df3[i] = df3[i].map(lambda x: meta[x] if x in meta else row.defaultValue)
            elif meta['type'] == 'gender':
                pass
    df3 = df3.groupby(basic).agg({'reach':sum, 'index':list}).reset_index()
    df3['rank'] = df3.reach.rank(method='first', ascending=False, pct=True)
    df3['density'] = df3['rank'].map(lambda x: classify(x, 0.02, 0.5))
    df3['den'] = df3.density.map(lambda x: {'dense':1,'super_dense':2,'sparse':0}[x])
    # use df3 class for df2
    for k, v in df3.groupby('density').index.sum().items():
        df2.loc[v, 'density'] = k
    df2.to_json(output_path, orient='records')


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)