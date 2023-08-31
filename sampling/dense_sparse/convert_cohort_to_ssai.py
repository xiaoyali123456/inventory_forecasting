import pandas as pd

config_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/ssai_configuration/ssai_configuration.json'
output_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/forecast_information/100/forecast_information.json'
df = pd.read_json(output_path)

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

backward = {v: k for k, v in forward.items()}
config = pd.read_json(config_path)
config['basic'] = config.tagType.map(lambda x: forward.get(x, x))
basic = [x for x in config['basic'] if x in df.columns]

df2 = df.copy()
for i in basic:
    if backward[i] in config.tagType.tolist():
        row = config[config.tagType == backward[i]].iloc[0]
        meta = row.metaData
        if meta['type'] == 'map':
            for x in row.acceptableValues:
                meta[x] = x
            df2[i] = df2[i].map(lambda x: row['prefix'] + meta[x] if x in meta else row.defaultValue)
        elif meta['type'] == 'gender':
            tmp = {'f': 'G_F', 'm': 'G_M'}
            df2[i] = df2[i].map(lambda x: tmp.get(x, 'G_U'))
df2.drop(columns='reach').drop_duplicates()
