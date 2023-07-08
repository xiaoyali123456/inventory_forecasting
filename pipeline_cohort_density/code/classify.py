import pandas as pd
import s3fs
import json

input_path = 's3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/sampling/dense_sparse/qdata_v4/'
config_path = 's3://hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/ssai_configuration/ssai_configuration.json'
output_path = 'hotstar-ads-data-internal-us-east-1-prod/data/workflows/adtech-segment-management/prod/forecast_information/forecast_information.json'

def classify(rank, threshold1=0.02, threshold2=0.5):
    if rank <= threshold1:
        return 'super_dense'
    elif rank <= threshold1 + threshold2:
        return 'dense'
    else:
        return 'sparse'

def transform_config(config):
    forward = {
        'AgeTag': 'age',
        'AffluenceTag': 'device',
        'GenderTag': 'gender',
        'StateTags': 'state',
        'CityTags': 'city',
        'AsnTag': 'asn',
        'PlatformTag': 'platform',
        'NCCSAffluenceTag': 'nccs',
        'language': 'language',
    }
    backward = {v: k for k,v in forward.items()}
    config['basic'] = config.tagType.map(lambda x: forward.get(x, x))


def main(cd):
    df = pd.read_parquet(input_path)
    df = df.loc[df.is_cricket == True]
    config = pd.read_json(config_path)
    transform_config(config)


    with s3fs.S3FileSystem().open(output_path, 'w') as fp:
        json.dump(obj, fp)

if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)