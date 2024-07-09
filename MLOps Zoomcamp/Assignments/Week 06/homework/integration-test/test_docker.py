from datetime import datetime
import pandas as pd
import os


# Set up the AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'dummy'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'dummy'

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_prepare_data():

    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df_input = pd.DataFrame(data, columns=columns)

    INPUT_FILE = "s3://nyc-duration/in/2023-01.parquet"
    S3_ENDPOINT_URL = "http://localhost:4566"
    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
    }


    df_input.to_parquet(
        INPUT_FILE,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
        )


test_prepare_data()


def test_batch_save_data():

    # Run batch.py script
    os.system('python batch.py 2023 01')

    filename = "s3://nyc-duration/out/2023-01.parquet"
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', "http://localhost:4566")
    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
    }

    df = pd.read_parquet(filename, storage_options=options)
    print(df)


test_batch_save_data()
