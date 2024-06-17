
import pickle
import pandas as pd
import sys
import sklearn

# saved model was from sklearn 1.5.0; use same version to predict.
print("sklearn version : ", sklearn.__version__)
del sklearn



taxi_type = 'yellow'
categorical = ['PULocationID', 'DOLocationID']



def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df



def load_model():
    with open('model.bin', 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model



def run():
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/{taxi_type}/{year:04d}-{month:02d}.parquet'

    print("[INFO] Loading model and data vectorizer...")
    dv, model = load_model()

    print(f"[INFO] Reading data for {taxi_type} taxi for {year:04d}/{month:02d}...")
    df = read_data(input_file)
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)

    print(f"[INFO] Generating predictions in {output_file}...")
    y_pred = model.predict(X_val)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df['y_pred'] = y_pred

    df_result = df[['ride_id', 'y_pred']].copy()
    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )

    print("[INFO] Mean predicted duration : ", df['y_pred'].mean())



if __name__ == "__main__":
    run()