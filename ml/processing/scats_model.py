import joblib
from sklearn.ensemble import RandomForestRegressor
from vaex.ml.sklearn import Predictor
from joblib import parallel_backend
import vaex.ml
import vaex
import time
import os

dir = os.path.dirname(__file__)
outdir = os.path.join(dir, 'output')
csv = os.path.join(dir, 'output', 'processed_scats.csv')
model_out = os.path.join(outdir, 'scats_model.json')


# we create the scats model which can be used to predict unkown avg_volumes of traffic for a
# lat: radians, lon: radians, dayOfWeek: int, hourOfDay: int
def create_scats_ml_model():

    start = time.time()
    print('starting scats ml modeling')

    # load existing csv into vaex dataframe
    if not os.path.exists(csv+'.hdf5'):
        df = vaex.from_csv(csv, convert=True,
                           copy_index=False, chunk_size=1000000)
        df.export(csv, shuffle=True)
    df = vaex.open(csv+'.hdf5', shuffle=True)
    df = df.sample(frac=1)

    # transform the features into more machine learning friendly vars
    pca_coord = vaex.ml.PCA(
        features=['lat', 'lon'], n_components=2, prefix='pca')
    df = pca_coord.fit_transform(df)

    cycl_transform_hour = vaex.ml.CycleTransformer(features=['hour'], n=24)
    df = cycl_transform_hour.fit_transform(df)

    cycl_transform_dow = vaex.ml.CycleTransformer(features=['dow'], n=7)
    df = cycl_transform_dow.fit_transform(df)

    # split into train and test
    df_train, df_test = df.ml.train_test_split(test_size=0, verbose=False)

    feats = df_train.get_column_names(regex='pca') + \
        df_train.get_column_names(regex='.*_x') + \
        df_train.get_column_names(regex='.*_y')
    target = 'avg_vol'

    print('dataWrangling done, ready to create model, time: {}s'.format(
        round(time.time() - start)))

    # create a randomForestRegression model
    model = RandomForestRegressor(random_state=42,  n_estimators=7*24)
    vaex_model = Predictor(
        features=feats,
        target=target,
        model=model,
        prediction_name='p_avg_vol'
    )

    # here we fit and train the model
    with parallel_backend('threading', n_jobs=8):
        vaex_model.fit(df=df_train)
        print('\n\nmodel created, time: {}s'.format(round(time.time() - start)))

    with parallel_backend('threading', n_jobs=8):
        joblib.dump(value=vaex_model, filename=model_out, compress=3)
        print('model written to output, time: {}s'.format(
            round(time.time() - start)))

    # client.shutdown()
    print('model trained, time: {}s'.format(round(time.time() - start)))
    return


if __name__ == "__main__":
    # execute only if run as a script
    create_scats_ml_model()
