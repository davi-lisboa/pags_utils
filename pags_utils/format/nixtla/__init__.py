


# fmt_nixtla
def fmt_nixtla(df, date_col_name = 'ds', vars_col_name = 'unique_id', value_name = 'y', show_var_names = False, label_encode = False):
    import pandas as pd
    import datetime as dt
    from sklearn.preprocessing import LabelEncoder

    if type(df.index) != pd.core.indexes.datetimes.DatetimeIndex:
        raise TypeError("DataFrame Index must be datetime-like")


    fmt_df = df.copy()
    fmt_df.index.name = date_col_name

    fmt_df = pd.melt(frame = fmt_df.reset_index(),
                    id_vars = date_col_name,
                    var_name = vars_col_name,
                    value_name = value_name)

    if label_encode == True:
        label_encoder = LabelEncoder()
        fmt_df[vars_col_name] = label_encoder.fit_transform(fmt_df[vars_col_name])

    if show_var_names == True:
        fmt_df['id_names'] = label_encoder.inverse_transform(fmt_df[vars_col_name])

    return fmt_df

# df_metrics
def df_metrics(df, metrics = ['mse', 'rmse', 'mae', 'mape', 'smape'], models: list = None):
    from utilsforecast.losses import mse, rmse, mae, mape, smape
    import pandas as pd

    mse_ = mse(df = df, models = models)
    rmse_ = rmse(df = df, models = models)
    mae_ = mae(df = df, models = models)
    mape_ = mape(df = df, models = models)
    smape_ = smape(df = df, models = models)

    metrics_list = [mse_, rmse_, mae_, mape_, smape_]

    df_metrics = pd.concat(metrics_list).round(4)
    df_metrics.index = metrics

    return df_metrics