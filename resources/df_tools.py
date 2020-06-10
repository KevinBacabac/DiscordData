import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import resources.tz


def count_df(df, interval='D', col='timestamp'):
    series = df[col].value_counts()
    # Set missing dates to 0
    series = series.resample(interval).sum().sort_index()

    count = pd.to_numeric(series.values, downcast='unsigned')
    return pd.DataFrame({col: series.index, 'Count': count})


def count_timestamp(df, localized=False, unix=False, interval='D', col='timestamp'):
    """Count number of events per day within dataframe. Mutates argument."""

    PRUNE = set(df.columns) - {col, }
    df.drop(PRUNE, axis=1, inplace=True)

    print(f"count_timestamp. col = {col}")
    print(df[col])

    if not is_datetime(df[col].dtypes):
        ifilt = df[col].apply(isinstance, args=(int,))
        #print(df.loc[~ifilt])

        sfilt = df[col].apply(isinstance, args=(str,))
        dfilt = ~(ifilt | sfilt)
        #print("dfilt\n", df.loc[dfilt], "\ndone dfilt")

        I = 0

        df['temp_timestamp'] = pd.to_datetime(0, unit='ms')
        df['temp_timestamp'] = df['temp_timestamp'].astype('datetime64[ns]')

        #print("test\n", df.iloc[I])
        df.loc[ifilt, ['temp_timestamp']] = pd.to_datetime(df.loc[ifilt, col], unit='ms')
        print("ifilt\n", df.loc[ifilt, ['temp_timestamp']])
        print(df.iloc[0].dtypes)
        #print("test2\n", df.iloc[I])
        df.loc[sfilt, ['temp_timestamp']] = pd.to_datetime(df.loc[sfilt, col].str.replace('"', ''))
        print("sfilt\n", df.loc[sfilt, ['temp_timestamp']])
        print(df.iloc[0].dtypes)
        #print("test3\n", df.iloc[I])
        df.loc[dfilt, ['temp_timestamp']] = pd.to_datetime(df.loc[dfilt, col])
        print("dfilt\n", df.loc[dfilt, ['temp_timestamp']])
        print(df.iloc[0].dtypes)

        #print(df['temp_timestamp'])
        print("test3")
        df[col] = pd.to_datetime(df['temp_timestamp'], utc=True)
        print("test4")
        del df['temp_timestamp']

    print(df[col].dtypes)

    """
    filt = df[col].apply(isinstance, args=(str,))
    filt2 = df[col].str.startswith('"', na=False)
    print(filt)
    print(filt2)
    temp_df = pd.to_datetime(df[filt & filt2][col].str.replace('"', ''))
    print(temp_df)
    print(type(temp_df))

    df.loc[filt & filt2, col] = temp_df"""

    filt = df[col].apply(isinstance, args=(pd.Timestamp,))
    print(filt)
    df.loc[filt, col] = pd.to_datetime(df[filt][col])

    if interval == 'D':  # Remove time but keep date
        df[col] = df[col].dt.normalize()

    return count_df(df, col=col, interval=interval)
