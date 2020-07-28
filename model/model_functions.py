import pandas as pd

def upsample(df):
    from sklearn.utils import resample

    # check which class is larger
    if len(df[df['type'] == 'FAKE']) > len(df[df['type'] == 'REAL']):
        df_majority = df[df.type == 'FAKE']
        df_minority = df[df.type == 'REAL']
    else:
        df_majority = df[df.type == 'REAL']
        df_minority = df[df.type == 'FAKE']


    # upsample minority class
    df_minority_upsampled = resample(
        df_minority, 
        replace=True,     # sample with replacement
        n_samples=len(df_majority),    # to match majority class
        random_state=123) # reproducible


    # combine majority class with upsampled minority class
    df_upsampled = pd.concat([df_majority, df_minority_upsampled])
    
    return df_upsampled

