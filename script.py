import pandas as pd
import re
import uuid
import os


def get_domain(site):
    try:
        return re.search("[a-z]*://[^/]*", site).group(0)
    except:
        return site


def get_vocab(df):
    return df["URL"].unique()


def get_url_time_df(df):
    new_df = df[["URL", "Total(sec)"]].groupby("URL").sum(
    ).sort_values(by=['Total(sec)'], ascending=False)["URL"]
    return new_df


def get_summarized_df(file_name):
    df = pd.read_csv(file_name, delimiter=",", skipinitialspace=True, quotechar='"', usecols=["URL", "Total(sec)"]
                     )
    df["URL"] = df["URL"].apply(get_domain)
    return df[["URL", "Total(sec)"]].groupby("URL").sum().reset_index()


def get_all_urls_df(file_names):
    # returns a dataframe with all urls
    df = pd.DataFrame()
    df['URL'] = ""
    for file_name in file_names:
        new_df = pd.read_csv(file_name, delimiter=",", skipinitialspace=True,
                             quotechar='"', usecols=["URL", "Total(sec)"])
        df = pd.concat([df, new_df], axis=0)
        # add df and new_df together... then get_vocab set df equal to it

    df["URL"] = df["URL"].apply(get_domain)

    return pd.DataFrame(get_vocab(df), columns=['URL'])


def append_to_df(file_names):
    # get all unique rows from all the csvs
    df = get_all_urls_df(file_names)
    # append to a fresh dataframe df
    for file_name in file_names:
        df_new = get_summarized_df(file_name)
        df_new["Total(sec)"] = df_new["Total(sec)"]/3600
        df_new = df_new.rename(columns={"Total(sec)": file_name[10:]})

        df = pd.merge(df, df_new, on="URL").fillna(0)
    return df


file_names = os.listdir()
file_names.remove("script.py")
year = file_names[0][10:]
df = append_to_df(file_names)
f_name = f"{year}-stats-{uuid.uuid4()}.csv"
df.to_csv(f_name)

print("All done!!!")
print("saved as "+f_name)
print("thanks for using")
