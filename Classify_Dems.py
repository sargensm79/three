# Get demographics for each user from CSV extract
# Don't include empty demographics
# (I imagine they'll get filled when the files are bigger...don't want to make NULL significant)

import os
import csv
import pandas as pd
import mysql.connector

os.chdir("/home/ubuntu/Spectrum/ThreeUK/")

extract_filename = [file for file in os.listdir("/home/ubuntu/")
                    if file.startswith("AUTOGRAPH_2017") and file.endswith(".csv")][0]

df = pd.read_csv("/home/ubuntu/" + extract_filename, quotechar='"', quoting=csv.QUOTE_ALL, sep=";")
cols = ["AG_ID"] + [col for col in df.columns.tolist() if col.startswith("D")]
df = df[cols]

for col in df:
    if df[col].isnull().values.any():
        del df[col]

df = pd.melt(df, id_vars="AG_ID", value_vars=cols[1:], var_name="Variable")

headers = pd.read_excel("3UK_postproc_Join_Keys.xlsx")

df = pd.merge(df, headers, how="inner", left_on="Variable", right_on="Variable")

dems = df[["AG_ID", "Category", "Subcategory", "value"]]

# Get subcategory at max value per category
idx = dems.groupby(['AG_ID', 'Category'])['value'].transform(max) == dems['value']

# Trimming out the remaining MAXs
dems = dems[idx]

dems.drop_duplicates(subset=["AG_ID", "Category"], keep="first", inplace=True)
dems = dems.rename(columns={"AG_ID": "userID"})

ids = [[uid] for uid in dems["userID"].unique()]

# I hate that I have to do this, but here we are
# Pop the data into the table so that you can update the values at the row level as needed
# (sqlalchemy only does table replacements)
dems = [[row[1]["userID"], row[1]["Category"], row[1]["Subcategory"]] for row in dems.iterrows()]


# OPEN DB CONNECTION
cnx = mysql.connector.connect(user="root", password="ch33s3h4t31", host="104.199.98.18", database="reporting_viz")

demcursor = cnx.cursor()

demcursor.execute("""set autocommit = 1;""")


demcursor.execute("""create temporary table demstaging like userDemographics;""")

demcursor.executemany("""INSERT INTO demstaging
                        (userID, Category, Subcategory)
                        VALUES
                        (%s, %s, %s);""", dems)

demcursor.execute("""INSERT INTO userDemographics
                    SELECT * FROM demstaging
                    ON DUPLICATE KEY UPDATE userDemographics.Subcategory = demstaging.Subcategory""")

cnx.commit()

print "Demographics updated!"

demcursor.close()

cnx.close()
