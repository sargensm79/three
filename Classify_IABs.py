# Get IAB 2.0 and 1.0 categories over 0 for each profile

import os
import csv
import pandas as pd
import mysql.connector
import sqlalchemy
from Classify_Dems import extract_filenames

os.chdir("/home/ubuntu/")

iab_key = pd.read_excel("/home/ubuntu/Spectrum/ThreeUK/IAB_KEEP.xlsx")

rel_cols = ["AG_ID"] + list(iab_key["Code"])

for extract_filename in extract_filenames:
    df = pd.read_csv(extract_filename, quotechar='"', quoting=csv.QUOTE_ALL, sep=";")
    
    df = df[rel_cols]
    df.set_index("AG_ID", inplace=True)

    for col in df:
        if df[col].isnull().values.all():
            del df[col]

    lst = []

    for count, row in enumerate(df.iterrows()):
        print(count)
        t1 = row[1]
        meet_threshold = t1[t1 >= 0.1]
        for var in list(meet_threshold.index):
            lst.append((meet_threshold.name, var))

    tdf = pd.DataFrame(lst, columns=["userID", "Variable"])

    headers = pd.read_excel("/home/ubuntu/Spectrum/ThreeUK/3UK_postproc_Join_Keys.xlsx")

    iabs = pd.merge(tdf, headers, how="inner", left_on="Variable", right_on="Variable")

    iabs = iabs[["userID", "Category", "Subcategory"]]

    # OPEN DB CONNECTION
    cnx = mysql.connector.connect(user="root", password="ch33s3h4t31", host="104.199.98.18", database="reporting_viz")

    ids = [[uid] for uid in iabs["userID"].unique()]

    iabcursor = cnx.cursor()

    iabcursor.execute("""SET autocommit = 1;""")

    iabcursor.execute("""create temporary table new_iabs (userID varchar(128), primary key (userID));""")

    iabcursor.executemany("""insert into new_iabs 
                        (userID)
                        VALUES
                        (%s);""", ids)

    print "IDs inserted"

    iabcursor.execute("""DELETE FROM userCategories WHERE userID IN (SELECT userID FROM new_iabs);""")

    print "Old IDs deleted"

    cnx.commit()

    iabcursor.close()

    cnx.close()

    sa_cnx = sqlalchemy.create_engine("mysql+mysqlconnector://root:ch33s3h4t31@104.199.98.18:3306/reporting_viz")

    iabs.to_sql(name="userCategories", con=sa_cnx, schema="reporting_viz", if_exists="append", chunksize=5000, index=False)

    print "New IABs inserted!"

