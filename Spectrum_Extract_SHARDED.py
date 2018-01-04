# Starting this for 3UK...
# Incrementally produce spectrum

import pandas as pd
import mysql.connector
import datetime
import csv
import os
import subprocess

# Read setup commands (e.g. get all completeds from past day)
setup_commands = open("/home/ubuntu/Spectrum/ThreeUK/SQL/Spectrum_Setup.sql").read().split(";")

cnx = mysql.connector.connect(user="root", password="ch33s3h4t31", host="104.199.98.18", database="reporting")

cur1 = cnx.cursor()

for sc in setup_commands:
    cur1.execute(sc)

ids = cur1.fetchall()

cur1.close()

cnx.close()

print "Retrieved IDs!"
print len(ids)

# Read headers (use later - force the result sets to conform to them)
final_headers = pd.read_csv("/home/ubuntu/Spectrum/ThreeUK/final_headers.csv")
cols = final_headers.columns.tolist()

# Read SQL commands for getting the brand votes
brand_vote_commands = open("/home/ubuntu/Spectrum/ThreeUK/SQL/Brand_Votes.sql").read().split(";")

temp_cnx = mysql.connector.connect(user="root", password="ch33s3h4t31", host="104.199.98.18", database="reporting")


def get_spectrum(rowset):
    spec_cur = temp_cnx.cursor()

    print "Getting spectrum!"

    spec_cur.executemany("""INSERT INTO kc
                            (foreignKey, profileID, creation, updateID)
                            VALUES
                            (%s, %s, %s, %s);""", rowset)

    temp_cnx.commit()

    spec = []

    spec_cur.execute("""SELECT 
    foreignKey AS 'Foreign Key',
    kc.profileID AS 'autoGraph ID',
    DATE_FORMAT(FROM_UNIXTIME(kc.creation / 1000),
            '%Y-%m-%d %H:%i:%S') AS 'Created timestamp',
    DATE_FORMAT(FROM_UNIXTIME(kc.updateID / 1000),
            '%Y-%m-%d %H:%i:%S') AS 'Last Update',
    CONCAT(parentName, ':', name) AS name,
    value
FROM
    kc
        INNER JOIN
    profileSpectrums ps ON ps.profileID = kc.profileID
        AND ps.updateID = kc.updateID
        WHERE parentName not like 'tags_3uk%'""")

    for row in spec_cur:
        spec.append(row)

    spec = pd.DataFrame(spec,
                        columns=["Foreign Key", "autoGraph ID", "Created timestamp", "Last Update", "name", "value"])
    spec = pd.pivot_table(spec, index=["Foreign Key", "autoGraph ID", "Created timestamp", "Last Update"],
                          columns="name", values="value")
    spec.reset_index(inplace=True)

    spec_cur.close()

    bv_cur = temp_cnx.cursor()

    print "Getting brand votes!"

    for command in brand_vote_commands:
        bv_cur.execute(command)

    brand_votes = []

    for r in bv_cur:
        brand_votes.append(r)

    bv_cur.close()

    brand_votes = pd.DataFrame(brand_votes, columns=["Foreign Key", "name", "vote"])

    brand_votes = pd.pivot_table(brand_votes, index="Foreign Key", columns="name", values="vote")

    brand_votes.reset_index(inplace=True)

    res = pd.merge(spec, brand_votes, how="left", left_on="Foreign Key", right_on="Foreign Key")
    res = final_headers.append(res)
    res = res[cols]

    print "Emptying table!"

    cc = temp_cnx.cursor()
    cc.execute("""TRUNCATE kc;""")
    temp_cnx.commit()
    cc.close()

    return res

def filewriting(lst):
    min_update = ""
    max_update = ""

    filename = "threeuk-profile-spectrum-" + datetime.datetime.strftime(datetime.datetime.now(),
                                                                                 "%Y%m%d%H%M%S") + ".csv"
    chunkSize = 10000
    
    print "Beginning " + filename

    with open("/home/ubuntu/" + filename, "a") as f:
        fw = csv.writer(f, lineterminator = "\n")
        fw.writerow(cols)
        timer = 1
        for i in range(0, len(lst), chunkSize):
            chunk = lst[i:i + chunkSize]
            print(len(chunk), timer)

            spec_partition = get_spectrum(chunk)
            spec_partition.to_csv(f, index=False, header=False)
            timer += 1

            min_u = min(spec_partition["Last Update"])
            if min_update == "":
                min_update = min_u
            elif min_u < min_update:
                min_update = min_u

            max_u = max(spec_partition["Last Update"])
            if max_update == "":
                max_update = max_u
            elif max_u > max_update:
                max_update = max_u
                
    prefix = min_update[:10].replace("-", "") + "000000"
    suffix = max_update.replace("-", "").replace(" ", "").replace(":", "")
    
    new_filename = "threeuk-profile-spectrum-" + prefix + "-" + suffix + ".csv"
    subprocess.call(["mv", "/home/ubuntu/" + filename, "/home/ubuntu/sharded_input/" + new_filename]) 
    
    extract_name = "AUTOGRAPH_" + prefix + "_" + suffix + ".csv"
    
    return [new_filename, extract_name]

filenames = {}

# Split ids into chunks of around 20K profiles
divisor = int(round(len(ids)/20000.0))
segment = int(round(len(ids)/divisor))

for i in range(0, divisor):
    start = segment * i
    end = start + segment
    if i+1 == divisor:
        print i+1, len(ids[start:])
        f = filewriting(ids[start:])
        filenames[f[0]] = f[1]
    else:
        print i+1, len(ids[start:end])
        f = filewriting(ids[start:end])
        filenames[f[0]] = f[1]

# Kill all the "non-temp" tables I had to make
close_table = temp_cnx.cursor()
close_table.execute("""drop table kc;""")
close_table.execute("""drop table key_creation;""")
temp_cnx.commit()
temp_cnx.close()


# Build header file name
overall_prefix = min([val.split("_")[1] for val in filenames.values()])
overall_suffix = max([val.split("_")[2] for val in filenames.values()]).replace(".csv", "")

# Edit header file name
old_header = [file for file in os.listdir("/home/ubuntu/") if file.startswith("AUTOGRAPH_PROFILE_HEADER")][0]
header_name = "AUTOGRAPH_PROFILE_HEADER_" + overall_prefix + "_" + overall_suffix + ".enc"
os.rename("/home/ubuntu/" + old_header, "/home/ubuntu/" + header_name)

# Set R file to run
r_file = "3UK_postproc_DEC26.R"


call_r_file = subprocess.call(["nohup", "Rscript", "/home/ubuntu/THREE/" + r_file])

if call_r_file != 0:
    print "R process failed at " + str(datetime.datetime.now())
    exit()

print "Extract file(s) written!"
"""
file_transfer = subprocess.call(["nohup", "/usr/bin/python", "/home/ubuntu/Spectrum/ThreeUK/File_Transfer.py"])

if file_transfer != 0:
    print "File transfer / rewrite failed at " + str(datetime.datetime.now())
else:
    print "Transfer complete!"
    os.remove("/home/ubuntu/nohup.out")
"""
