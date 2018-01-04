# Fix detail files (correct headers)
# Write control file
# Encrypt, transfer detail CSV(s)

import datetime
import subprocess
import pysftp
import os
import time
import pandas as pd
import csv

home_path = "/home/ubuntu/"
input_path = home_path + "sharded_input/"

header_name = [file for file in os.listdir("/home/ubuntu/") if file.startswith("AUTOGRAPH_PROFILE_HEADER")][0]


def fix_file(fn):
    # Remove the period from the extract file headers, rewrite
    os.chdir(input_path)
    df = pd.read_csv(fn, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    df.rename(columns={"Created.timestamp": "Created timestamp", "Last.Update": "Last Update"}, inplace=True)
    length = str(len(df["AG_ID"].unique()) + 1)
    df.to_csv(home_path + fn, index=False, sep=";", quotechar='"', quoting=csv.QUOTE_ALL)
    os.remove(fn)
    del df

    return [fn.replace(".csv", ".enc"), length]


# Write the control file
control_filename = "AUTOGRAPH_" + str(datetime.date.today() - datetime.timedelta(1)).replace("-",
                                                                                             "") + "_000000_001.txt"

fopen = open(home_path + control_filename, "w")

filenames = []

for file in os.listdir(input_path):
    if file.startswith("AUTOGRAPH"):
        fopen.write(";".join(fix_file(file)))
        fopen.write("\n")
        filenames.append(file)
    else:
        os.remove(input_path + file)

fopen.write(";".join([header_name, "1399"]))
fopen.close()

print "Control file written!"

print "Opening VPN connection!"

# Connect to OpenVPN
vpn_connect = subprocess.call(["sudo", "openvpn", "--config",
                               home_path + "Spectrum/ThreeUK/File_Transfer/prod_vpn/prodh3g.ovpn",
                               "--daemon"])

if vpn_connect != 0:
    print "VPN connection failed!"
    exit()

time.sleep(10)


# Encrypt the detail files
def encrypt_file(filename):
    encoded_file = filename.replace(".csv", ".enc")
    encrypt_file = subprocess.call(["openssl", "enc", "-e",
                                    "-aes-256-cbc", "-in", home_path + filename,
                                    "-out", home_path + encoded_file,
                                    "-pass",
                                    "file:" + home_path + "Spectrum/ThreeUK/File_Transfer/privkey_AUTOGRAPH_prd.pem"])

    if encrypt_file != 0:
        print "encryption for " + filename + " failed...for some reason"
        exit()

    return encoded_file


# Get list of encoded file names
encoded_extracts = [encrypt_file(fn) for fn in filenames]

# Add to master list of files to transfer
files_to_transfer = [header_name, control_filename] + encoded_extracts

# Add .tmp extension
print "Renaming files"

for fname in files_to_transfer:
    print fname
    os.rename(home_path + fname, home_path + fname + ".tmp")

print "Connecting to SFTPP"

# Transfer the files
sftp = pysftp.Connection('191.0.0.8', username='autograph', password='ggK2t.')
for file in files_to_transfer:
    sftp.put(home_path + file + ".tmp", preserve_mtime=True)
    sftp.rename(file + ".tmp", file)
    print file, "transferred successfully!"

sftp.close()

# Rename (eventually delete) files from .tmp
print "Removing .tmp extension"

for fn in files_to_transfer:
    print fn
    os.rename(home_path + fn + ".tmp", home_path + fn)

# Kill OpenVPN
print "File transferred! Killing VPN connection"

vpn_disconnect = subprocess.call(["sudo", "killall", "openvpn"])

if vpn_disconnect != 0:
    print "Couldn't kill VPN..."
else:
    print "Finished transfer!"

time.sleep(10)
for ee in encoded_extracts:
    os.remove(home_path + ee)
os.remove(home_path + control_filename)
