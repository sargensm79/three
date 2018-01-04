# Run R file

import subprocess
import datetime
import os

r_file = "3UK_postproc_DEC26.R"


call_r_file = subprocess.call(["nohup", "Rscript", "/home/ubuntu/THREE/" + r_file])

if call_r_file != 0:
    print "R process failed at " + str(datetime.datetime.now())
    exit()

print "Extract file(s) written!"

os.remove("/home/ubuntu/nohup.out")
