#!/usr/bin/env python

import os
import shutil
import sys

if len(sys.argv) != 4:
    print(sys.argv[0] + ' FOLDER_A FOLDER_B TARGET')
    sys.exit(1)

# FOLDER_A is also the source, means every file that is extra in
# FOLDER_A is copied to TARGET
(FOLDER_A, FOLDER_B, TARGET) = sys.argv[1:4]

for d in (FOLDER_A, FOLDER_B, TARGET):
    if not os.path.isdir(d):
        print(d + " is not a directory")
        sys.exit(1)


def in_folder(name):
    return [f for f in os.listdir(name) if os.path.isfile(os.path.join(name, f))]


distinct = [f for f in in_folder(FOLDER_A) if f not in in_folder(FOLDER_B)]

if len(distinct) == 0:
    sys.exit(0)

for f in distinct:
    shutil.copy(os.path.join(FOLDER_A, f), TARGET)
