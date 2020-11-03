import argparse
from GlacierProject.main import *

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--datadir', help='data directory', type=str)
parser.add_argument('-l', '--list', help='list of glims ids', type=str)
args = parser.parse_args()

glimsid_list = args.list.split(',')

print(args.datadir)
print(glimsid_list)

# run_pipeline(glimsid_list, args.datadir)