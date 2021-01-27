from GlaciersGEE.main import *
import json
import sys

def main(targets):
	all_config = json.load(open('config/all-params.json'))

	if 'all' in targets:
		id_fp = all_config['id_fp']
		data_dir = all_config['data_dir']
		folder_name = all_config['folder_name']
		delimiter = all_config['delimiter']
		run_pipeline(id_fp, data_dir, folder_name, delim=delimiter)

if __name__ == '__main__':
	targets = sys.argv[1:]
	main(targets)