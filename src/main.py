from GlaciersGEE.query import *
from GlaciersGEE.gee import *
from GlaciersGEE.drive import *
import ee
from tenacity import retry, wait_exponential

def authenticate():
	# Earth Engine authentication
	# try:
	# 	ee.Initialize()
	# 	print('The Earth Engine package initialized successfully!')
	# except ee.EEException:
	# 	print('The Earth Engine package failed to initialize!')

	# Google Drive API authentication
	service = start_service()
	print('Drive service started')

	return service

def prep_joined(glimsid_list, datadir):
	'''
	Gets a subset of joined to prepare for querying
	:param glimsid_list: list of GLIMS IDs to query
	:param datadir: data directory
	'''
	joined = load_train_set(datadir + 'joined/joined.shp')
	return joined[joined.glac_id.isin(glimsid_list)]

@retry(wait=wait_exponential(multiplier=1, min=2, max=20))
def single_glacier(
	glims_id, 
	subset,
	drive_service, 
	folder_name,
	begDate='1984-01-01', 
    endDate='2020-01-01', 
    cloud_tol=20, 
    landsat=True, 
    dem=True,
	gmted=True):
	'''
	Queries a dictionary of glacier data, requests data
	from GEE
	:param glims_id: GLIMS ID to query
	:param subset: subset training set from prep_joined
	'''
	print('Beginning glacier', glims_id)
	queried = id_query(glims_id, subset)
	ee_download(glims_id, queried, drive_service, folder_name)
	# sends init req to GEE for metadata
	# creates drive location, adds metadata
	# sends request to GEE

def run_pipeline(glims_id_input, datadir, folder_name, delim=None, ee_params=None, pool=False):
	'''
	Runs the data extraction pipeline
	:param glims_id_input: GLIMS IDs to pass through pipeline; either python list or text filepath
	:param datadir: data directory
	:param delim: delimiter to split on if glims_id_input is a text file
	:param ee_params: file containing custom parameters for Earth Engine collection
	'''
	# authenticate first
	drive_service = authenticate()

	if delim:
		# read in list from text file
		with open(glims_id_input) as fh:
			ids_list = fh.read().split(delim)
	else:
		ids_list = glims_id_input

	train_set = prep_joined(ids_list, datadir)
	# run glaciers one at a time
	for glims_id in ids_list:
			single_glacier(glims_id, train_set, drive_service, folder_name)

