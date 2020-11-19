import csv
import os
import sys
import ee
import pandas as pd
from datetime import date
from GlaciersGEE.drive import *

#Reorganize landsat download function
def ee_download(
    glacierID, 
    glacierObject, 
    drive_service,
    folder_name,
    begDate='1984-01-01', 
    endDate='2020-01-01', 
    cloud_tol=20, 
    landsat=True, 
    dem=True,
    gmted=True):
    '''
    Download images from GEE
    '''
    # Initial earth engine connection, key much be on your computer, thus 
    # you must once in terminal run ee.Authenticate() for any new computer 
    # that you are using for implementation of this google earth engine communication
    try:
        ee.Initialize()
        print('The Earth Engine package initialized successfully!')
    except ee.EEException:
        print('The Earth Engine package failed to initialize!')

    def cloudscore(image):
        '''
        Inner function for computing cloud score such that we can remove 
        bad images from the landsat collections we download.
        Implementation in javascript can be found of Google Earth Engine 
        website under (landsat algorithms), translation to python by KH.
        Further help from Nicholas Clinton at 
        https://gis.stackexchange.com/questions/252685/filter-landsat-images-base-on-cloud-cover-over-a-region-of-interest
        '''
        cloud = ee.Algorithms.Landsat.simpleCloudScore(image).select('cloud')
        cloudiness = cloud.reduceRegion(ee.Reducer.mean(),
                                        geometry=region,
                                        scale=30)
        image = image.set(cloudiness)
        return image

    # Our glacier region can be found in the imported dictionary as an 
    # argument under bounding box (list of lists of coordinates).
    # We must create a gee polygon in order to use that to clip the images
    region = ee.Geometry.Polygon(glacierObject['bbox'])
    # Dummy request to Earth engine to compute glacier object values and send to toDrive
    # Creates image collections for later batch export

    if gmted:
        get_parent_folder_id(drive_service, name=str(glacierObject['glac_id']))
        # DEM 60 degrees
        # Create image from GEE and clip to our glacier region
        DEM = ee.Image("USGS/GMTED2010")
        DEM = DEM.clip(region)
        #  export this image to drive in the glimsID folder specified in input object
        task = ee.batch.Export.image.toDrive(
            image = DEM.clip(region),
            scale = 30,
            region = region.bounds().getInfo()['coordinates'],
            folder = str(glacierObject['glac_id']),
            fileNamePrefix = 'USGS_GMTED2010')
        task.start()
        print("gmted sent to drive")
        return 

    # Landsat 8 image collection
    print("Getting Landsat 8 collection")
    if date.fromisoformat(endDate) > date.fromisoformat("2013-01-01"):
        # First filter the collection of images by date and region of glacier
        #  Landsat 8 starts on 01-01-13

        colL8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_TOA')
        colL8 = colL8.filterDate('2013-01-01',endDate)
        colL8 = colL8.filterBounds(region)
        count = colL8.size()

        # Filter out cloudiest images based on tolerance set as parameter
        # We need bands 2-7
        withCloudiness = colL8.map(algorithm=cloudscore)

        filteredCollectionL8 = withCloudiness.filter(ee.Filter.lt('cloud', cloud_tol))
        filteredCollectionL8 = filteredCollectionL8.select(['B2', 'B3', 'B4', 'B5', 'B6', 'B10'])

        # In order to collect dates for object and pushing images to drive 
        # make our collection a list thus we can loop over and the size of the collection
        collectionListL8 = filteredCollectionL8.toList(colL8.size())
        collectionSizeL8 = collectionListL8.size().getInfo()
        # Landsat 7 Image collection
        # Same for the other landsats based on dates of start

    print("Getting Landsat 7 collection")
    if date.fromisoformat("1999-01-01") > date.fromisoformat(begDate):
        colL7 = ee.ImageCollection('LANDSAT/LE07/C01/T1_TOA')
        colL7 = colL7.filterDate(begDate,endDate)
        colL7 = colL7.filterBounds(region)
        count = colL7.size()


    else:
        colL7 = ee.ImageCollection('LANDSAT/LE07/C01/T1_TOA')
        colL7 = colL7.filterDate('1999-01-01',endDate)
        colL7 = colL7.filterBounds(region)
        count = colL7.size()

    withCloudiness = colL7.map(algorithm=cloudscore)

    filteredCollectionL7 = withCloudiness.filter(ee.Filter.lt('cloud', cloud_tol))
    filteredCollectionL7 = filteredCollectionL7.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6_VCID_1'])
    collectionListL7 = filteredCollectionL7.toList(colL7.size())
    collectionSizeL7 = filteredCollectionL7.size().getInfo()

    # Landsat 5 image collection
    print("Getting landsat 5 collection")
    if date.fromisoformat(endDate) < date.fromisoformat('2012-05-01'):
        colL5 = ee.ImageCollection('LANDSAT/LT05/C01/T1_TOA')\
                .filterDate(begDate,endDate)\
                .filterBounds(region)

        count = colL5.size()
    else:

        colL5 = ee.ImageCollection('LANDSAT/LT05/C01/T1_TOA')\
                .filterDate(begDate,'2012-05-01')\
                .filterBounds(region)

        count = colL5.size()

    withCloudiness = colL5.map(algorithm=cloudscore)

    filteredCollectionL5 = withCloudiness.filter(ee.Filter.lt('cloud', cloud_tol))
    filteredCollectionL5 = filteredCollectionL5.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6'])
    collectionListL5 = filteredCollectionL5.toList(colL5.size())
    collectionSizeL5 = filteredCollectionL5.size().getInfo()

    # For each image collection we need the list of dates from that image as well 
    # as the sensor(to be added) it comes from, the following code extracts that 
    # information and adds to glacierObject.
    # Names of image attributes found at : https://developers.google.com/earth-engine/datasets/catalog/landsat
    L8Dates = []
    L7Dates = []
    L5Dates = []
    print("starting date collection")
    # Loop over image collection lists made above and then get the date acquired and append to list
    for i in range(collectionSizeL8):
        image = ee.Image(collectionListL8.get(i)).clip(region)
        date_acquired = image.get("DATE_ACQUIRED")
        L8Dates.append(str(date_acquired.getInfo()))
    print("L8 date collection complete")
    for i in range(collectionSizeL7):
        image = ee.Image(collectionListL7.get(i)).clip(region)
        date_acquired = image.get("DATE_ACQUIRED")
        L7Dates.append(str(date_acquired.getInfo()))
    print("L7 date collection complete")
    for i in range(collectionSizeL5):
        image = ee.Image(collectionListL5.get(i)).clip(region)
        date_acquired = image.get("DATE_ACQUIRED")
        L5Dates.append(str(date_acquired.getInfo()))
    print("l5 date collection complete")

    # Add date lists to the glacier object
    glacierObject['L8Dates'] = L8Dates
    glacierObject['L7Dates'] = L7Dates
    glacierObject['L5Dates'] = L5Dates
    # The title of the folder where the glacier object and images will be located
    glacierObject['fileaddress'] = str(glacierObject['glac_id'])
    glacierObject['drivefile_id'] = str("NA")

    # CSV implementation for making the dictionary a csv and then writing it 
    # such that it can be used to push to drive location.
    # Help found from code at: https://www.tutorialspoint.com/How-to-save-a-Python-Dictionary-to-CSV-file

    print("Making google drive glacier object")
    # Google Drive API implementation
    # Getting google drive API to work found at: 
    # https://medium.com/@annissouames99/how-to-upload-files-automatically-to-drive-with-python-ee19bb13dda
    # Uploading files code found at: https://developers.google.com/drive/api/v3/manage-uploads
    # However that is where the help from developers group stopped helping, 
    # do not use it for creating folders and such, their solutions don't work with python API
    # Or are outdated
    # g_login = GoogleAuth()
    # g_login.LocalWebserverAuth()
    # drive = GoogleDrive(g_login)

    # Helpful links:
    # https://github.com/gsuitedevs/PyDrive/issues/72
    # https://towardsdatascience.com/how-to-manage-files-in-google-drive-with-python-d26471d91ecd
    nameforfile = str(glacierObject['glac_id']) + ".csv"
    # Assign mime type such that it creates a file in drive
    # folder_metadata = {
    # 'title': str(glacierObject['glac_id']),
    # 'mimeType': 'application/vnd.google-apps.folder'
    # }
    # folder = drive.CreateFile(folder_metadata)
    # folder.Upload()

    # NEW DRIVE CODE:
    try:
        parentID = get_parent_folder_id(drive_service, name=folder_name)
    except:
        parentID = create_folder(drive_service, folder_name)

    folderid = create_folder(drive_service, str(glacierObject['glac_id']), parentID=parentID)

    # Need the id of the folder because google drive does not work like a normal file system
    # operates on ID's found in metadata
    # folderid = get_parent_folder_id(drive_service, name='glaciers')
    print('Folder ID: %s' % folderid)
    glacierObject["drivefile_id"] = str(folderid)

    print("Creating CSV")
    if os.path.exists("glacierInfo" + ".csv"):
        with open('glacierInfo.csv','a') as f:
            w = csv.DictWriter(f, glacierObject.keys())
            if f.tell() == 0:
                w.writeheader()
                w.writerow(glacierObject)
            else:
                w.writerow(glacierObject)

    else:
        csv_file = "glacierInfo" + ".csv"
        csv_columns = ['GlimsID', 'bbox','L8Dates','L7Dates','L5Dates', 'fileaddress', 'drivefile_id']
        with open(csv_file, "w") as f:
            writer = csv.writer(f)
            writer.writerow(glacierObject)

    # No longer need the following until all glaciers are run
    # Now that we created the folder we must create the file that will 
    # store the glacier object using the csv created above
    # Then upload to drive
    # file = drive.CreateFile({"parents": [{"kind": "drive# fileLink", "id": folderid}]})
    # file.SetContentFile(str(glacierID) + ".csv")
    # file.Upload()

    # file=drive.CreateFile(glacierObject)
    # file.SetContentFile(str(glacierID) + ".csv")
    # file.Upload()

    print("glacier object uploaded to google drive")

    # Now is the part behind the GEE server
    # First the DEM
    if dem:
        # DEM 30 degrees
        # Create image from GEE and clip to our glacier region
        DEM = ee.Image("USGS/SRTMGL1_003")
        DEM = DEM.clip(region)
        #  export this image to drive in the glimsID folder specified in input object
        task = ee.batch.Export.image.toDrive(
            image = DEM.clip(region),
            scale = 30,
            region = region.bounds().getInfo()['coordinates'],
            folder = str(glacierObject['glac_id']),
            fileNamePrefix = 'USGS_SRTMGL1_003')
        task.start()
        print("dem sent to drive")
    if landsat == True:
        # We already created the image collections previously and now in a for loop we
        # batch export the images from the collections with the date as the name
        # similarly for each of the three landsat collections
        for i in range(collectionSizeL8):
            image = ee.Image(collectionListL8.get(i)).clip(region)
            filename = image.get("DATE_ACQUIRED")
            task = ee.batch.Export.image.toDrive(
                image=ee.Image(collectionListL8.get(i)).clip(region),
                scale=30,
                region=region.bounds().getInfo()['coordinates'], 
                folder=str(glacierObject['glac_id']),
                fileNamePrefix=str(filename.getInfo()))
            task.start()
        print(collectionSizeL8)
        print("L8 images sent to drive")
        for i in range(collectionSizeL7):
            image = ee.Image(collectionListL7.get(i)).clip(region)
            filename = image.get("DATE_ACQUIRED")
            task = ee.batch.Export.image.toDrive(
                image=ee.Image(collectionListL7.get(i)).clip(region),
                scale=30,
                region=region.bounds().getInfo()['coordinates'],
                folder=str(glacierObject['glac_id']),
                fileNamePrefix=str(filename.getInfo()))
            task.start()
        print(collectionSizeL7)
        print("L7 images sent to drive")
        for i in range(collectionSizeL5):
            image = ee.Image(collectionListL5.get(i)).clip(region)
            filename = image.get("DATE_ACQUIRED")
            task = ee.batch.Export.image.toDrive(
                image=ee.Image(collectionListL5.get(i)).clip(region),
                scale=30,
                region=region.bounds().getInfo()['coordinates'],
                folder=str(glacierObject['glac_id']),
                fileNamePrefix=str(filename.getInfo()))
            task.start()
        print(collectionSizeL5)
        print("L5 images sent to drive")    

# Retrieve images from google drive using file id from previous function
# First have to upload to csv to read and then get file id from csv
def retrieve_images(glims_id):
    g_login = GoogleAuth()
    g_login.LocalWebserverAuth()
    drive = GoogleDrive(g_login)

    data = pd.read_csv("glacierInfo.csv")
    object = data.loc[data['glac_id'] == glims_id]
    # folder_id = object['drivefile_id']
    # file_list = drive.ListFile(
    #     {'q': "folder_id in parents"}).GetList()  # use your own folder ID here
    #
    #
    # for f in file_list:
    #     # 3. Create & download by id.
    #     print('title: %s, id: %s' % (f['title'], f['id']))
    #     fname = f['title']
    #     print('downloading to {}'.format(fname))
    #     f_ = drive.CreateFile({'id': f['id']})
    #     f_.GetContentFile(fname)

    # Make a zip file instead and then download

if __name__ == '__main__':
    dct = dict()
    dct['glac_id'] = "G098570E39226N"
    dct['bbox'] = [[98.54723199999999, 39.210651], 
                          [98.59022299999999, 39.210651], 
                          [98.59022299999999, 39.243147], 
                          [98.54723199999999, 39.243147], 
                          [98.54723199999999, 39.210651]]
    # ee_download("G098570E39226N", dct, cloud_tol = 20, landsat = False, dem = False, begDate = "2000-01-01", endDate = "2014-01-01")
    # print(dct.items())
    retrieve_images("G098570E39226N")

    # McCall Glacier
    # dct['glac_id'] = "G216152E69302N"
    # dct['bbox'] = [[-143.860976, 69.276937],[-143.779992, 69.276937],[-143.779992, 69.334028],[-143.860976, 69.334028]]
    # ee_download("G216152E69302N", dct, cloud_tol = 20, landsat = False, dem = True)