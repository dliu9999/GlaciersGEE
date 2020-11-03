'''
TODO: query.py does _________________-
'''

import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
from shapely.geometry import Point
import os

def open_glims_shp(poly_fp, cols, pt_fp=None, outp=None, chunksize=50000):
    '''
    Open glims shapefile, keeping only most recent observations

    :param poly_fp: filepath to glims_polygons.shp
    :param pt_fp: filepath to glims_points.shp
    :param cols: columns to keep
    :param outp: output filepath folder
    :param chunksize: chunksize for reading in shapefile in fiona
    '''

    # Read in polygon file
    file = fiona.open(poly_fp)
    
    def reader(file, cols, chunksize):
        ''' 
        Read in shapefile, keeping only columns in usecols 

        :param cols: columns to keep
        :param r: portions of file to open
        '''
        out = []
        for k, feature in enumerate(file):
            if (k + 1) % chunksize:
                f = {k: feature[k] for k in ['geometry']}
                f['properties'] = {k: feature['properties'][k] for k in cols}
                out.append(f)
            else:
                yield gpd.GeoDataFrame.from_features(out).drop_duplicates('glac_id', keep='last')
                out = []
                
    counter = 1
    for chunk in reader(file, cols, chunksize):
        try:
            glims = glims.append(chunk, ignore_index=True)
        except NameError:
            glims = chunk
        print('Finished chunk', counter)
        counter += 1
    
    glims = glims.drop(columns=['anlys_time'])
    glims.crs = {'init' :'epsg:4326'}

    # Read in point file and merge

    pts =  gpd.read_file(pt_fp)
    pts['x'] = pts.geometry.x
    pts['y'] = pts.geometry.y
    pts_tomerge = pts[['glacier_id', 'x', 'y']].rename(columns={'glacier_id': 'glac_id'})
    glims = glims.merge(pts_tomerge).drop_duplicates('glac_id', keep='last')

    if outp:
        os.mkdir(outp)
        glims.to_file(outp + '/glims_polys.shp')
    
    return glims

def read_glims_gdf(fp, cols=None, pt_fp=None, outp=None):
    '''
    Read in the glims shapefile
    :param fp: filepath of either glims_gdf.shp or glims_polygons.shp
    :param outp: output filepath folder of glims_gdf.shp if fp == glims_polygons.shp
    '''
    if outp:                                        
        glims_gdf = open_glims_shp(fp, cols, pt_fp=pt_fp, outp=outp)       # opens the raw shp file
    else:
        glims_gdf = gpd.read_file(fp)                            # reads in cleaned shp file
        glims_gdf.crs = {'init' :'epsg:4326'}
    
    return glims_gdf

def read_wgms_gdf(*filepaths, gdf_fp=None, outp=None):
    '''
    Read in the wgms file as a GeoDataFrame
    :param filepaths: filepaths of wanted wgms datasets
    :param gdf_fp: filepath to wgms.shp, allows for direct opening of shapefile
    :param outp: output filepath folder of wgms_gdf
    '''
    if gdf_fp:
        wgms_gdf = gpd.read_file(gdf_fp)
        wgms_gdf.crs = {'init' :'epsg:4326'}
        return wgms_gdf

    wanted_cols = 'POLITICAL_UNIT NAME WGMS_ID LATITUDE LONGITUDE PRIM_CLASSIFIC GLIMS_ID'.split()

    for f in filepaths:
        df = pd.read_csv(f, encoding='latin1')
        df = df[df.columns[df.columns.isin(wanted_cols)]]
        try:
            wgms = wgms.merge(df)
        except NameError:
            wgms = df

    # keep only valley, wanted cols
    wgms = wgms[wgms.PRIM_CLASSIFIC == 5].reset_index(drop=True)

    geometry = [Point(xy) for xy in zip(wgms['LONGITUDE'], wgms['LATITUDE'])]
    crs = {'init': 'epsg:4326'}

    wgms_gdf = gpd.GeoDataFrame(wgms, crs=crs, geometry=geometry).drop(columns=['LONGITUDE', 'LATITUDE'])

    if outp:
        os.mkdir(outp)
        wgms_gdf.to_file(outp + '/wgms.shp')

    return wgms_gdf

def sjoin(glims_gdf=None, wgms_gdf=None, glims_fp=None, wgms_fps=None, outp=None):
    '''
    Spatially join glims and wgms datasets
    :param glims_gdf: glims_gdf output from read_glims_gdf()
    :param wgms_gdf: wgms_gdf output from read_wgms_gdf()
    :param glims_fp: filepath of glims_gdf
    :param wgms_fp: list of filepaths for wgms_gdf (wA and wAA)
    :param outp: output filepath of joined.shp
    '''
    # If input are filepaths not df objects
    if glims_fp:
        glims_gdf = read_glims_gdf(glims_fp)
        wgms_gdf = read_wgms_gdf(*wgms_fps)

    joined = gpd.sjoin(glims_gdf, wgms_gdf, op='intersects')

    if outp:
        os.mkdir(outp)
        joined.to_file(outp + '/joined.shp')
    
    return joined.drop_duplicates('glac_id')

def load_train_set(fp):
    '''
    Load in training set for querying
    :param fp: filepath of joined.shp
    '''
    try:
        return gpd.read_file(fp)
    except:
        # incomplete
        # glims_gdf = read_glims_gdf()                # load glims gdf
        # wgms_gdf = read_wgms_gdf()                  # load wgms gdf
        # joined = sjoin(glims_gdf, wgms_gdf)         # spatial join
        # os.mkdir('../data/joined')
        # joined.to_file(fp)
        # return joined
        return

def id_query(glims_id, subset, scale_fact=1.1):
    '''
    Query info from given ID
    :param id: glims ID to query
    :param subset: subset of joined GeoDataFrame
    :param scalefact: factor to scale bounding box by, default 10%
    '''
    subs = subset[subset.glac_id == glims_id]
    coords = list(zip(*np.asarray(subs.geometry.squeeze().exterior.coords.xy)))
    bbox = list(zip(*np.asarray(subs.envelope.scale(xfact=scale_fact, yfact=scale_fact).squeeze().exterior.coords.xy)))
    to_drop = ['geometry', 'GLIMS_ID', 'WGMS_ID']
    dct = dict(subs.drop(columns=to_drop).squeeze())
    dct['coords'] = coords
    dct['bbox'] = bbox
    return dct

if __name__ == '__main__':
    # example
    glims_fp = '../data/glims_polys/glims_polys.shp'
    wgms_fp = '../data/wgms/wgms.shp'
    glims_gdf = read_glims_gdf(glims_fp)                         # load glims gdf
    wgms_gdf = read_wgms_gdf(gdf_fp=wgms_fp)                     # load wgms gdf
    joined = sjoin(glims_gdf, wgms_gdf)                          # spatial join
    glac_dict = id_query('G212063E61198N', joined)               # query ID info from joined



