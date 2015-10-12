'''
Script to organize the DCMIP-2012 data into a directory structure of the form: 

/data/%(project_id)s/%(model)s/%(test_case)s/%(resolution)s/%(levels)s/%(grid)s/%(equation)s

suitable for publishing via the ESGF publisher.:w

'''

import glob
import os

ROOT_DIR = '/data/dcmip-2012'

if __name__ == '__main__':
    
    # loop over list of netCDF files (and associated .txt files)
    # ['/data/dcmip-2012/cam-fv/cam-fv.11.medium.L60.latlon.hydro.nc', ....]
    files = glob.glob('%s/*/*.nc' % ROOT_DIR) + glob.glob('%s/*/*.txt' % ROOT_DIR)
    for oldpathname in files:
        filename = os.path.basename(oldpathname)
        
        # create sub-directory 
        # cam-fv/42/medium/L30/latlon/hydro/2nd_order_div_damping
        subdir = filename.replace('.nc','').replace('.','/')
        
        # must add equation='default' if missing from filename
        # uzim/51/ultra/L30/interp_latlon/hydro
        # cam-fv/22/medium/L60/latlon/hydro/4th_order_div_damping
        subdirs = subdir.split('/')
        if len(subdirs)==6:
            subdir += '/default'
        newdir = '%s/%s' % (ROOT_DIR, subdir)
        if not os.path.exists(newdir):
            os.makedirs(newdir)
            
        # move the file
        newpathname = '%s/%s/%s' % (ROOT_DIR, subdir, filename)
        print 'Moving: %s --> %s' % (oldpathname, newpathname)
        os.rename(oldpathname, newpathname)