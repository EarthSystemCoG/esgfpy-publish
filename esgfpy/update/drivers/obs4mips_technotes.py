import logging
from esgfpy.update.utils import updateSolr

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8984/solr'

# associate tech note to datasets
myDict = {'id:obs4MIPs.NASA-JPL.AIRS.ta.mon.v20110608|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote']},
          'id:obs4MIPs.NASA-JPL.AIRS.hus.mon.v20110608|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/husTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Specific Humidity Technical Note|technote']},
          'id:obs4MIPs.REMSS.AMSRE.tos.mon.v20111031|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/tosTechNote_AMSRE_L3_v7_200206-201012.pdf|AMSRE Sea Surface Temperature Technical Note|technote']},
          'id:obs4MIPs.CNES.AVISO.zos.mon.v20110829|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|technote']},           
          'id:obs4MIPs.NASA-JPL.MLS.ta.mon.v20111025|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/taTechNote_MLS_L3_v03-3x_200408-201012.pdf|MLS Air Temperature Technical Note|technote']},
          'id:obs4MIPs.NASA-JPL.MLS.hus.mon.v20111025|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/husTechNote_MLS_L3_v03-3x_200408-201012.pdf|MLS Specific Humidity Technical Note|technote']},          
          'id:obs4MIPs.NASA-GSFC.MODIS.clt.mon.v20111130|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/cltTechNote_MODIS_L3_C5_200003-201109.pdf|MODIS Cloud Fraction Technical Note|technote']},
          'id:obs4MIPs.NASA-JPL.QuikSCAT.sfcWind.mon.v20120411|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/sfcWind_QuikSCAT_L2B_v20110531_199908-200910.pdf|QuikSCAT sfcWind Technical Note|technote']},
          'id:obs4MIPs.NASA-JPL.QuikSCAT.uas.mon.v20120411|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/uas_QuikSCAT_L2B_v20110531_199908-200910.pdf|QuikSCAT uas Technical Note|technote']},
          'id:obs4MIPs.NASA-JPL.QuikSCAT.vas.mon.v20120411|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://www.earthsystemcog.org/site_media/projects/obs4mips/vas_QuikSCAT_L2B_v20110531_199908-200910.pdf|QuikSCAT vas Technical Note|technote']},
          'id:obs4MIPs.NASA-JPL.TES.tro3.mon.v20110608|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/tro3TechNote_TES_L3_tbd_200507-200912.pdf|TES Ozone Technical Note|technote']},
          }

updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')

# associate tech note to files
myDict = {'dataset_id:obs4MIPs.NASA-JPL.AIRS.ta.mon.v20110608|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.AIRS.hus.mon.v20110608|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/husTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Specific Humidity Technical Note|technote']},
          'dataset_id:obs4MIPs.REMSS.AMSRE.tos.mon.v20111031|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/tosTechNote_AMSRE_L3_v7_200206-201012.pdf|AMSRE Sea Surface Temperature Technical Note|technote']},
          'dataset_id:obs4MIPs.CNES.AVISO.zos.mon.v20110829|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.MLS.ta.mon.v20111025|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/taTechNote_MLS_L3_v03-3x_200408-201012.pdf|MLS Air Temperature Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.MLS.hus.mon.v20111025|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/husTechNote_MLS_L3_v03-3x_200408-201012.pdf|MLS Specific Humidity Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-GSFC.MODIS.clt.mon.v20111130|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/cltTechNote_MODIS_L3_C5_200003-201109.pdf|MODIS Cloud Fraction Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.QuikSCAT.sfcWind.mon.v20120411|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/sfcWind_QuikSCAT_L2B_v20110531_199908-200910.pdf|QuikSCAT sfcWind Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.QuikSCAT.uas.mon.v20120411|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/uas_QuikSCAT_L2B_v20110531_199908-200910.pdf|QuikSCAT uas Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.QuikSCAT.vas.mon.v20120411|esgf-data.jpl.nasa.gov': 
            {'xlink':['https://www.earthsystemcog.org/site_media/projects/obs4mips/vas_QuikSCAT_L2B_v20110531_199908-200910.pdf|QuikSCAT vas Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.TES.tro3.mon.v20110608|esgf-data.jpl.nasa.gov':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/tro3TechNote_TES_L3_tbd_200507-200912.pdf|TES Ozone Technical Note|technote']},
          }

updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='files')

