"""
returns list of collections (datasets) for a list of platform/instruments
"""

import re
import urllib.request
import xml.etree.ElementTree as ET
import csv

acronyms = ['ACE', 'ACE(MAG)', 'ACE(SIS)', 'ACE(SWEPAM)', 'ACRIMSAT(ACRIM3)', 'AERONET', 'AGAGE', 'Airborne_Hyperspectral', 'AMPERE', 'AQUA', 'AQUA(AIRS)', 'AQUA(AMSU)', 'AQUA(CERES)', 'AQUA(MODIS)', 'AQUARIUS']

acronyms = ["ACE", "ACE(MAG)", "ACE(SIS)", "ACE(SWEPAM)","ACRIMSAT(ACRIM III)", "AERONET",
            "AGAGE", "Airborne_Hyperspectral", "AMPERE",  "AQUA", "AQUA(AIRS)",
            "AQUA(AMSU)", "AQUA(CERES)", "AQUA(MODIS)", "AQUARIUS_SAC-D", "AURA",
            "AURA(MLS)", "AURA(OMI)", "AURA(TES)", "CALIPSO(CALIOP)",
            "CloudSat", "EO-1", "EO-1(ALI)", "EO-1(HYPERION)",
            "GAW_Aerosol_LiDAR(GALION)", "GRACE", "GRACE(GNSSRO)", "ICEBRIDGE",
            "JPL/TEC", "MASTER", "MPLNET", "SUOMI-NPP", "SUOMI-NPP(ATMS)", "SUOMI-NPP(CERES)",
            "SUOMI-NPP(CrIS)", "SUOMI-NPP(OMPS)", "SUOMI-NPP(VIIRS)", "SDO", "SDO(AIA)", "SDO(EVE)",
            "SDO(HMI)", "SEBASS_HS/TIR", "SOHO", "SOHO(COSTEP)", "SOHO(EIT)",
            "SOHO(LASCO)", "SORCE(TIM)", "STEREO A", "STEREO A(IMPACT)",
            "STEREO A(PLASTIC)", "STEREO A(SECCHI/COR1&COR2)","STEREO A(SECCHI)",
            "STEREO A(SECCI/EUVI)", "STEREO A(SWAVES)", "TCCON","TERRA",
            "TERRA(ASTER)", "TERRA(CERES)", "TERRA(MISR)", "TERRA(MODIS)",
            "TERRA(MOPITT)", "TRMM", "WIND", "WIND(MFI)", "WIND(SWE)",
            "WIND(WAVES)", "WMO_GAW_Aerosol_Ntwk"]

count = 0 


with open('CMR_SBA_query.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    for acronym in acronyms:
        plat_inst = acronym.replace(')','').split('(')
        plat_inst.append('')
        plat_inst = [urllib.request.pathname2url(x) for x in plat_inst]
        if len(plat_inst) == 3:
            plat_inst[1] += '*'
        url = 'https://cmr.earthdata.nasa.gov/search/collections?platform={}&instrument={}'.format(plat_inst[0],plat_inst[1])
        options = '&options[platform][ignore_case]=true&options[platform][pattern]=true&options[instrument][ignore_case]=true&options[instrument][pattern]=true'
        api = urllib.request.urlopen(url + options)
        e = ET.parse(api)
        if e.find('.//hits').text == '0':
            count +=1
        writer.writerow([acronym,e.find('.//hits').text])
        print("number of hits for {}: {}".format(acronym, e.find('.//hits').text))

print("Zero Hits: ", count, "Hits: ", len(acronyms) - count)
