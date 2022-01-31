import requests as rq
import xml.etree.ElementTree as ET
import json
import xml.dom.minidom
import math
#import xmltodict
#import os
#from os import listdir
#from os.path import isfile, join

NASA_CMR_HOST = "https://cmr.earthdata.nasa.gov"
MAAP_CMR_HOST = "https://cmr.ops.maap-project.org"
SEARCH_COLLECTION_PATH = "/search/collections"
SEARCH_GRANULE_PATH = "/search/granules"
SEARCH_SOFTWARE_PATH = "/search/software"
PARAMETERS = "short_name=GEDI02_B&version=002"

def main():
    #0. Build URLs
    url_nasa = NASA_CMR_HOST + SEARCH_GRANULE_PATH
    url_maap = MAAP_CMR_HOST + SEARCH_GRANULE_PATH
    options = "page_size=2000&page_num=1"
    request_nasa = url_nasa + "?" + options + "&" + PARAMETERS
    request_maap = url_maap + "?" + options + "&" + PARAMETERS
    #1. Get a list of granules from NASA
    print(request_nasa) #testing
    print("Searching granlues from " + NASA_CMR_HOST)
    result = rq.get(request_nasa).text
    dom = xml.dom.minidom.parseString(result)
    dom_str = dom.toprettyxml()
    file_output = open("list_granules_xml_output_nasa.txt", "w") # reformat the xml file
    file_output.write(dom_str)
    file_output.close()

    tree = ET.parse("list_granules_xml_output_nasa.txt")
    root = tree.getroot()
    nasa_granules = {}

    total_granules = int(root.find('hits').text)
    page_num = 1
    round_num = int(math.ceil(total_granules / 2000))
    print("Total granules and number of rounds:" + str(round_num) + "; Given: " + str(total_granules) + " granules")
    while page_num <= round_num:
        req = url_nasa + "?" + PARAMETERS + "&" + "page_size=2000&page_num=" + str(page_num)
        print(req)
        res = rq.get(req).text
        xml_str = xml.dom.minidom.parseString(res).toprettyxml()
        file_output = open("temp_g_xml.txt", "w") # reformat the xml file
        file_output.write(xml_str)
        file_output.close()
        r = ET.parse("temp_g_xml.txt").getroot()
        
        for ref in r.findall('references/reference'):
            _name = ref.find('name')
            if(_name is not None):
                _name_text = _name.text.strip()
            else:
                _name_text = ""
            _location = ref.find('location')
            if(_location is not None):
                _location_text = _location.text.strip()
            else:
                _location_text = ""
            nasa_granules[_name_text] = _location_text
        page_num += 1
            
    with open("Nasa_granule_urs.json", "w") as outfile:
        json.dump(nasa_granules, outfile)

    #2. Get a list of granules from MAAP
    print(request_maap) #testing
    print("Searching granlues from " + MAAP_CMR_HOST)
    result = rq.get(request_maap).text
    dom = xml.dom.minidom.parseString(result)
    dom_str = dom.toprettyxml()
    file_output = open("list_granules_xml_output_maap.txt", "w") # reformat the xml file
    file_output.write(dom_str)
    file_output.close()

    tree = ET.parse("list_granules_xml_output_maap.txt")
    root = tree.getroot()
    maap_granules = {}

    total_granules = int(root.find('hits').text)
    page_num = 1
    round_num = int(math.ceil(total_granules / 2000))
    print("Total granules and number of rounds:" + str(round_num) + "; Given: " + str(total_granules) + " granules")
    while page_num <= round_num:
        req = url_maap + "?" + PARAMETERS + "&" + "page_size=2000&page_num=" + str(page_num)
        print(req)
        res = rq.get(req).text
        xml_str = xml.dom.minidom.parseString(res).toprettyxml()
        file_output = open("temp_g_xml.txt", "w") # reformat the xml file
        file_output.write(xml_str)
        file_output.close()
        r = ET.parse("temp_g_xml.txt").getroot()

        for ref in r.findall('references/reference'):
            _name = ref.find('name')
            if(_name is not None):
                _name_text = _name.text.strip()
            else:
                _name_text = ""
            _location = ref.find('location')
            if(_location is not None):
                _location_text = _location.text.strip()
            else:
                _location_text = ""
            maap_granules[_name_text] = _location_text
        page_num += 1
            
    with open("MAAP_granule_urs.json", "w") as outfile:
        json.dump(maap_granules, outfile)

    #3. Check the missing granules
    missing_granlues = {}
    for key, value in nasa_granules.items():
        if(key in maap_granules):
            continue
        missing_granlues[key] = value
    
    print("Missing granule urs are writing to Missing_granule_urs.json...")
    with open("Missing_granule_urs.json", "w") as outfile:
        json.dump(missing_granlues, outfile)
    print("Done!")
    return

if __name__ == '__main__':
    main()
