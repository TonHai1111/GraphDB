import requests as rq
import xml.etree.ElementTree as ET
import json
import xml.dom.minidom
import math
import xmltodict
import os
from os import listdir
from os.path import isfile, join

NASA_CMR_HOST = "https://cmr.earthdata.nasa.gov"
MAAP_CMR_HOST = ""
#MAAP_TEST_CMR_HOST = "http://cmr-s-services-lb-tf-1501391941.us-east-1.elb.amazonaws.com"
MAAP_TEST_CMR_HOST = "https://cmr.uat.maap-project.org"
SEARCH_COLLECTION_PATH = "/search/collections"
SEARCH_GRANULE_PATH = "/search/granules"
SEARCH_SOFTWARE_PATH = "/search/software"

class Collection:
    def __init__(self):
        self.id = "-1"
        self.revision_id = -1
        self.location = ""
        self.name = ""
        self.deleted = False
    
    def __init__(self, _id, _revision_id, _location, _name, _deleted):
        self.id = _id
        self.revision_id = _revision_id
        self.location = _location
        self.name = _name
        self.deleted = _deleted

class Granule:
    def __init__(self):
        self.id = "-1"
        self.revision_id = -1
        self.location = ""
        self.name = ""
        self.deleted = False
    
    def __init__(self, _id, _revision_id, _location, _name, _deleted):
        self.id = _id
        self.revision_id = _revision_id
        self.location = _location
        self.name = _name
        self.deleted = _deleted

def print_log(mode, message, priority):
    if(mode == "DEBUG"):
        print(message)
    elif(mode == "RUN"):
        if(priority == "HIGH"):
            print(message)

def downloadCollections(host, options, mode):
    #0. Build URL
    if(host == "nasa_cmr" or host == "cmr"):
        url = NASA_CMR_HOST
    elif(host == "maap_cmr" or host == "maap"):
        url = MAAP_CMR_HOST
    elif(host == "maap_test_cmr" or host == "test"):
        url = MAAP_TEST_CMR_HOST
    else:
        url = MAAP_TEST_CMR_HOST
    url = url + SEARCH_COLLECTION_PATH
    # http://cmr-s-services-lb-tf-1501391941.us-east-1.elb.amazonaws.com/search/collections
    options += "?page_size=2000&page_num=1"
    request = url + options
    #1. Get a list of collections
    print_log(mode, "Collection searching...", "HIGH")
    result = rq.get(request).text
    dom = xml.dom.minidom.parseString(result)
    dom_str = dom.toprettyxml()
    file_output = open("request_xml_output.txt", "w") # reformat the xml file
    file_output.write(dom_str)
    file_output.close()
    print_log(mode, "Successuflly wrote the result to files!", "HIGH")

    tree = ET.parse("request_xml_output.txt")
    root = tree.getroot()
    cols = []
    # Get a list of collections
    # TODO: should be aware on the facthat if the number of collections > 2000
    for ref in root.findall('references/reference'):
        isDel = ref.find('deleted')
        if (isDel is not None):
            _deleted = bool(isDel.text.strip())
        else:
            _deleted = False
        _name = ref.find('name')
        if(_name is not None):
            _name_text = _name.text.strip()
        else:
            _name_text = ""
        _id = ref.find('id')
        if(_id is not None):
            _id_text = _id.text.strip()
        else:
            _id_text = "-1"
        _location = ref.find('location')
        if(_location is not None):
            _location_text = _location.text.strip()
        else:
            _location_text = ""
        _revision_id = ref.find('revision-id')
        if(_revision_id is not None):
            _revision_id = int(_revision_id.text.strip())
        else:
            _revision_id = -1
        
        #Search for existing collection
        #found = False
        #for i in range(0, len(cols)):
        #    if(cols[i].id == _id_text):
        #        found = True
        #        if(cols[i].revision_id < _revision_id):
        #            cols[i].name = _name_text
        #            cols[i].location = _location_text
        #            cols[i].revision_id = _revision_id
        #            cols[i].deleted = _deleted
        #        break
        #if (not found):
        #    col = Collection(_id_text, _revision_id, _location_text, _name_text, _deleted)
        #    cols.append(col)
        col = Collection(_id_text, _revision_id, _location_text, _name_text, _deleted)
        cols.append(col)
    #Remove deleted collections
    #i = 0
    #while i < len(cols):
    #    if(cols[i].deleted == True):
    #        cols.pop(i)
    #    else:
    #        i += 1
        
    print_log(mode, "Total number of collections: " + str(len(cols)), "LOW")    

    for i in range(0, len(cols)):
        print_log(mode, "Collection " + str(i) + ": " + str(cols[i].name), "LOW")
        print_log(mode, "\t" + str(cols[i].id), "LOW")
        print_log(mode, "\t" + str(cols[i].revision_id), "LOW")
        print_log(mode, "\t" + str(cols[i].location), "LOW")
        print_log(mode, "\t" + str(cols[i].deleted), "LOW")

    #2. Download collection metadata and store it in the Data/Collections folder
    for i in range(0, len(cols)):
        url = cols[i].location
        print_log(mode, "Download " + str(url), "HIGH")
        col_meta = rq.get(url).text
        file_out = open("Data/Collections/" + str(cols[i].id) + ".json", "w")
        file_out.write(col_meta)
        file_out.close
    print_log(mode, "Total: " + str(i + 1) + " collections", "HIGH")
    print_log(mode, "Done", "HIGH")
    return

def downloadGranules(host, options, mode):    
    #0. Build URL
    if(host == "nasa_cmr" or host == "cmr"):
        url = NASA_CMR_HOST
    elif(host == "maap_cmr" or host == "maap"):
        url = MAAP_CMR_HOST
    elif(host == "maap_test_cmr" or host == "test"):
        url = MAAP_TEST_CMR_HOST
    else:
        url = MAAP_TEST_CMR_HOST
    url = url + SEARCH_GRANULE_PATH
    options += "?page_size=2000&page_num=1"
    request = url + options
    #1. Get a list of granules
    print_log(mode, "Granule searching...", "HIGH")
    result = rq.get(request).text
    dom = xml.dom.minidom.parseString(result)
    dom_str = dom.toprettyxml()
    file_output = open("list_granules_xml_output.txt", "w") # reformat the xml file
    file_output.write(dom_str)
    file_output.close()
    print_log(mode, "Successuflly wrote the result to files!", "HIGH")

    tree = ET.parse("list_granules_xml_output.txt")
    root = tree.getroot()
    granules = []

    total_granules = int(root.find('hits').text)
    page_num = 1
    round_num = math.ceil(total_granules / 2000)
    print("Total granules and number of rounds:")
    print(total_granules)
    print(round_num)
    while page_num <= round_num:
        req = url + "?page_size=2000&page_num=" + str(page_num)
        print(req)
        res = rq.get(req).text
        xml_str = xml.dom.minidom.parseString(res).toprettyxml()
        file_output = open("temp_g_xml.txt", "w") # reformat the xml file
        file_output.write(xml_str)
        file_output.close()
        r = ET.parse("temp_g_xml.txt").getroot()

        for ref in r.findall('references/reference'):
            isDel = ref.find('deleted')
            if (isDel is not None):
                _deleted = bool(isDel.text.strip())
            else:
                _deleted = False
            _name = ref.find('name')
            if(_name is not None):
                _name_text = _name.text.strip()
            else:
                _name_text = ""
            _id = ref.find('id')
            if(_id is not None):
                _id_text = _id.text.strip()
            else:
                _id_text = "-1"
            _location = ref.find('location')
            if(_location is not None):
                _location_text = _location.text.strip()
            else:
                _location_text = ""
            _revision_id = ref.find('revision-id')
            if(_revision_id is not None):
                _revision_id = int(_revision_id.text.strip())
            else:
                _revision_id = -1
            
            gra = Granule(_id_text, _revision_id, _location_text, _name_text, _deleted)
            granules.append(gra)

        #print(page_num)
        page_num += 1
        if (page_num > 6): #CMR does not allow to download more than 10000 metadata.
            break

    print_log(mode, "Total number of granules: " + str(len(granules)), "LOW")
    
    temp = open("temp_granules.txt", "w")
    temp.write("Total number of granules: " + str(len(granules)) + "\n")
    for i in range(0, len(granules)):
        #print_log(mode, "Granule " + str(i) + ": " + str(granules[i].name), "LOW")
        #print_log(mode, "\t" + str(granules[i].id), "LOW")
        #print_log(mode, "\t" + str(granules[i].revision_id), "LOW")
        #print_log(mode, "\t" + str(granules[i].location), "LOW")
        #print_log(mode, "\t" + str(granules[i].deleted), "LOW")
        temp.write("Granule " + str(i) + ": " + str(granules[i].name) + "\n")
        temp.write("\t" + str(granules[i].id) + "\n")
        temp.write("\t" + str(granules[i].revision_id) + "\n")
        temp.write("\t" + str(granules[i].location) + "\n")
        temp.write("\t" + str(granules[i].deleted) + "\n")
    temp.close()
    
    #2. Download collection metadata and store it in the Data/Granules folder
    for i in range(0, len(granules)):
        url = granules[i].location + ".umm_json"
        print_log(mode, "Download " + str(url), "HIGH")
        gra_meta = rq.get(url).text
        file_out = open("Data/Granules/" + str(granules[i].id) + ".json", "w")
        file_out.write(gra_meta)
        file_out.close
    print_log(mode, "Total: " + str(i + 1) + " Granules", "HIGH")
    print_log(mode, "Done", "HIGH")
    return

#Download Granules in with different collection
def downloadGranules_v2(host, options, mode, out_dir):    
    #0. Build URL
    if(host == "nasa_cmr" or host == "cmr"):
        url = NASA_CMR_HOST
    elif(host == "maap_cmr" or host == "maap"):
        url = MAAP_CMR_HOST
    elif(host == "maap_test_cmr" or host == "test"):
        url = MAAP_TEST_CMR_HOST
    else:
        url = MAAP_TEST_CMR_HOST
    url = url + SEARCH_GRANULE_PATH
    if(options == ""):
        request = url + "?page_size=2000&page_num=1"
    else:
        request = url + options + "&page_size=2000&page_num=1"
    #request = url + options
    #1. Get a list of granules
    print_log(mode, "Granule searching...", "HIGH")
    result = rq.get(request).text
    print(request)
    dom = xml.dom.minidom.parseString(result)
    dom_str = dom.toprettyxml()
    file_output = open("list_granules_xml_output.txt", "w") # reformat the xml file
    file_output.write(dom_str)
    file_output.close()
    print_log(mode, "Successuflly wrote the result to files!", "HIGH")

    tree = ET.parse("list_granules_xml_output.txt")
    root = tree.getroot()
    granules = []

    total_granules = int(root.find('hits').text)
    page_num = 1
    round_num = int(math.ceil(total_granules / 2000))
    print("Total granules and number of rounds:")
    print(total_granules)
    print(round_num)
    while page_num <= round_num:
        if(options == ""):
            req = url + "?page_size=2000&page_num=" + str(page_num)
        else:
            req = url+ options + "&page_size=2000&page_num=" + str(page_num)
        #req = url + options
        print(req)
        res = rq.get(req).text
        xml_str = xml.dom.minidom.parseString(res).toprettyxml()
        file_output = open("temp_g_xml.txt", "w") # reformat the xml file
        file_output.write(xml_str)
        file_output.close()
        r = ET.parse("temp_g_xml.txt").getroot()

        for ref in r.findall('references/reference'):
            isDel = ref.find('deleted')
            if (isDel is not None):
                _deleted = bool(isDel.text.strip())
            else:
                _deleted = False
            _name = ref.find('name')
            if(_name is not None):
                _name_text = _name.text.strip()
            else:
                _name_text = ""
            _id = ref.find('id')
            if(_id is not None):
                _id_text = _id.text.strip()
            else:
                _id_text = "-1"
            _location = ref.find('location')
            if(_location is not None):
                _location_text = _location.text.strip()
            else:
                _location_text = ""
            _revision_id = ref.find('revision-id')
            if(_revision_id is not None):
                _revision_id = int(_revision_id.text.strip())
            else:
                _revision_id = -1
            
            gra = Granule(_id_text, _revision_id, _location_text, _name_text, _deleted)
            granules.append(gra)

        #print(page_num)
        page_num += 1
        if (page_num > 2): #CMR does not allow to download more than 10000 metadata.
            break           # Only download 4000 granules max per collection

    print_log(mode, "Total number of granules: " + str(len(granules)), "LOW")
    
    temp = open("temp_granules.txt", "w")
    temp.write("Total number of granules: " + str(len(granules)) + "\n")
    for i in range(0, len(granules)):
        #print_log(mode, "Granule " + str(i) + ": " + str(granules[i].name), "LOW")
        #print_log(mode, "\t" + str(granules[i].id), "LOW")
        #print_log(mode, "\t" + str(granules[i].revision_id), "LOW")
        #print_log(mode, "\t" + str(granules[i].location), "LOW")
        #print_log(mode, "\t" + str(granules[i].deleted), "LOW")
        temp.write("Granule " + str(i) + ": " + str(granules[i].name) + "\n")
        temp.write("\t" + str(granules[i].id) + "\n")
        temp.write("\t" + str(granules[i].revision_id) + "\n")
        temp.write("\t" + str(granules[i].location) + "\n")
        temp.write("\t" + str(granules[i].deleted) + "\n")
    temp.close()
    
    # Create folder if it does not exist
    if not os.path.exists("Data/Granules/" + str(out_dir)):
        os.makedirs("Data/Granules/" + str(out_dir))
    #2. Download collection metadata and store it in the Data/Granules folder
    i = 0
    for i in range(0, len(granules)):
        url = granules[i].location + ".umm_json"
        print_log(mode, "Download " + str(url), "HIGH")
        gra_meta = rq.get(url).text
        file_out = open("Data/Granules/" + str(out_dir) + "/" + str(granules[i].id) + ".json", "w")
        file_out.write(gra_meta)
        file_out.close
    print_log(mode, "Total: " + str(i + 1) + " Granules", "HIGH")
    print_log(mode, "Done", "HIGH")
    return

def getGranules():
    #0. Build URL
    ops_cmr_url = "https://cmr.ops.maap-project.org"
    nasa_cmr_url = "https://cmr.earthdata.nasa.gov"

    ops_cmr_url += "/search/granules"
    nasa_cmr_url += "/search/granules"
    
    ops_cmr_request = ops_cmr_url + "?collection_concept_id=C1201300747-NASA_MAAP&bounding_box=-180,50,180,75&temporal=2020-06-01T00:00:00.000Z,2020-09-30T23:59:59.000Z&page_size=2000&page_num=1"
    nasa_cmr_request = nasa_cmr_url + "?collection_concept_id=C1997321091-NSIDC_ECS&bounding_box=-180,50,180,75&temporal=2020-06-01T00:00:00.000Z,2020-09-30T23:59:59.000Z&page_size=2000&page_num=1"
    
    ops_result = rq.get(nasa_cmr_request).text
    dom = xml.dom.minidom.parseString(ops_result)
    dom_str = dom.toprettyxml()
    file_output = open("list_granules_nasa.txt", "w")
    file_output.write(dom_str)
    file_output.close()
    tree = ET.parse("list_granules_nasa.txt")
    root = tree.getroot()
    total_granules = int(root.find('hits').text)
    page_num = 1
    round_num = int(math.ceil(total_granules / 2000))
    print("Total granules and number of rounds:" + str(total_granules) + "\t" + str(round_num))
    
    granules_nasa = []
    while page_num <= round_num:
        nasa_cmr_request = nasa_cmr_url + "?collection_concept_id=C1997321091-NSIDC_ECS&bounding_box=-180,50,180,75&temporal=2020-06-01T00:00:00.000Z,2020-09-30T23:59:59.000Z&page_size=2000&page_num=" + str(page_num)
        print(nasa_cmr_request)
        res_nasa = rq.get(nasa_cmr_request).text
        xml_str = xml.dom.minidom.parseString(res_nasa).toprettyxml()
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
            granules_nasa.append(_name_text)
        page_num += 1
    page_num = 1

    granules_ops = []
    while page_num <= round_num:
        ops_cmr_request = ops_cmr_url + "?collection_concept_id=C1201300747-NASA_MAAP&bounding_box=-180,50,180,75&temporal=2020-06-01T00:00:00.000Z,2020-09-30T23:59:59.000Z&page_size=2000&page_num="  + str(page_num)
        print(ops_cmr_request)
        res_ops = rq.get(ops_cmr_request).text
        xml_str = xml.dom.minidom.parseString(res_ops).toprettyxml()
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
            granules_ops.append(_name_text)
        page_num += 1
    
    f_nasa = open("f_nasa.txt", "w")
    for i in range(len(granules_nasa)):
        f_nasa.write(str(granules_nasa[i]) + "\n")
    f_nasa.close()
    f_ops = open("f_ops.txt", "w")
    for i in range(len(granules_ops)):
        f_ops.write(str(granules_ops[i]) + "\n")
    f_ops.close()
    return

if __name__ == '__main__':
    getGranules()
