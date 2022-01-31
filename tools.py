import json
from os import listdir
from os.path import isfile, join

def get_all_full_filenames(path):
    files = []
    for item in listdir(path):
        item_fullpath = join(path, item)
        if isfile(item_fullpath):
            files.append(item_fullpath)
        else:
            f = get_all_full_filenames(item_fullpath)
            files.extend(f)
    return files

def formatSpecialCharacter(val):
    res = val.replace("'", "\\'")
    return res

def buildCypherRelationship(granuleUR, collection_concept_id):
    cqlQuery = "MATCH (g:Granule), (c:Collection) " +\
    "WHERE g.GranuleUR='" + str(granuleUR) + \
    "' and c.Collection_concept_id='" + str(collection_concept_id) + "' " +\
    "MERGE (g)-[:BELONG_TO]->(c)"
    return cqlQuery

def buildCypherRelationship_Summarization(granuleUR, collection_concept_id):
    cqlQuery = "MATCH (g:Granule), (c:Collection) " +\
    "WHERE g.GranuleUR='" + str(granuleUR) + \
    "' and c.Collection_concept_id='" + str(collection_concept_id) + "' " +\
    "MERGE (xG:sGranule {Name:'" + str(collection_concept_id) + \
    "'}) " + \
    "MERGE (g)-[:BELONG_TO {viz_level:'N'}]->(xG) " + \
    "MERGE (xG)-[:BELONG_TO {viz_level:'N'}]->(c) " + \
    "MERGE (c)-[:HAS {viz_level:'E'}]->(g) " 
    return cqlQuery

def buildCypherfromjson(json_file, collection_concept_id):
    cqlQuery = ""
    file_obj = open(json_file)
    dict_obj = json.load(file_obj)
    # Take ShortName, EntryTitle and Version
    ShortName = dict_obj.pop('ShortName')
    EntryTitle = dict_obj.pop('EntryTitle')
    Version = dict_obj.pop('Version')
    cqlQuery = "MERGE (c:Collection {" + \
    "ShortName:'" +  ShortName + "', " + \
    "Collection_concept_id:'" + str(collection_concept_id) + "', " + \
    "EntryTitle:'" + EntryTitle + "', " + \
    "Version:'" + Version + "'}) "

    set_words = "ON CREATE SET "  
    comma = ""  
    set_string = ""
    # Abstract, Collection Citations, Quality and Purpose
    words = {"Abstract", "Collection Citations", "Quality", "Purpose"}
    for w in words:
        if w in dict_obj:
            set_string += comma + "c." + str(w) + "='" + formatSpecialCharacter(str(dict_obj[w])) + "'"
            comma = ", "
    
    # TemporalExtents, SpatialExtent, Metadatadates, AdditionalAttribute, Projects, DOI, DataDates, RelatedUrls
    words2 = {"TemporalExtents", "SpatialExtent", "Metadatadates", "AdditionalAttribute", "Projects", "DOI"}
    if ("TemporalExtents" in dict_obj):
        if("RangeDateTimes" in dict_obj["TemporalExtents"]):
            st = dict_obj["TemporalExtents"][0]["RangeDateTimes"][0]["BeginningDateTime"]
            en = dict_obj["TemporalExtents"][0]["RangeDateTimes"][0]["EndingDateTime"]
            set_string += comma + "c.`TemporalExtents.RangeDateTimes`" + \
            "='" + str(st) + ", " + str(en) + "'"
            comma = ", "
        elif("SingleDateTimes" in dict_obj["TemporalExtents"]):
            st = dict_obj["TemporalExtents"][0]["SingleDateTimes"][0]
            en = dict_obj["TemporalExtents"][0]["SingleDateTimes"][1]
            set_string += comma + "c.`TemporalExtents.SingleDateTimes`='" + \
            str(st) + ", " + str(en) + "'"
            comma = ", "
    
    if ("SpatialExtent" in dict_obj):
        north = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["NorthBoundingCoordinate"]
        west = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["WestBoundingCoordinate"]
        east = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["EastBoundingCoordinate"]
        south = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["SouthBoundingCoordinate"]
        set_string += comma + "c.`SpatialExtent.BoundingRectangles`='" + \
        str(north) + ", " +  str(west) + ", " + str(east) + ", " + str(south) + "'"
        comma = ", "
        
        stype = dict_obj["SpatialExtent"]["SpatialCoverageType"]
        ssystem = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["CoordinateSystem"]
        sgranule = dict_obj["SpatialExtent"]["GranuleSpatialRepresentation"]
        set_string += comma + "c.`SpatialExtent.SpatialCoverageType`='" + str(stype) + "'"
        set_string += comma + "c.`SpatialExtent.CoordinateSystem`='" + str(ssystem) + "'"
        set_string += comma + "c.`SpatialExtent.GranuleSpatialRepresentation`='" + str(sgranule) + "'"
    
    if("MetadataDates" in dict_obj):
        for item in dict_obj["MetadataDates"]:
            if(item["Type"] == "UPDATE"):
                set_string += comma + "c.`MetadataDates.UPDATE`='" + str(item["Date"]) + "'"
                comma = ", "
            if(item["Type"] == "CREATE"):
                set_string += comma + "c.`MetadataDates.CREATE`='" + str(item["Date"]) + "'"
                comma = ", "
    
    if("Projects" in dict_obj):
        set_string += comma + "c.`Projects.ShortName`='" + str(dict_obj["Projects"][0]["ShortName"]) + "', " + \
        "c.`Projects.LongName`='" + str(dict_obj["Projects"][0]["LongName"]) + "'"
        comma = ", "
    
    
    if("DOI" in dict_obj):
        for k in dict_obj["DOI"]:
            set_string += comma + "c.`DOI." + str(k) + "`='" + str(dict_obj["DOI"][k]) + "'"
            comma = ", "

    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() != "data format"): # Dataformat is an entity
                set_string += comma + "c.`" + str(item["Name"]) + "`='" + \
                formatSpecialCharacter(str(item["Description"])) + "'"
                for k in item.keys():
                    if (k != 'Name' and k != 'Description'):
                        set_string += comma + "c.`" + str(item["Name"]) + \
                        "." + str(k) + "`='" + \
                        str(item[k]) + "'"
                comma = ", "
    
    if("DataDates" in dict_obj):
        for item in dict_obj["DataDates"]:
            if(item["Type"] == "UPDATE"):
                set_string += comma + "c.`MetadataDates.UPDATE`='" + str(item["Date"]) + "'"
                comma = ", "
            if(item["Type"] == "CREATE"):
                set_string += comma + "c.`MetadataDates.CREATE`='" + str(item["Date"]) + "'"
                comma = ", "
    
    if("RelatedUrls" in dict_obj):
        count = 1
        for item in dict_obj["RelatedUrls"]:
            for k in item.keys():
                set_string += comma + "c.`RelatedUrl." + str(count) + "." + \
                str(k) + "`='" + formatSpecialCharacter(str(item[k])) + "'"
                comma = ", "
            count += 1
    
    cqlQuery += set_words + set_string

    #Add entities
    # ScienceKeywords, Platforms, DataCenters, LocationKeywords, AdditionalAttributes->Dataformat
    # ProcessingLevel, MetadataLanguages, CollectionDataType, CollectionProgress,
    # ContactPersons/ContactInformation/ContactGroups (belong to DataCenters)
    ent_string = ""
    newline = "\n"
    count = 0
    if("ScienceKeywords" in dict_obj):
        for item in dict_obj["ScienceKeywords"]: 
            count += 1
            # Category
            ent_string += newline + "MERGE (kc" + str(count) + \
            ":`ScienceKeywords.Category` {Category:'" + \
            str(item["Category"]) + "'})"
            # Topic
            ent_string += newline + "MERGE (kt" + str(count) + \
            ":`ScienceKeywords.Topic` {Topic:'" + \
            str(item["Topic"]) + "', " + "Category:'" + str(item["Category"]) + "'})" + \
            newline + "MERGE (kt" + str(count) + ")-[:CHILD_OF]->(kc" + str(count) + ")"
            # Term
            ent_string += newline + "MERGE (ke" + str(count) + \
            ":`ScienceKeywords.Term` {Term:'" + \
            str(item["Term"]) + "', Topic:'" + str(item["Topic"]) + "', Category:'" + \
            str(item["Category"]) + "'})" + \
            newline + "MERGE (ke" + str(count) + ")-[:CHILD_OF]->(kt" + str(count) + ")"
            # Variable
            v_count = 1
            while (("VariableLevel" + str(v_count)) in item):
                ent_string += newline + "MERGE (va" + str(count) + str(v_count) + \
                ":`ScienceKeywords.Variable` {Variable:'" + \
                str(item["VariableLevel" + str(v_count)]) + "', Term:'" + str(item["Term"]) +\
                "', Topic:'" + str(item["Topic"]) + "', Category:'" + str(item["Category"]) + \
                "'})" + \
                newline + "MERGE (va" + str(count) + str(v_count) + ")-[:CHILD_OF]->(ke" + str(count) + ")" +\
                newline + "MERGE (c)-[:HAS]->(va" + str(count) + str(v_count) + ")"
                v_count += 1
            if(v_count == 1):
                ent_string += newline + "MERGE (c)-[:HAS]->(ke" + str(count) + ")"
    # Platform
    if("Platforms" in dict_obj):
        pcount = 0
        for item in dict_obj["Platforms"]:
            pcount += 1
            ent_string += newline + "MERGE (pf" + str(pcount) + \
            ":Platforms {Type:'" + str(item["Type"]) + "', ShortName:'" + \
            str(item["ShortName"]) + "'})" 
            if("LongName" in item):
                ent_string += newline + "ON CREATE SET pf" + str(pcount) + \
                ".LongName='" + str(item["LongName"]) + "'" + \
                newline + "ON MATCH SET pf" + str(pcount) + ".LongName='" +\
                str(item["LongName"]) + "'"
            # Instruments
            inscount = 0
            if("Instruments" in item):
                for ins in item["Instruments"]:
                    inscount += 1
                    ent_string += newline + "MERGE (Ins" + str(pcount) + str(inscount) +\
                    ":Instruments {ShortName:'" + str(ins["ShortName"]) + "'})"
                    if("LongName" in ins):
                        ent_string += " ON CREATE SET Ins" + str(pcount) + str(inscount) + \
                        ".LongName='" + str(ins["LongName"]) + "'" + \
                        newline + "ON MATCH SET Ins" + str(pcount) + str(inscount) + ".LongName='" +\
                        str(ins["LongName"]) + "'"
                    ent_string += newline + "MERGE (pf" + str(pcount) + ")-[:HAS]->(Ins" +\
                    str(pcount) + str(inscount) + ")"
                    if("ComposedOf" in ins):
                        comcount = 0
                        for com in ins["ComposedOf"]:
                            comcount += 1
                            ent_string += newline + "MERGE (com" + str(pcount) + str(inscount) + str(comcount) +\
                            ":ComposedOf {ShortName:'" + str(com["ShortName"]) + "', LongName:'" + \
                            str(com["LongName"]) + "'})"  + \
                            newline + "MERGE (Ins" + str(pcount) + str(inscount) + ")-[:HAS]->(com" +\
                            str(pcount) + str(inscount) + str(comcount) + ")"
                    ent_string += "MERGE (c)-[:HAS]->(Ins" + str(pcount) + str(inscount) + ")"
            # Connect Platform to collection if there is no instrument
            if(inscount == 0):
                ent_string += newline + "MERGE (c)-[:HAS]->(pf" + str(pcount) + ")"
    # DataCenters
    if("DataCenters" in dict_obj):
        #Role is the connetion between DataCenter and Collection
        dccount = 0
        for dc in dict_obj["DataCenters"]:
            dccount += 1
            ent_string += newline + "MERGE (dc" + str(dccount) + ":DataCenters {ShortName:'" +\
            str(dc["ShortName"]) + "'})"
            if("LongName" in dc):
                ent_string += " ON CREATE SET dc" + str(dccount) + \
                ".LongName='" + str(dc["LongName"]) + "'" + \
                newline + "ON MATCH SET dc" + str(dccount) + ".LongName='" +\
                str(dc["LongName"]) + "'"
            #
            #TODO: ContactPersons/ContactInformation/ContactGroups
            #
            for ro in dc["Roles"]:
                ent_string += newline + "MERGE (c)-[:" + str(ro) + "]->(dc" +\
                str(dccount) + ")"
    
    # DataFormat
    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() == "data format"):
                # TODO: add an entity
                ent_string += newline + "MERGE (format:`Data Format` {ShortName:'" +\
                str(item["Value"]) + "'}) ON CREATE SET format.Description='" +\
                str(item["Description"]) + "', format.DataType='" +\
                str(item["DataType"]) + "'" + \
                newline + "MERGE (c)-[:HAS]->(format)"
    
    # ProcessingLevel - ProcessingLevelDescription should be an attribute of the relationship.
    if("ProcessingLevel" in dict_obj):
        ent_string += newline + "MERGE (pl:ProcessingLevel {Id:'" + \
        str(dict_obj["ProcessingLevel"]["Id"]) + "'})"
        if("ProcessingLevelDescription" in dict_obj["ProcessingLevel"]):
            ent_string += newline + "MERGE (c)-[:HAS {Description:'" +\
            str(dict_obj["ProcessingLevel"]["ProcessingLevelDescription"]) + "'}]->(pl)" 
        else:
            ent_string += newline + "MERGE (c)-[:HAS]->(pl)"
    # MetadataLanguages
    if("MetadataLanguages" in dict_obj):
        ent_string += newline + "MERGE (ml:MetadataLanguages {Id:'" + \
        str(dict_obj["MetadataLanguages"]) + "'})" + newline + "MERGE (c)-[:LANGUAGE]->(ml)"

    # CollectionDataType
    if("CollectionDataType" in dict_obj):
        ent_string += newline + "MERGE (cdt:CollectionDataType {Id:'" + \
        str(dict_obj["CollectionDataType"]) + "'})" + newline + "MERGE (c)-[:DATATYPE]->(cdt)"


    # CollectionProgress - status as an entity - viewing this as a type of storing data
    if("CollectionProgress" in dict_obj):
        ent_string += newline + "MERGE (cp:CollectionProgress {Id:'" + \
        str(dict_obj["CollectionProgress"]) + "'})" + newline + "MERGE (c)-[:PROGRESS_STATUS]->(cp)"

    cqlQuery += ent_string
    return cqlQuery, collection_concept_id

'''
Graph with Summarization 
'''
def buildCypherfromjson_Summarization(json_file, collection_concept_id):
    cqlQuery = ""
    file_obj = open(json_file)
    dict_obj = json.load(file_obj)
    # Take ShortName, EntryTitle and Version
    ShortName = dict_obj.pop('ShortName')
    EntryTitle = dict_obj.pop('EntryTitle')
    Version = dict_obj.pop('Version')
    cqlQuery = "MERGE (c:Collection {" + \
    "ShortName:'" +  ShortName + "', " + \
    "Collection_concept_id:'" + str(collection_concept_id) + "', " + \
    "EntryTitle:'" + EntryTitle + "', " + \
    "Version:'" + Version + "'}) "

    set_words = "ON CREATE SET "  
    comma = ""  
    set_string = ""
    # Abstract, Collection Citations, Quality and Purpose
    words = {"Abstract", "Collection Citations", "Quality", "Purpose"}
    for w in words:
        if w in dict_obj:
            set_string += comma + "c." + str(w) + "='" + formatSpecialCharacter(str(dict_obj[w])) + "'"
            comma = ", "
    
    # TemporalExtents, SpatialExtent, Metadatadates, AdditionalAttribute, Projects, DOI, DataDates, RelatedUrls
    words2 = {"TemporalExtents", "SpatialExtent", "Metadatadates", "AdditionalAttribute", "Projects", "DOI"}
    if ("TemporalExtents" in dict_obj):
        if("RangeDateTimes" in dict_obj["TemporalExtents"]):
            st = dict_obj["TemporalExtents"][0]["RangeDateTimes"][0]["BeginningDateTime"]
            en = dict_obj["TemporalExtents"][0]["RangeDateTimes"][0]["EndingDateTime"]
            set_string += comma + "c.`TemporalExtents.RangeDateTimes`" + \
            "='" + str(st) + ", " + str(en) + "'"
            comma = ", "
        elif("SingleDateTimes" in dict_obj["TemporalExtents"]):
            st = dict_obj["TemporalExtents"][0]["SingleDateTimes"][0]
            en = dict_obj["TemporalExtents"][0]["SingleDateTimes"][1]
            set_string += comma + "c.`TemporalExtents.SingleDateTimes`='" + \
            str(st) + ", " + str(en) + "'"
            comma = ", "
    
    if ("SpatialExtent" in dict_obj):
        north = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["NorthBoundingCoordinate"]
        west = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["WestBoundingCoordinate"]
        east = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["EastBoundingCoordinate"]
        south = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["SouthBoundingCoordinate"]
        set_string += comma + "c.`SpatialExtent.BoundingRectangles`='" + \
        str(north) + ", " +  str(west) + ", " + str(east) + ", " + str(south) + "'"
        comma = ", "
        
        stype = dict_obj["SpatialExtent"]["SpatialCoverageType"]
        ssystem = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["CoordinateSystem"]
        sgranule = dict_obj["SpatialExtent"]["GranuleSpatialRepresentation"]
        set_string += comma + "c.`SpatialExtent.SpatialCoverageType`='" + str(stype) + "'"
        set_string += comma + "c.`SpatialExtent.CoordinateSystem`='" + str(ssystem) + "'"
        set_string += comma + "c.`SpatialExtent.GranuleSpatialRepresentation`='" + str(sgranule) + "'"
    
    if("MetadataDates" in dict_obj):
        for item in dict_obj["MetadataDates"]:
            if(item["Type"] == "UPDATE"):
                set_string += comma + "c.`MetadataDates.UPDATE`='" + str(item["Date"]) + "'"
                comma = ", "
            if(item["Type"] == "CREATE"):
                set_string += comma + "c.`MetadataDates.CREATE`='" + str(item["Date"]) + "'"
                comma = ", "
    
    if("Projects" in dict_obj):
        set_string += comma + "c.`Projects.ShortName`='" + str(dict_obj["Projects"][0]["ShortName"]) + "', " + \
        "c.`Projects.LongName`='" + str(dict_obj["Projects"][0]["LongName"]) + "'"
        comma = ", "
    
    
    if("DOI" in dict_obj):
        for k in dict_obj["DOI"]:
            set_string += comma + "c.`DOI." + str(k) + "`='" + str(dict_obj["DOI"][k]) + "'"
            comma = ", "

    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() != "data format"): # Dataformat is an entity
                set_string += comma + "c.`" + str(item["Name"]) + "`='" + \
                formatSpecialCharacter(str(item["Description"])) + "'"
                for k in item.keys():
                    if (k != 'Name' and k != 'Description'):
                        set_string += comma + "c.`" + str(item["Name"]) + \
                        "." + str(k) + "`='" + \
                        str(item[k]) + "'"
                comma = ", "
    
    if("DataDates" in dict_obj):
        for item in dict_obj["DataDates"]:
            if(item["Type"] == "UPDATE"):
                set_string += comma + "c.`MetadataDates.UPDATE`='" + str(item["Date"]) + "'"
                comma = ", "
            if(item["Type"] == "CREATE"):
                set_string += comma + "c.`MetadataDates.CREATE`='" + str(item["Date"]) + "'"
                comma = ", "
    
    if("RelatedUrls" in dict_obj):
        count = 1
        for item in dict_obj["RelatedUrls"]:
            for k in item.keys():
                set_string += comma + "c.`RelatedUrl." + str(count) + "." + \
                str(k) + "`='" + formatSpecialCharacter(str(item[k])) + "'"
                comma = ", "
            count += 1
    
    cqlQuery += set_words + set_string

    #Add entities
    # ScienceKeywords, Platforms, DataCenters, LocationKeywords, AdditionalAttributes->Dataformat
    # ProcessingLevel, MetadataLanguages, CollectionDataType, CollectionProgress,
    # ContactPersons/ContactInformation/ContactGroups (belong to DataCenters)
    ent_string = ""
    newline = "\n"
    count = 0
    if("ScienceKeywords" in dict_obj):
        for item in dict_obj["ScienceKeywords"]: 
            count += 1
            # Category
            ent_string += newline + "MERGE (kc" + str(count) + \
            ":`ScienceKeywords.Category` {Category:'" + \
            str(item["Category"]) + "'})"
            ent_string += newline + "MERGE (c)-[:HAS {viz_level:'0'}]->(kc" + str(count) + ")"
            # Topic
            ent_string += newline + "MERGE (kt" + str(count) + \
            ":`ScienceKeywords.Topic` {Topic:'" + \
            str(item["Topic"]) + "', " + "Category:'" + str(item["Category"]) + "'})" + \
            newline + "MERGE (kt" + str(count) + ")-[:CHILD_OF]->(kc" + str(count) + ")"
            ent_string += newline + "MERGE (c)-[:HAS {viz_level:'1'}]->(kt" + str(count) + ")"
            # Term
            ent_string += newline + "MERGE (ke" + str(count) + \
            ":`ScienceKeywords.Term` {Term:'" + \
            str(item["Term"]) + "', Topic:'" + str(item["Topic"]) + "', Category:'" + \
            str(item["Category"]) + "'})" + \
            newline + "MERGE (ke" + str(count) + ")-[:CHILD_OF]->(kt" + str(count) + ")"
            # Variable
            v_count = 1
            while (("VariableLevel" + str(v_count)) in item):
                ent_string += newline + "MERGE (va" + str(count) + str(v_count) + \
                ":`ScienceKeywords.Variable` {Variable:'" + \
                str(item["VariableLevel" + str(v_count)]) + "', Term:'" + str(item["Term"]) +\
                "', Topic:'" + str(item["Topic"]) + "', Category:'" + str(item["Category"]) + \
                "'})" + \
                newline + "MERGE (va" + str(count) + str(v_count) + ")-[:CHILD_OF]->(ke" + str(count) + ")" +\
                newline + "MERGE (c)-[:HAS {viz_level:'E'}]->(va" + str(count) + str(v_count) + ")"
                v_count += 1
            if(v_count == 1):
                ent_string += newline + "MERGE (c)-[:HAS {viz_level:'E'}]->(ke" + str(count) + ")"
            else:
                ent_string += newline + "MERGE (c)-[:HAS {viz_level:'2'}]->(ke" + str(count) + ")"
    # Platform
    if("Platforms" in dict_obj):
        pcount = 0
        for item in dict_obj["Platforms"]:
            pcount += 1
            ent_string += newline + "MERGE (pf" + str(pcount) + \
            ":Platforms {Type:'" + str(item["Type"]) + "', ShortName:'" + \
            str(item["ShortName"]) + "'})" 
            if("LongName" in item):
                ent_string += newline + "ON CREATE SET pf" + str(pcount) + \
                ".LongName='" + str(item["LongName"]) + "'" + \
                newline + "ON MATCH SET pf" + str(pcount) + ".LongName='" +\
                str(item["LongName"]) + "'"
            # Instruments
            inscount = 0
            if("Instruments" in item):
                for ins in item["Instruments"]:
                    inscount += 1
                    ent_string += newline + "MERGE (Ins" + str(pcount) + str(inscount) +\
                    ":Instruments {ShortName:'" + str(ins["ShortName"]) + "'})"
                    if("LongName" in ins):
                        ent_string += " ON CREATE SET Ins" + str(pcount) + str(inscount) + \
                        ".LongName='" + str(ins["LongName"]) + "'" + \
                        newline + "ON MATCH SET Ins" + str(pcount) + str(inscount) + ".LongName='" +\
                        str(ins["LongName"]) + "'"
                    ent_string += newline + "MERGE (pf" + str(pcount) + ")-[:HAS]->(Ins" +\
                    str(pcount) + str(inscount) + ")"
                    if("ComposedOf" in ins):
                        comcount = 0
                        for com in ins["ComposedOf"]:
                            comcount += 1
                            ent_string += newline + "MERGE (com" + str(pcount) + str(inscount) + str(comcount) +\
                            ":ComposedOf {ShortName:'" + str(com["ShortName"]) + "', LongName:'" + \
                            str(com["LongName"]) + "'})"  + \
                            newline + "MERGE (Ins" + str(pcount) + str(inscount) + ")-[:HAS]->(com" +\
                            str(pcount) + str(inscount) + str(comcount) + ")"
                    ent_string += "MERGE (c)-[:HAS {viz_level:'E'}]->(Ins" + str(pcount) + str(inscount) + ")"
            # Connect Platform to collection if there is no instrument
            if(inscount == 0):
                ent_string += newline + "MERGE (c)-[:HAS {viz_level:'N'}]->(pf" + str(pcount) + ")"
            else:
                ent_string += newline + "MERGE (c)-[:HAS {viz_level:'0'}]->(pf" + str(pcount) + ")"
    # DataCenters
    if("DataCenters" in dict_obj):
        #Role is the connetion between DataCenter and Collection
        dccount = 0
        for dc in dict_obj["DataCenters"]:
            dccount += 1
            ent_string += newline + "MERGE (dc" + str(dccount) + ":DataCenters {ShortName:'" +\
            str(dc["ShortName"]) + "'})"
            if("LongName" in dc):
                ent_string += " ON CREATE SET dc" + str(dccount) + \
                ".LongName='" + str(dc["LongName"]) + "'" + \
                newline + "ON MATCH SET dc" + str(dccount) + ".LongName='" +\
                str(dc["LongName"]) + "'"
            #
            #TODO: ContactPersons/ContactInformation/ContactGroups
            #
            for ro in dc["Roles"]:
                ent_string += newline + "MERGE (c)-[:" + str(ro) + \
                " {viz_level:'N'}]->(dc" +str(dccount) + ")"
    
    # DataFormat
    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if((str(item["Name"]).lower() == "data format") and ("Value" in item)):
                # TODO: add an entity
                ent_string += newline + "MERGE (format:`Data Format` {ShortName:'" +\
                str(item["Value"]) + "'}) ON CREATE SET format.Description='" +\
                str(item["Description"]) + "', format.DataType='" +\
                str(item["DataType"]) + "'" + \
                newline + "MERGE (c)-[:HAS {viz_level:'N'}]->(format)"
    
    # ProcessingLevel - ProcessingLevelDescription should be an attribute of the relationship.
    if("ProcessingLevel" in dict_obj):
        ent_string += newline + "MERGE (pl:ProcessingLevel {Id:'" + \
        str(dict_obj["ProcessingLevel"]["Id"]) + "'})"
        if("ProcessingLevelDescription" in dict_obj["ProcessingLevel"]):
            ent_string += newline + "MERGE (c)-[:HAS {Description:'" +\
            str(dict_obj["ProcessingLevel"]["ProcessingLevelDescription"]) + \
            "', viz_level:'N'}]->(pl)" 
        else:
            ent_string += newline + "MERGE (c)-[:HAS {viz_level:'N'}]->(pl)"
    # MetadataLanguages
    if("MetadataLanguages" in dict_obj):
        ent_string += newline + "MERGE (ml:MetadataLanguages {Id:'" + \
        str(dict_obj["MetadataLanguages"]) + "'})" + \
        newline + "MERGE (c)-[:LANGUAGE {viz_level:'N'}]->(ml)"

    # CollectionDataType
    if("CollectionDataType" in dict_obj):
        ent_string += newline + "MERGE (cdt:CollectionDataType {Id:'" + \
        str(dict_obj["CollectionDataType"]) + "'})" + \
        newline + "MERGE (c)-[:DATATYPE {viz_level:'N'}]->(cdt)"


    # CollectionProgress - status as an entity - viewing this as a type of storing data
    if("CollectionProgress" in dict_obj):
        ent_string += newline + "MERGE (cp:CollectionProgress {Id:'" + \
        str(dict_obj["CollectionProgress"]) + "'})" + \
        newline + "MERGE (c)-[:PROGRESS_STATUS {viz_level:'N'}]->(cp)"

    cqlQuery += ent_string
    return cqlQuery, collection_concept_id

#Granule
def buildCypherfromjson_Granule(json_file):
    cqlQuery = ""
    file_obj = open(json_file)
    dict_obj = json.load(file_obj)
    granuleUR = dict_obj.pop('GranuleUR')
    set_words = "ON CREATE SET "  
    comma = ""  
    set_string = ""
    
    cqlQuery = "MERGE (g:Granule {" + \
    "GranuleUR:'" + granuleUR + "'}) "

    # TemporalExtents, SpatialExtent, ProviderDates, AdditionalAttribute, Projects, RelatedUrls, DataGranule
    if ("TemporalExtent" in dict_obj):
        if("RangeDateTime" in dict_obj["TemporalExtent"]):
            st = dict_obj["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
            en = dict_obj["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]
            set_string += comma + "g.`TemporalExtent.RangeDateTime`" + \
            "='" + str(st) + ", " + str(en) + "'"
            comma = ", "
        elif("SingleDateTime" in dict_obj["TemporalExtent"]):
            st = dict_obj["TemporalExtent"]["SingleDateTime"][0]
            en = dict_obj["TemporalExtent"]["SingleDateTime"][1]
            set_string += comma + "g.`TemporalExtent.SingleDateTime`='" + \
            str(st) + ", " + str(en) + "'"
            comma = ", "
    
    if ("SpatialExtent" in dict_obj):
        if("BoundingRectangles" in dict_obj ["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]):
            north = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["NorthBoundingCoordinate"]
            west = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["WestBoundingCoordinate"]
            east = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["EastBoundingCoordinate"]
            south = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["SouthBoundingCoordinate"]
            set_string += comma + "g.`SpatialExtent.BoundingRectangles`='" + \
            str(north) + ", " +  str(west) + ", " + str(east) + ", " + str(south) + "'"
            comma = ", "
        elif("GPolygons" in dict_obj ["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]):
            plgon = ""
            cm = ""
            for p in dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]["Boundary"]["Points"]:
                plgon += cm + str(p["Longitude"]) + ", " + str(p["Latitude"])
                cm = ", "            
            set_string += comma + "g.`SpatialExtent.GPolygons`='" + plgon + "'"
            comma = ", "
        
        #stype = dict_obj["SpatialExtent"]["SpatialCoverageType"]
        #ssystem = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["CoordinateSystem"]
        #sgranule = dict_obj["SpatialExtent"]["GranuleSpatialRepresentation"]
        #set_string += comma + "g.`SpatialExtent.SpatialCoverageType`='" + str(stype) + "'"
        #set_string += comma + "g.`SpatialExtent.CoordinateSystem`='" + str(ssystem) + "'"
        #set_string += comma + "g.`SpatialExtent.GranuleSpatialRepresentation`='" + str(sgranule) + "'"
    
    if("ProviderDates" in dict_obj):
        for item in dict_obj["ProviderDates"]:
            if(item["Type"] == "Update"):
                set_string += comma + "g.`ProviderDates.UPDATE`='" + str(item["Date"]) + "'"
                comma = ", "
            if(item["Type"] == "Insert"):
                set_string += comma + "g.`ProviderDates.INSERT`='" + str(item["Date"]) + "'"
                comma = ", "
    
    #TODO: Should turn this to entity
    if("Projects" in dict_obj):
        set_string += comma + "g.`Projects.ShortName`='" + str(dict_obj["Projects"][0]["ShortName"]) + "', " + \
        "g.`Projects.Campaigns`='" + str(dict_obj["Projects"][0]["Campaigns"]) + "'"
        comma = ", "
    
    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() != "data format"): # Dataformat is an entity
                set_string += comma + "g.`" + str(item["Name"]) + "`='" + \
                formatSpecialCharacter(str(item["Values"][0])) + "'"
                #for k in item.keys():
                #    if (k != 'Name' and k != 'Description'):
                #        set_string += comma + "g.`" + str(item["Name"]) + \
                #        "." + str(k) + "`='" + \
                #        str(item[k]) + "'"
                comma = ", "
        
    if("RelatedUrls" in dict_obj):
        count = 1
        for item in dict_obj["RelatedUrls"]:
            for k in item.keys():
                set_string += comma + "g.`RelatedUrl." + str(count) + "." + \
                str(k) + "`='" + formatSpecialCharacter(str(item[k])) + "'"
                comma = ", "
            count += 1
    
    if("DataGranule" in dict_obj):
        set_string += comma + "g.`DataGranule.DayNightFlag`='" +\
        str(dict_obj["DataGranule"]["DayNightFlag"]) + "', g.`DataGranule.ProductionDateTime`='" +\
        str(dict_obj["DataGranule"]["ProductionDateTime"]) + "'"
        if("ArchiveAndDistributionInformation" in dict_obj["DataGranule"]):
            for k in dict_obj["DataGranule"]["ArchiveAndDistributionInformation"].keys():
                set_string += ", g.`DataGranule.ArchiveAndDistributionInformation" + \
                str(k) + "`='" + str(dict_obj["DataGranule"]["ArchiveAndDistributionInformation"][k]) + "'"
        elif("Identifiers" in dict_obj["DataGranule"]):
            ic = 1
            for item in dict_obj["DataGranule"]:
                set_string += ", g.`DataGranule.Identifiers." + str(ic) + "`='" +\
                str(item["Identifier"]) + "', g.`DataGranule.Identifiers." + str(ic) +\
                ".Type`='" + str(item["IdentifierType"]) + "'"

    cqlQuery += set_words + set_string

    #Add entities
    # Platforms, AdditionalAttributes->Dataformat, MeasuredParameters, MetadataSpecification
    ent_string = ""
    newline = "\n"
    count = 0
    # Platform
    if("Platforms" in dict_obj):
        pcount = 0
        for item in dict_obj["Platforms"]:
            pcount += 1
            ent_string += newline + "MERGE (pf" + str(pcount) + \
            ":Platforms {ShortName:'" + str(item["ShortName"]) + "'})" 
            if("LongName" in item):
                ent_string += newline + "ON CREATE SET pf" + str(pcount) + \
                ".LongName='" + str(item["LongName"]) + "'" + \
                newline + "ON MATCH SET pf" + str(pcount) + ".LongName='" +\
                str(item["LongName"]) + "'"
            # Instruments
            inscount = 0
            if("Instruments" in item):
                for ins in item["Instruments"]:
                    inscount += 1
                    ent_string += newline + "MERGE (Ins" + str(pcount) + str(inscount) +\
                    ":Instruments {ShortName:'" + str(ins["ShortName"]) + "'})"
                    if("LongName" in ins):
                        ent_string += " ON CREATE SET Ins" + str(pcount) + str(inscount) + \
                        ".LongName='" + str(ins["LongName"]) + "'" + \
                        newline + "ON MATCH SET Ins" + str(pcount) + str(inscount) + ".LongName='" +\
                        str(ins["LongName"]) + "'"
                    ent_string += newline + "MERGE (pf" + str(pcount) + ")-[:HAS]->(Ins" +\
                    str(pcount) + str(inscount) + ")"
                    if("ComposedOf" in ins):
                        comcount = 0
                        for com in ins["ComposedOf"]:
                            comcount += 1
                            ent_string += newline + "MERGE (com" + str(pcount) + str(inscount) + str(comcount) +\
                            ":ComposedOf {ShortName:'" + str(com["ShortName"]) + "'})" + \
                            newline + "MERGE (Ins" + str(pcount) + str(inscount) + ")-[:HAS]->(com" +\
                            str(pcount) + str(inscount) + str(comcount) + ")"
                    ent_string += "MERGE (g)-[:HAS]->(Ins" + str(pcount) + str(inscount) + ")"
            # Connect Platform to collection if there is no instrument
            if(inscount == 0):
                ent_string += newline + "MERGE (g)-[:HAS]->(pf" + str(pcount) + ")"
        
    # DataFormat
    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() == "data format"):
                ent_string += newline + "MERGE (format:`Data Format` {ShortName:'" +\
                str(item["Values"][0]) + "'})" +\
                newline + "MERGE (g)-[:HAS]->(format)"
    
    #MeasuredParameters
    if("MeasuredParameters" in dict_obj):
        for item in dict_obj["MeasuredParameters"]:
            ent_string += newline + "MERGE (mp:MeasuredParameters {ParameterName:'" +\
            str(item["ParameterName"]) + "'})" +\
            newline + "MERGE (g)-[:HAS]->(mp)"

    #MetadataSpecification
    if("MetadataSpecification" in dict_obj):
        ent_string += newline + "MERGE (mds:MetadataSpecification {URL:'" +\
        str(dict_obj["MetadataSpecification"]["URL"]) + "', Name:'" +\
        str(dict_obj["MetadataSpecification"]["Name"]) + "', Version:'" +\
        str(dict_obj["MetadataSpecification"]["Version"]) + "'})" +\
        newline + "MERGE (g)-[:HAS]->(mds)"
    
    cqlQuery += ent_string

    #Add connection to the collection

    return cqlQuery, granuleUR

'''
Graph with Summarization 
'''
def buildCypherfromjson_Granule_Summarization(json_file):
    cqlQuery = ""
    file_obj = open(json_file)
    dict_obj = json.load(file_obj)
    granuleUR = dict_obj.pop('GranuleUR')
    set_words = "ON CREATE SET "  
    comma = ""  
    set_string = ""
    
    cqlQuery = "MERGE (g:Granule {" + \
    "GranuleUR:'" + granuleUR + "'}) "

    # TemporalExtents, SpatialExtent, ProviderDates, AdditionalAttribute, Projects, RelatedUrls, DataGranule
    
    if ("TemporalExtent" in dict_obj):
        if("RangeDateTime" in dict_obj["TemporalExtent"]):
            st = dict_obj["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
            en = dict_obj["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]
            set_string += comma + "g.`TemporalExtent.RangeDateTime`" + \
            "='" + str(st) + ", " + str(en) + "'"
            comma = ", "
        elif("SingleDateTime" in dict_obj["TemporalExtent"]):
            st = dict_obj["TemporalExtent"]["SingleDateTime"][0]
            en = dict_obj["TemporalExtent"]["SingleDateTime"][1]
            set_string += comma + "g.`TemporalExtent.SingleDateTime`='" + \
            str(st) + ", " + str(en) + "'"
            comma = ", "
    
    if ("SpatialExtent" in dict_obj):
        if("BoundingRectangles" in dict_obj ["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]):
            north = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["NorthBoundingCoordinate"]
            west = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["WestBoundingCoordinate"]
            east = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["EastBoundingCoordinate"]
            south = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]["SouthBoundingCoordinate"]
            set_string += comma + "g.`SpatialExtent.BoundingRectangles`='" + \
            str(north) + ", " +  str(west) + ", " + str(east) + ", " + str(south) + "'"
            comma = ", "
        elif("GPolygons" in dict_obj ["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]):
            plgon = ""
            cm = ""
            for p in dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]["Boundary"]["Points"]:
                plgon += cm + str(p["Longitude"]) + ", " + str(p["Latitude"])
                cm = ", "            
            set_string += comma + "g.`SpatialExtent.GPolygons`='" + plgon + "'"
            comma = ", "
        
        #stype = dict_obj["SpatialExtent"]["SpatialCoverageType"]
        #ssystem = dict_obj["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["CoordinateSystem"]
        #sgranule = dict_obj["SpatialExtent"]["GranuleSpatialRepresentation"]
        #set_string += comma + "g.`SpatialExtent.SpatialCoverageType`='" + str(stype) + "'"
        #set_string += comma + "g.`SpatialExtent.CoordinateSystem`='" + str(ssystem) + "'"
        #set_string += comma + "g.`SpatialExtent.GranuleSpatialRepresentation`='" + str(sgranule) + "'"
    
    if("ProviderDates" in dict_obj):
        for item in dict_obj["ProviderDates"]:
            if(item["Type"] == "Update"):
                set_string += comma + "g.`ProviderDates.UPDATE`='" + str(item["Date"]) + "'"
                comma = ", "
            if(item["Type"] == "Insert"):
                set_string += comma + "g.`ProviderDates.INSERT`='" + str(item["Date"]) + "'"
                comma = ", "
    
    #TODO: Should turn this to entity
    if("Projects" in dict_obj):
        set_string += comma + "g.`Projects.ShortName`='" + str(dict_obj["Projects"][0]["ShortName"]) + "', " + \
        "g.`Projects.Campaigns`='" + str(dict_obj["Projects"][0]["Campaigns"][0]) + "'"
        comma = ", "
    
    
    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() != "data format"): # Dataformat is an entity
                set_string += comma + "g.`" + str(item["Name"]) + "`='" + \
                formatSpecialCharacter(str(item["Values"][0])) + "'"
                comma = ", "
        
    if("RelatedUrls" in dict_obj):
        count = 1
        for item in dict_obj["RelatedUrls"]:
            for k in item.keys():
                set_string += comma + "g.`RelatedUrl." + str(count) + "." + \
                str(k) + "`='" + formatSpecialCharacter(str(item[k])) + "'"
                comma = ", "
            count += 1
    
    if("DataGranule" in dict_obj):
        set_string += comma + "g.`DataGranule.DayNightFlag`='" +\
        str(dict_obj["DataGranule"]["DayNightFlag"]) + "', g.`DataGranule.ProductionDateTime`='" +\
        str(dict_obj["DataGranule"]["ProductionDateTime"]) + "'"
        if("ArchiveAndDistributionInformation" in dict_obj["DataGranule"]):
            for k in dict_obj["DataGranule"]["ArchiveAndDistributionInformation"][0].keys():
                set_string += ", g.`DataGranule.ArchiveAndDistributionInformation" + \
                str(k) + "`='" + str(dict_obj["DataGranule"]["ArchiveAndDistributionInformation"][0][k]) + "'"
        elif("Identifiers" in dict_obj["DataGranule"]):
            ic = 1
            for item in dict_obj["DataGranule"]:
                set_string += ", g.`DataGranule.Identifiers." + str(ic) + "`='" +\
                str(item["Identifier"]) + "', g.`DataGranule.Identifiers." + str(ic) +\
                ".Type`='" + str(item["IdentifierType"]) + "'"

    cqlQuery += set_words + set_string

    #Add entities
    # Platforms, AdditionalAttributes->Dataformat, MeasuredParameters, MetadataSpecification
    ent_string = ""
    newline = "\n"
    count = 0
    # Platform
    if("Platforms" in dict_obj):
        pcount = 0
        for item in dict_obj["Platforms"]:
            pcount += 1
            ent_string += newline + "MERGE (pf" + str(pcount) + \
            ":Platforms {ShortName:'" + str(item["ShortName"]) + "'})" 
            if("LongName" in item):
                ent_string += newline + "ON CREATE SET pf" + str(pcount) + \
                ".LongName='" + str(item["LongName"]) + "'" + \
                newline + "ON MATCH SET pf" + str(pcount) + ".LongName='" +\
                str(item["LongName"]) + "'"
            # Instruments
            inscount = 0
            if("Instruments" in item):
                for ins in item["Instruments"]:
                    inscount += 1
                    ent_string += newline + "MERGE (Ins" + str(pcount) + str(inscount) +\
                    ":Instruments {ShortName:'" + str(ins["ShortName"]) + "'})"
                    if("LongName" in ins):
                        ent_string += " ON CREATE SET Ins" + str(pcount) + str(inscount) + \
                        ".LongName='" + str(ins["LongName"]) + "'" + \
                        newline + "ON MATCH SET Ins" + str(pcount) + str(inscount) + ".LongName='" +\
                        str(ins["LongName"]) + "'"
                    ent_string += newline + "MERGE (pf" + str(pcount) + ")-[:HAS]->(Ins" +\
                    str(pcount) + str(inscount) + ")"
                    if("ComposedOf" in ins):
                        comcount = 0
                        for com in ins["ComposedOf"]:
                            comcount += 1
                            ent_string += newline + "MERGE (com" + str(pcount) + str(inscount) + str(comcount) +\
                            ":ComposedOf {ShortName:'" + str(com["ShortName"]) + "'})" + \
                            newline + "MERGE (Ins" + str(pcount) + str(inscount) + ")-[:HAS]->(com" +\
                            str(pcount) + str(inscount) + str(comcount) + ")"                    
                    ent_string += "MERGE (g)-[:HAS {viz_level:'E'}]->(Ins" + str(pcount) + str(inscount) + ")"
            # Connect Platform to collection if there is no instrument
            if(inscount == 0):
                ent_string += newline + "MERGE (g)-[:HAS {viz_level:'N'}]->(pf" + str(pcount) + ")"
            else:
                ent_string += newline + "MERGE (g)-[:HAS {viz_level:'0'}]->(pf" + str(pcount) + ")"
    
    # DataFormat
    if("AdditionalAttributes" in dict_obj):
        for item in dict_obj["AdditionalAttributes"]:
            if(str(item["Name"]).lower() == "data format"):
                ent_string += newline + "MERGE (format:`Data Format` {ShortName:'" +\
                str(item["Values"][0]) + "'})" +\
                newline + "MERGE (g)-[:HAS {viz_level:'N'}]->(format)"

    #MeasuredParameters
    if("MeasuredParameters" in dict_obj):
        mpCount = 1
        for item in dict_obj["MeasuredParameters"]:
            ent_string += newline + "MERGE (mp" + str(mpCount) + ":MeasuredParameters {ParameterName:'" +\
            str(item["ParameterName"]) + "'})" +\
            newline + "MERGE (g)-[:HAS {viz_level:'N'}]->(mp" + str(mpCount) + ")"
            mpCount += 1

    #MetadataSpecification
    if("MetadataSpecification" in dict_obj):
        ent_string += newline + "MERGE (mds:MetadataSpecification {URL:'" +\
        str(dict_obj["MetadataSpecification"]["URL"]) + "', Name:'" +\
        str(dict_obj["MetadataSpecification"]["Name"]) + "', Version:'" +\
        str(dict_obj["MetadataSpecification"]["Version"]) + "'})" +\
        newline + "MERGE (g)-[:HAS {viz_level:'N'}]->(mds)"
    
    cqlQuery += ent_string
    return cqlQuery, granuleUR

def convert_umm_to_CVS(directory, output):
    return