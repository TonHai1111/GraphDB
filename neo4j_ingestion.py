from neo4j import GraphDatabase
import json
from tools import *

class DatabaseConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    #@staticmethod
    def createNode(self, nodeLable, properties):
        #create Cyper Query Language (CQL) string
        cqlQuery = "CREATE (node:" + nodeLable #Cyper query language
        if (len(properties) > 0):
            cqlQuery += " { "
            comma = ""
            for u, v in properties.items():
                cqlQuery += comma + str(u) + ": '" + str(v) + "' "
                comma = ", "
            cqlQuery += " } "
        cqlQuery += ")"
        print(cqlQuery)
        with self.driver.session() as session:
            result = session.run(cqlQuery)
        return result

    ''' Input descriptions
     Node's format:
     [
        [{
            "label": "<Label node 1>"
        },
        {
            "properties": {"<property>": "<Value>", "<property>": "<value>"}
        }]
        ,
        [{
            "label": "<Lable node 2>"
        },
        {
            "properties": {"<property>": "<value>", "<property>": "<value>", "<property>": "<value>"}
        }]    
     ]
    '''
    def createNodes(self, nodes): #many sessions
        if (len(nodes) > 0):
            for l, p in nodes:
                self.createNode(l["label"], p["properties"])
        return

    def createNodes_v2(self, nodes): #one session
        if (len(nodes) > 0):
            cqlQuery = "CREATE "
            index = 1
            commaN = ""
            for l, p in nodes:
                cqlQuery += commaN + "(node" + str(index) + ":" + str(l["label"])
                if (len(p["properties"]) > 0):
                    cqlQuery += " { "
                    commaP = ""
                    for u, v in p["properties"].items():
                        cqlQuery += commaP + str(u) + ": '" + str(v) + "' "
                        commaP = ", "
                    cqlQuery += " } "
                index += 1
                cqlQuery += ")"
                commaN = ", "
            print(cqlQuery)
            with self.driver.session() as session:
                result = session.run(cqlQuery)
        return

    ''' relationship template
    [{
        "type": "<Relationship type>"
    },
    {
        "properties": {"<property>": "<Value>", "<property>": "<value>"}
    }]
    '''
    def createRelationship(self, label1, label2, id1, id2, relationship):
        cqlQuery = "MATCH (a:" + str(label1) + "), (b:" + str(label2) + ") "
        cqlQuery += "WHERE a.id = '" + str(id1) + "' and b.id = '" + str(id2) + "' "
        t, p = relationship
        cqlQuery += "CREATE (a)-[r:" + t["type"] 
        if (len(p["properties"]) > 0):
            cqlQuery += " { "
            commaP = ""
            for u, v in p["properties"].items():
                cqlQuery += commaP + str(u) + ": '" + str(v) + "' "
                commaP = ", "
            cqlQuery += " } "
        cqlQuery += "]->(b)"
        print (cqlQuery)
        with self.driver.session() as session:
            result = session.run(cqlQuery)
        return
    
    #Json template
    def loadJSON_file(self, json_file, collection_concept_id):
        #cqlQuery, cID = buildCypherfromjson(json_file)
        cqlQuery, cID = buildCypherfromjson_Summarization(json_file, collection_concept_id)
        #fout = open('cqlQuery1.txt', 'w')
        #fout.write(cqlQuery)
        #fout.close()
        with self.driver.session() as session:
            result = session.run(cqlQuery)
        return cID

    # Load from list of json files in a directory
    def loadJSON_dir(self, dir):
        filenames = []
        f = get_all_full_filenames(dir)
        filenames.extend(f)
        count = 0
        for item in filenames:
            print(item)
            cname = item[-26:-5]
            self.loadJSON_file(item, cname)
            count += 1
        print (count)
        return
    
    #Granule
    def loadGranuleJSON_file(self, json_file):
        #cqlQuery, g_UR = buildCypherfromjson_Granule(json_file)
        cqlQuery, g_UR = buildCypherfromjson_Granule_Summarization(json_file)
        #fout = open('cqlQuery1.txt', 'w')
        #fout.write(cqlQuery)
        #fout.close()
        with self.driver.session() as session:
            result = session.run(cqlQuery)
        return g_UR
    
    def createRelationship(self, granuleUR, collection_concept_id):
        #cqlQuery = buildCypherfromjson_Granule(json_file)
        cqlQuery = buildCypherRelationship(granuleUR, collection_concept_id)
        #fout = open('cqlQuery2.txt', 'w')
        #fout.write(cqlQuery)
        #fout.close()
        with self.driver.session() as session:
            result = session.run(cqlQuery)
        return

    def createRelationship_Summarization(self, granuleUR, collection_concept_id):
        #cqlQuery = buildCypherfromjson_Granule(json_file)
        cqlQuery = buildCypherRelationship_Summarization(granuleUR, collection_concept_id)
        #fout = open('cqlQuery2.txt', 'w')
        #fout.write(cqlQuery)
        #fout.close()
        with self.driver.session() as session:
            result = session.run(cqlQuery)
        return

    def loadGranuleJSON_dir(self, dir, collection_concept_id):
        filenames = []
        f = get_all_full_filenames(dir)
        filenames.extend(f)
        count = 0
        for item in filenames:
            print(item)
            g_UR = self.loadGranuleJSON_file(item)
            self.createRelationship_Summarization(g_UR, collection_concept_id)
            count += 1
        print (count)
        return

    #TODO
    def loadCVS(self, cvs_file):
        return
    
    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self.create_greeting, message)
            print(greeting)
    @staticmethod
    def create_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from note ' + id(a)", message=message)
        return result.single()[0]

    
    #def createRelationship(self):
    #    return
    
def test_CreateNode():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "tonhai"
    con = DatabaseConnection(uri, user, password)
    #con.print_greeting("Hello Hai!")
    a = [[{"label": "HaiNode2"}, {"properties": {'name': 'Jim', 'id': 789, 'dept': "CS"}}],
    [{"label": "HaiNode2"}, {"properties": {'name': 'John', 'id': 456}}]]
    b = [{"type": "Connection"}, {"properties": {"Distance": "20 miles", "Direction": "one way"}}]
    #res = con.createNode("HaiNode1", {'name': 'Jay', 'id': 123})
    #con.createNodes_v2(a)
    con.createRelationship("HaiNode2", "HaiNode2", "789", "456", b)
    #print(res)
    con.close()
    return

def test_readJSON():
    uri = "bolt://localhost:7687"
    
    user = "neo4j"
    password = "tonhai"
    con = DatabaseConnection(uri, user, password)
    #con.loadJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200000492-NASA_MAAP.json')
    #/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200015072-ESA_MAAP.json
    con.loadJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200015072-ESA_MAAP.json')
    #/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200125169-NASA_MAAP.json
    #con.loadJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200125169-NASA_MAAP.json')
    #/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200231011-NASA_MAAP.json
    #con.loadJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/C1200231011-NASA_MAAP.json')
    con.close()
    return

def test_readJSON_dir():
    uri = "bolt://localhost:7687"
    
    user = "neo4j"
    password = "tonhai"
    con = DatabaseConnection(uri, user, password)
    con.loadJSON_dir('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Collections/')
    con.close()
    return

def test_readJSON_Granule():
    uri = "bolt://localhost:7687"    
    user = "neo4j"
    password = "tonhai"
    con = DatabaseConnection(uri, user, password)
    #collection_concept_id = "C1200120114-NASA_MAAP"
    #g_UR = con.loadGranuleJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/C1200120114-NASA_MAAP/G1200124804-NASA_MAAP.json')
    #g_UR = con.loadGranuleJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/C1200120114-NASA_MAAP/G1200124805-NASA_MAAP.json')
    g_UR = con.loadGranuleJSON_file('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/C1200120114-NASA_MAAP/G1200124806-NASA_MAAP.json')
    con.createRelationship_Summarization(g_UR,"C1200120114-NASA_MAAP")
    con.close()
    return

def test_readJSON_Granule_dir():
    uri = "bolt://localhost:7687"    
    user = "neo4j"
    password = "tonhai"
    clist = ["C1200110729-NASA_MAAP", "C1200120114-NASA_MAAP", "C1200015072-ESA_MAAP", 
            "C1200015073-ESA_MAAP", "C1200125308-NASA_MAAP", "C1200110768-NASA_MAAP", 
            "C1200166513-NASA_MAAP", "C1200110748-NASA_MAAP", "C1200116827-NASA_MAAP", 
            "C1200235708-NASA_MAAP", "C1200015069-ESA_MAAP", "C1200125169-NASA_MAAP", 
            "C1200115768-NASA_MAAP", "C1200231011-NASA_MAAP", "C1200015149-NASA_MAAP", 
            "C1200015188-NASA_MAAP", "C1200109548-NASA_MAAP", "C1200120106-NASA_MAAP", 
            "C1200109243-NASA_MAAP", "C1200000521-NASA_MAAP", "C1200231029-NASA_MAAP", 
            "C1200109237-NASA_MAAP", "C1200015150-NASA_MAAP", "C1200090708-NASA_MAAP", 
            "C1200125145-NASA_MAAP", "C1200015068-NASA_MAAP", "C1200235707-NASA_MAAP", 
            "C1200166514-NASA_MAAP", "C1200125168-NASA_MAAP", "C1200235694-NASA_MAAP", 
            "C1200231031-NASA_MAAP", "C1200097475-NASA_MAAP", "C1200125309-NASA_MAAP", 
            "C1200000308-NASA_MAAP", "C1200231030-NASA_MAAP", "C1200116820-NASA_MAAP", 
            "C1200109244-NASA_MAAP", "C1200097474-NASA_MAAP", "C1200090741-NASA_MAAP", 
            "C1200000522-NASA_MAAP", "C1200000492-NASA_MAAP", "C1200109245-NASA_MAAP", 
            "C1200110728-NASA_MAAP", "C1200117888-NASA_MAAP", "C1200116818-NASA_MAAP", 
            "C1200125288-NASA_MAAP", "C1200015148-NASA_MAAP", "C1200231010-NASA_MAAP", 
            "C1200110769-NASA_MAAP", "C1200109238-NASA_MAAP", "C1200090707-NASA_MAAP"]
    
    #con.loadGranuleJSON_dir('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/C1200120106-NASA_MAAP/', 'C1200120106-NASA_MAAP')
    #con.loadGranuleJSON_dir('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/C1200116827-NASA_MAAP/', 'C1200116827-NASA_MAAP')
    #con.loadGranuleJSON_dir('/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/C1200110768-NASA_MAAP/', 'C1200110768-NASA_MAAP')
    for item in clist:
        con = DatabaseConnection(uri, user, password)
        dir = "/home/tonhai/MAAP/CMR/graphDB-CMR/Data/Granules/" + str(item) + "/"
        con.loadGranuleJSON_dir(dir, str(item)) 
        con.close()
    return

if __name__ == "__main__":
    print("Hello")
    #test_CreateNode()
    #test_readJSON()
    #test_readJSON_dir()
    #test_readJSON_Granule()
    test_readJSON_Granule_dir()
    print("Done!")