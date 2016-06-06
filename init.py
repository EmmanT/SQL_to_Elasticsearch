import pymssql, datetime, decimal, string, sys, urllib, simplejson, time, hashlib
from json import dumps,loads, JSONEncoder
from elasticsearch import Elasticsearch, helpers

# Elasticsearch server
es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])

# variable declaration for the connection string
vhost = 'Sql.Sql.com'
vuser = 'Domain\User'
vpass = 'abc123'
vdb = 'database'

class PythonObjectEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, (decimal.Decimal)):
            return float(obj)
        return str(obj)

def SQL_bulk_store(anyid=None):
    conn = pymssql.connect(host=vhost, user=vuser, password=vpass, database=vdatabase, as_dict=True)
    cur = conn.cursor()
    jsondicts = []
    rowcnt = 0
    rowinc = 0
    cur.execute("SELECT * FROM yourTable WHERE  yourid >= %s", anyid)


    #anyonymous function to convert a null value into something else
    #for this example convert it to ''
    strconv = lambda val: str('') if val is None else str(val)

    for row in cur:
        rowcnt = rowcnt+1
        rowinc = rowinc+ 1

        # Hash certain records to create a unique id for json
        hashid = hashlib.md5(row['field1'] + row['field2'] + row['field3'] + row['field4'] + \
                             strconv(row['field5']))

        ## encode data properly
        jsondata = dumps(row,cls=PythonObjectEncoder)


        esjson = {
            "_index": 'IndexName',
            "_type": 'DocTypeName',
            "_id": hashid.hexdigest(),
            "_source": jsondata
        }
        jsondicts.append(esjson)

        #Bulk Insert which is using Generators
        if rowcnt == 2000:
            helpers.bulk(client = es, actions =jsondicts,stats_only='True')
            rowcnt=0
            print 'Now at ',rowinc, ' recordeset.'
            jsondicts=[]
    conn.close()
    helpers.bulk(client = es, actions =jsondicts,stats_only='True')


if __name__ == "__main__":
    print datetime.datetime.now(), ' Storing data.'
    print 'Starting to execute at', anyid

    #This allows to find the last data id in your doc_type and load to filter your sql selection
    anyid = es.search(index="IndexName", doc_type="DocTypeName",
                         body={"query": {
                                "match_all": {}
                            },
                         "size": 0,
                         "aggs": {
                            "1":{
                                "max": {
                                    "field" : "FieldName"
                                }
                            }
                        }
                         }, ignore=[400, 404])['aggregations']['1']['value_as_string']

    SQL_bulk_store(anyid)
    print datetime.datetime.now(), ' 100% Complete'