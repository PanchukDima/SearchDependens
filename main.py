import psycopg2
import re

class pg_object():
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.schema = kwargs.get("schema")
        self.type = kwargs.get("type")
        self.deps = []
        self.body = None
        self.ignore = []#["pkg_global"]
        self.connStr = "host=127.0.0.1 dbname=postgres"


    def __dict__(self):
        return {
            "name": self.name,
            "schema": self.schema,
            "deps": self.deps,
            "type": self.type
        }
    def parseRegexpDeps(self):
        if self.body is not None:
            self.deps+= self.parseProceduresDeps()
            self.deps+= self.parseFunctionsDeps()

    def parseFunctionsDeps(self):
        l_raw_parse = re.findall(pattern=r'(select\s*(\w*)\.(\w*)\()', string=self.body, flags=re.I)
        l_raw_parse += re.findall(pattern=r'(from\s*(\w*)\.(\w*)\()', string=self.body, flags=re.I)
        l_raw_parse += re.findall(pattern=r'(:=\s*(\w*)\.(\w*)\()', string=self.body, flags=re.I)
        print(l_raw_parse)
        list_objects = []
        for parse_data in l_raw_parse:
            if parse_data[2] not in self.ignore:
                if parse_data[1].lower() == "call":
                    list_objects.append(pg_object(name=parse_data[2], schema="solution_med", type="function"))
                else:
                    list_objects.append(pg_object(name=parse_data[2], schema=parse_data[1], type="function"))
        return list_objects
    def parseProceduresDeps(self):
        l_raw_parse = re.findall(pattern=r'(CALL|call)\s*([\w]*)\.([\w]*)\(', string=self.body)
        list_objects = []
        for parse_data in l_raw_parse:
            if parse_data[2] not in self.ignore:
                if parse_data[1].lower() == "call":
                    list_objects.append(pg_object(name=parse_data[2], schema="solution_med", ))
                else:
                    list_objects.append(pg_object(name=parse_data[2], schema=parse_data[1], type="procedure"))
        return list_objects


    def getProcBody(self):
        db = psycopg2.connect(self.connStr)
        cur = db.cursor()
        l_sql = (f"""SELECT n.nspname AS schema
                          ,proname AS fname
                          ,proargnames AS args
                          ,t.typname AS return_type
                          ,d.description
                          ,pg_get_functiondef(p.oid) as definition
                      FROM pg_proc p
                      JOIN pg_type t
                        ON p.prorettype = t.oid
                      LEFT OUTER
                      JOIN pg_description d
                        ON p.oid = d.objoid
                      LEFT OUTER
                      JOIN pg_namespace n
                        ON n.oid = p.pronamespace
                     WHERE  n.nspname='{self.schema}'
                       AND proname='{self.name}'""")
        cur.execute(l_sql)
        data = cur.fetchone()
        if data is not None:
            self.body = data[5]
        db.close()


def recursive(pg_object):
    pg_object.getProcBody()
    pg_object.parseRegexpDeps()
    if len(pg_object.deps) > 0:
        for deps in pg_object.deps:
            recursive(deps)
    else:
        return

def recursiveView(pg_object, level=0):
    print('\t' * (level),
          f'Object_name: {pg_object.__dict__()["schema"]}.{pg_object.__dict__()["name"]} Type: {pg_object.__dict__()["type"]}')
    level +=1
    if len(pg_object.deps) > 0:
        for deps in pg_object.deps:
            recursiveView(deps, level)
    else:
        return

if __name__ == '__main__':
    obj = pg_object(schema="pkg_remd_egisz", name="get_all_cdadocuments_clob")
    #obj = pg_object(schema="public", name="insert_data14")
    recursive(obj)
    recursiveView(obj)
