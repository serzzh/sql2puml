# import pyodbc
import sys
import getopt

from tabledef import Tabledef
from columndef import Columndef
from relationdef import Relationdef

"""
def Connect(driverOverride, server, host, port, dbname, username, password):

    if server == 'mssql':
        driver = '{SQL Server}'
        if driverOverride != '':
            driver = driverOverride
        
        if (port != '1433'):
            thePort = ',' + port
        else:
            thePort = ''

        if username == '':
            #Driver={SQL Server};Server=myServerAddress;Database=myDataBase;Trusted_Connection=Yes;
            connstr = (f'Driver={driver};Server={host}{thePort};Database={dbname};Trusted_Connection=yes;')
        else:
            # Driver={SQL Server};Server=myServerAddress;Database=myDataBase;Uid=myUsername;Pwd=myPassword;
            connstr = (f'Driver={driver};Server={host}{thePort};Database={dbname};Uid={username};Pwd={password};')
    elif server == 'mysql':
        # Driver={MySQL ODBC 8.0 Unicode Driver};Server=myServerAddress;Port=3306;Database=myDataBase;User=myUsername;Password=myPassword;Option=3;
        driver = '{MySQL ODBC 8.0 Unicode Driver}'
        if driverOverride != '':
            driver = driverOverride
        connstr = (f'Driver={driver};Server={host};Port={port};Database={dbname};User={username};Password={password};Option=3;')
    
    connection = pyodbc.connect(connstr)
    return connection


def Disconnect(connection) -> None:
    connection.close()
"""

def EmitPumlHeader(dbname, zerorows):
    print(f'@startuml {dbname}\n')
    print('skinparam Linetype ortho\n')
    if zerorows:
        if zerorows == 'show':
            print("""hide stereotype
hide circle            
skinparam class<<empty>> {
  backgroundColor #FFF
  borderColor #CCC
  fontColor #CCC
}""")
        else:
            print(f"{zerorows} <<empty>>\n")


def EmitPumlFooter():
    print('\n@enduml')


# Use table name lowercased and with spaces replaced by underscores
def PumlName(sqlName) -> str:
    pumlName = sqlName.lower().replace(' ', '_')
    return pumlName

def EmitTableHeader(tablename, rowcount):    
    stereotype = " <<empty>> " if rowcount is not None and rowcount == 0 else ""
    print('entity "' + tablename + '" as ' + PumlName(tablename) + stereotype + ' {')


def EmitTableDef(connection, table:Tabledef):
    line = ''
    seperated = False

    for columnName, column in table.Columns.items():
        if column.IsKey == False:               # Put out primary key seperator
            if seperated == False:              # before the first non primary key column
                print('\t--')
                seperated = True

        if column.IsMandatory:
            line = '\t* '
        else:
            line = '\t'
        
        line = line + column.Name
        if column.IsUnique:
            line = line + '*'

        line = line + ':' + column.Datatype
        print(line)


def EmitTableFooter():
    print('}\n')


def EmitTable(connection, table:Tabledef):
    EmitTableHeader(table.Name, table.RowCount)
    EmitTableDef(connection, table)
    EmitTableFooter()

def EmitRelations(connection, table:Tabledef, colnames):
    for name, rel in table.Relationships.items():
        names=''
        if colnames == True:
            names = f' : {rel.PrimaryColumn.Name}  = {rel.ForeignColumn.Name}'
        print(f'{PumlName(rel.PrimaryTable.Name)} {rel.PumlRelation} {PumlName(rel.ForeignTable.Name)}{names}')


def printStderr(*a): 
	# Here a is the array holding the objects 
	# passed as the arguement of the function 
	print(*a, file = sys.stderr) 

"""
def PrintUsage():
    printStderr('Usage: python sql2puml.py OPTIONS [FILE]')
    printStderr('OPTIONS')    
    printStderr('\t-d, --database <database name>\tName of database to get diagram for')    
    printStderr('\t[-s, --schema <schema name>]\tName of schema within the database, default dbo with SQL Server')
    printStderr('\t[-S, --server <RDBMS Server>]\tSupply one of mssql, mysql. Default is mssql')
    printStderr('\t[-h, --host <server name>]\tServer to connect to, default localhost')
    printStderr('\t[-p, --port <SQL listen port>]\tPort to connect to, default 1433')
    printStderr('\t[-o, --out <output filename>]\tFilename to save output to, default write to console')
    printStderr('\t[-u, --user <username>]\tUsername to connect as')
    printStderr('\t[-P, --password <password>]\tPassword to connect with')
    printStderr('\r[-n, --names\tInclude column names on relationships')
    printStderr('\t[-z, --zerorows <mode>]\tSupply one of show, hide, remove. Default is None, i.e., empty tables appear normally')
    printStderr('\nExample: python sql2puml.py -server localhost -port 1433 -dbname pubs -schema dbo')

"""

# Redifine init class method for reference type definition (one-one, one-many, many-one, etc)
class MyRel(Relationdef):

   def __IsOne(self, table, col):
       return False
       

   def __init__(self, name:str, primaryTable, primaryColumn:Columndef, foreignTable, foreignColumn:Columndef):
        self.Name = name
        self.PrimaryTable = primaryTable
        self.PrimaryColumn = primaryColumn
        self.ForeignTable = foreignTable
        self.ForeignColumn = foreignColumn
        self.PumlRelation = ''

        # Work out the relatioship type
        # Zero or One   |o--    --o|
        # Exactly One   ||--    --||
        # Zero or Many  }o--    --o{
        # One or Many   }|--    --|{


        # Find out if the relationship is mandatory "|" or optional "o" at either end
        if primaryColumn.IsMandatory:
            pmin = '|'
        else:
            pmin = 'o'

        if primaryColumn.IsKey:
            pmax = '|'
        else:
            pmax = '}'

        primary = pmax + pmin

        if foreignColumn.IsMandatory:
            fmin = '|'
        else:
            fmin =  'o'

        if primaryColumn.IsKey:
            fmax = '|'
        else:
            fmax = '|'

        foreign = fmin + fmax
        self.PumlRelation = primary + '--' + foreign
        
def schema_filter(x):
    return not x.startswith("pg_")


def main(argv) -> None:
    """
    server = 'mssql'
    host = 'localhost'
    port = '1433'
    dbname = ''
    schema = ''
    filename = ''
    conn = None
    fileHandle = None
    username = ''
    password=''
    driver = ''
    zerorows = None
    colnames = False


    try:
        opts, args = getopt.getopt(argv, 'S:d:s:h:p:o:u:P:D:z:n', ['server=','database=','schema=','host=','port=','out=','user=','password=','driver=','zerorows=','names'])
        for opt, arg in opts:
            if opt in ('-S', '--server'):
                server = arg.lower()
            elif opt in ('-d', '--database'):
                dbname = arg 
            elif opt in ('-s', '--schema'):
                schema = arg
            elif opt in ('-h', '--host'):
                host = arg
            elif opt in ('-p', '--port'):
                port = arg
            elif opt in ('-o', '--out'):
                filename = arg
            elif opt in ('-u', '--user'):
                username = arg
            elif opt in ('-P', '--password'):
                password = arg
            elif opt in ('-n', '--names'):
                colnames = True
            elif opt in ('-D', '--driver'):
                driver = arg
            elif opt in ('-z', '--zerorows'):
                zerorows = arg

        if filename != '':            
            original_stdout = sys.stdout
            fileHandle = open(filename, 'w')                # Change the standard output to filename
            sys.stdout = fileHandle

        if dbname == '':
            raise ValueError('No database name supplied')

        if server == 'msql':
            schema = ''

        if server == 'mssql' and schema == '':
            schema = 'dbo'


        conn = Connect(driver, server, host, port, dbname, username, password)
        tables = Tabledef.Get(conn, schema)
        EmitPumlHeader(dbname, zerorows)
        for name, table in tables.items():
            EmitTable(conn, table)

        for name, table in tables.items():
            EmitRelations(conn, table, colnames)

        EmitPumlFooter()      
        
    except getopt.GetoptError:
        PrintUsage()

    except ValueError as ve:
        PrintUsage()

    except Exception as e:
        printStderr(f'EXCEPTION: {e.args[1]}')
    
    finally:
        if fileHandle != None:
            sys.stdout = fileHandle

        if conn != None:
            Disconnect(conn)
    """

    # New code from here
    import os, csv, json

    input_dir = "input"
    files = os.listdir(input_dir)
    print (files)

    output_dir = "output"
    db_structure = []

    filter_file = "filter/db_structure_new.csv"
    # Read table for filtering
    with open(filter_file, 'r') as ffile:
        filter_table = {}
        csv_reader = csv.reader(ffile, delimiter=";")
        next(csv_reader, None) # Skip csv header    
        for row in csv_reader:
            filter_table[row[2]] = row[5]

    for filename in files:

        input_file = os.path.join(input_dir, filename)

        with open(input_file, 'r') as mfile:
            tables = {}
            csv_reader = csv.reader(mfile)
            next(csv_reader, None) # Skip csv header
            for row in csv_reader:
                # Define table
                db_name = row[0]
                tab_name = row[2]
                col = json.loads(row[4])
                schema_name = row[1]
                query_name = row[3]


                if schema_filter(schema_name) and not filter_table[tab_name]:
                    
                    
                    # Write table data to objects                   
                    if tab_name in tables:
                        table = tables[tab_name]
                    else:
                        table = Tabledef(tab_name)
                        tables[tab_name] = table
                        table.Columns = {}
                    
 

                    if col["column_name"] not in table.Columns:
                        table.Columns[col["column_name"]] = Columndef(col["column_name"], 1, "undef", 0, tab_name)
                        column = table.Columns[col["column_name"]]
                        column.Name = col["column_name"]
                        column.Datatype = col["datatype"]
                        column.IsMandatory = True if col["is_required"] == "NOT NULL" else False
                        column.IsKey = True if col["PK"] == "PK" else False
                        column.IsUnique = True if col["PK"] == "PK" else False
                        column.IsCompositeKey = False  # True if this column is part of a composite primary key
                        column.Parent = ''

                        # Update db infostructure 
                        db_structure.append([
                            db_name, 
                            schema_name, 
                            tab_name,
                            col["table_comment"], 
                            column.Name,
                            col["column_comment"], 
                            column.Datatype,
                            column.IsMandatory,
                            column.IsKey,
                            column.IsUnique
                            ]
                        )
                    
                    table.Columns = dict(sorted(table.Columns.items(), key=lambda item: item[1].IsKey, reverse=True))

                    

        with open(input_file, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader, None) # Skip csv header
            for row in csv_reader:
                # Define table
                db_name = row[0]
                tab_name = row[2]
                col = json.loads(row[4])
                schema_name = row[1]
                query_name = row[3]

                if schema_filter(schema_name) and not filter_table[tab_name]:
                    # Relationships
                    if query_name == "2_fks":
                        if col["constraint_name"] not in table.Relationships and col["constraint_type"] == "FOREIGN KEY" and not filter_table[col["foreign_table_name"]]:
                            _ftn = col["foreign_table_name"]
                            print(col["constraint_name"], tab_name, col["foreign_table_name"])
                            tables[tab_name].Relationships[col["constraint_name"]] = MyRel(
                                # connectionCursor = "",
                                name = col["constraint_name"], 
                                primaryTable = tables[tab_name],
                                primaryColumn = tables[tab_name].Columns[col["column_name"]],        
                                foreignTable = tables[_ftn],
                                foreignColumn = tables[_ftn].Columns[col["foreign_column_name"]],
                                )

        
        # Remove id-parent_id

        # Remove h-relations
        
        # Remove h-tables



        # Remove field attributes table
        
        
        # Output to PlantUML
        base_name, _ = os.path.splitext(filename)
        output_file = base_name + ".puml"

        if output_file != '':            
            original_stdout = sys.stdout
            fileHandle = open(os.path.join(output_dir, output_file) , 'w')                # Change the standard output to filename
            sys.stdout = fileHandle
        
        EmitPumlHeader(db_name, False)

        for name, table in tables.items():
            EmitTable("", table)

        for name, table in tables.items():
            EmitRelations("", table, True)

        EmitPumlFooter()


    # Save database infostructure to CSV

    db_structure_file =  os.path.join(output_dir, "db_structure.csv")
    fields = [
        "db_name", 
        "schema_name", 
        "table_name",
        "table_comment", 
        "column_name",
        "column_comment", 
        "column_datatype",
        "column_isMandatory",
        "column_isKey",
        "column_isUnique"       
    ]

    with open(db_structure_file, 'w') as f:
        
        # using csv.writer method from CSV package
        write = csv.writer(f)
        
        write.writerow(fields)
        write.writerows(db_structure)
    
    # End new code      


if __name__ == '__main__':
    main(sys.argv[1:])
