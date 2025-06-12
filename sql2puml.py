# import pyodbc
import sys
import getopt

from tabledef import Tabledef
from columndef import Columndef
from relationdef import Relationdef
from dict import dict_prefix
import logging
logger = logging.getLogger(__name__)

# Redefine table class
class MyTable(Tabledef):

    def __init__(self, name:str, db_name, schema, comment=None, module=None, excl=False, alias_name=None):
        self.Name = name
        self.Columns = {}
        self.Relationships = {}      # Relationdef[]
        self.CompositePK = False    
        self.RowCount = None
        # self.table_del_list = {}
        self.LongName = module + "." + name
        self.has_links = False
        self.AliasName = alias_name
        self.Excl = excl
        self.Module = module
        self.DbName = db_name
        self.Schema = schema
        self.Comment = comment
        
    
    def relink_h(self):    
        for name, rel in self.Relationships.items():

            # remove h-tables
            if hasattr(rel, 'ForeignTable'):
                # .info(name)
                if hasattr(rel.ForeignTable, 'Alias') and not rel.ForeignTable.Name.endswith(('_s', '_ss')):
                    self.table_del_list[rel.ForeignTable.Name] = rel.ForeignTable.Alias.Name
                    rel.ForeignTable = rel.ForeignTable.Alias
                
                if hasattr(rel.ForeignColumn, 'AliasColumn'):               
                    rel.ForeignColumn = rel.ForeignColumn.AliasColumn

    def relink_l(self):
        for name, rel in self.Relationships.items():           
            # remove l-tables          
            
            
            if hasattr(rel, 'PrimaryTable'):
                if rel.PrimaryTable.Name.endswith('_l'):

                    self.table_del_list[rel.PrimaryTable.Name] = rel.ForeignTable.Name
                    self.Relationships[name]  = MyRel(
                        name = name,
                        primaryTable = rel.ForeignTable,
                        primaryColumn = rel.ForeignColumn,
                        foreignTable = rel.PrimaryTable.Reverse.Alias,
                        foreignColumn = rel.PrimaryColumn.ReverseColumn.AliasColumn,
                        )
                    rel = self.Relationships[name] 

    # del loop links
    def del_loop(self):
        del_list = []
        for name, rel in self.Relationships.items():
            if rel.PrimaryTable.Name == rel.ForeignTable.Name:
                del_list.append(name)

        for name in del_list:
            del(self.Relationships[name])

    def del_side_h_tables(self):
        side_links = []
        
        for name, rel in self.Relationships.items():
            if rel.PrimaryTable.Name.endswith(('_h', '_hh')):
                side_links.append(name)
                self.table_del_list[rel.PrimaryTable.Name] = "side_h_p"
            if rel.ForeignTable.Name.endswith(('_h', '_hh')):
                side_links.append(name)
                self.table_del_list[rel.ForeignTable.Name] = "side_h_f"
        
        for name in side_links:
            del(self.Relationships[name])






# Redefine init class method for reference type definition (one-one, one-many, many-one, etc)

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
        self.one2one_pk = primaryColumn.IsKey
        self.one2one_fk = foreignColumn.IsKey


        if self.one2one_pk:
            foreignTable.Alias = primaryTable
            foreignColumn.AliasColumn = primaryColumn
        
        if self.one2one_fk:
            primaryTable.Reverse = foreignTable
            primaryColumn.ReverseColumn = foreignColumn

        primaryTable.has_links = True
        foreignTable.has_links = True


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
    
       
def EmitPumlHeader(dbname, zerorows):
    print(f'@startuml {dbname}\n')
    print('left to right direction')
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
    print('entity "' + tablename.split('.')[-1] + '" as ' + PumlName(tablename) + stereotype + ' {')


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


def EmitTable(connection, table:MyTable):
    EmitTableHeader(table.LongName, table.RowCount)
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


 
def schema_filter(x):
    return not x.startswith("pg_")


def main(argv) -> None:

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger.info('Started')


    # New code from here
    import os, csv, json

    input_dir = "input"
    files = os.listdir(input_dir)

    output_dir = "output"
    db_structure = []
    tables = {}       

    filter_file = "filter/db_structure_new.csv"
    # Creating table objects according to db_structure file
    with open(filter_file, 'r') as ffile:
        csv_reader = csv.reader(ffile, delimiter=";")
        next(csv_reader, None) # Skip csv header    
        for row in csv_reader:
            tables[row[2]] = MyTable(
                name=row[2],
                db_name=row[0],
                schema=row[1],
                comment=row[3],
                module=row[5],
                alias_name=row[8].replace("#N/A", ""))
            
            tables[row[2]].Excl = True if row[6]=='X' else False     

    
    # only ZIIoT
    ziiot_files = [x for x in files if x.startswith('mes_conf__zif')]

    
    # Output to PlantUML
    # base_name, _ = os.path.splitext(filename)


    for filename in ziiot_files:

        input_file = os.path.join(input_dir, filename)
        base_name, _ = os.path.splitext(filename)
        base_name = base_name.replace("_metadata","")

        with open(input_file, 'r') as mfile:

            csv_reader = csv.reader(mfile)
            next(csv_reader, None) # Skip csv header
            for row in csv_reader:
                # Define table
                db_name = row[0]
                tab_name = row[2]
                col = json.loads(row[4])
                schema_name = row[1]
                query_name = row[3]


                if schema_filter(schema_name):
                                       
                    # Write table data to objects                   
                    if tab_name in tables:
                        table = tables[tab_name]

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

                if tab_name in tables.keys():
                    # Relationships
                    table = tables[tab_name]

                    if query_name == "2_fks" and (table.Excl is False or tables[tab_name].AliasName):
                        
                        table = tables[tab_name] \
                        if not tables[tab_name].AliasName \
                            else tables[tables[tab_name].AliasName]
                        
                        foreign_table = tables[col["foreign_table_name"]] \
                        if not tables[col["foreign_table_name"]].AliasName \
                            else tables[tables[col["foreign_table_name"]].AliasName]
                        
                        column = table.Columns[col["column_name"]] \
                            if col["column_name"] in table.Columns \
                            else None
                        
                        foreign_column = foreign_table.Columns[col["foreign_column_name"]] \
                            if col["foreign_column_name"] in foreign_table.Columns \
                            else None

                        if col["constraint_name"] not in table.Relationships and col["constraint_type"] == "FOREIGN KEY" \
                            and column is not None and foreign_column is not None:
                            table.Relationships[col["constraint_name"]] = MyRel(
                                name = col["constraint_name"], 
                                primaryTable = table,
                                primaryColumn = column,        
                                foreignTable = foreign_table,
                                foreignColumn = foreign_column,
                            )

    # Remove field attributes table
    
    modules = {}

    for name, table in tables.items():           
        table.del_loop()
        group = ".".join(table.Module.split(sep=".")[0:2])
        modules[group] = [table.Name] if group not in modules.keys() else modules[group] + [table.Name]
           
    # .info(modules)
    
    
    for module, tablist in modules.items():

        output_file = module + ".puml"


        if output_file != '':            
            original_stdout = sys.stdout
            fileHandle = open(os.path.join(output_dir, output_file) , 'w')                # Change the standard output to filename
            sys.stdout = fileHandle
        
        EmitPumlHeader(module, False)
        
        for tabname in tablist:
            if not tables[tabname].Excl:
                EmitTable("", tables[tabname])
        
        for tabname in tablist:
            if not tables[tabname].Excl:
                EmitRelations("", tables[tabname], True)
        
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
    logger.info('Finished')

if __name__ == '__main__':
    main(sys.argv[1:])
