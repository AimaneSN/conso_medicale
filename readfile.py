import re as re

def get_firstlines(filepath, n, h : bool):
    # input : 
      ## filepath : chemin du fichier [type:string] 
      ## n : nombre de lignes à afficher [type:integer]
      ## h : inclure l'en-tête ou non (True : Inclure l'en-tête, False : Ne pas inclure l'en-tête) [type:bool]
    #output : liste contenant les n premières lignes (type : list)
    file = open(filepath, 'r', encoding="ISO-8859-1")
    firstlines=[]
    file.seek(0)
    for i in range (1,n+2):
        firstlines.append(file.readline().strip('\n'))
    file.close()
    if h==False:
        return firstlines[1:]
    return firstlines[:n+1]

def get_colnames(filepath : str):
    # input : 
      ## filepath :chemin du fichier [type:string]
    # output : liste contenant les noms des variables du fichier et le séparateur (type: list)
    firstline=get_firstlines(filepath, 1,1)
    sep_file="";
    separateurs = [',', ';', '\|']
    for sep in separateurs:
        expr=re.compile("[A-Za-z]"+sep+"[A-Za-z]")
        if expr.search(firstline[0]):
            sep_file=sep
    if sep_file=='\|':
        columns=firstline[0].split('|')
        return columns
    if sep_file=="":
        print("separateur non trouvé")
        return -1
    columns=firstline[0].split(sep_file)
    return columns+[sep_file]

def get_col_firstlines(filepath : str, column_name : str, n : int):
    # input : 
      ## filepath : chemin du fichier [type:tring] 
      ## column_name : nom de la colonne [type:string] 
      ## n : nombre d'observations à afficher [type:integer]
    # output : liste contenant les premières observations de la colonne choisie [lines: list]
    cols=get_colnames(filepath)
    colindex=cols.index(column_name)
    sep= cols[len(cols)-1]
    lines=[]
    firstlines=get_firstlines(filepath, n, 0)
    for line in firstlines:
        row=line.split(sep)
        lines.append(row[colindex])
    return lines

def get_data(filepath : str, sep_file : str, rownum : int):
    # input : 
      ## filepath : Chemin du fichier[type:string]
      ## sep_file : Séparateur du fichier[type:string]. Exemple: sep_file=";"
      ## rownum ; Nombre d'enregistrements(lignes) à récupérer [type:entier]
    # output : 
      ## l : liste de listes. Exemple : l[0] est la liste des élèments de la première ligne (l'en-tête).
    
    rows = get_firstlines(filepath,rownum,1)
    l = []
    for row in rows:
        l.append(row.split(sep_file))
    return l


def get_file(filepath : str, filesep : str, sparkses):
    # input :
      ## filepath : Chemin du fichier[type:string]
      ## filesep : Séparateur entre les colonnes [type:string]
    # output : dataframe Spark ou -1 en cas d'échec
    try:
        df=sparkses.read.options(inferSchema=True).option("encoding", "ISO-8859-1").csv(filepath, header=True, sep=filesep)
        print("schéma trouvé pour le fichier", filepath)
        return df
    except:
        print("Erreur, le programme ne peut pas trouver le schéma pour le fichier", filepath)
        return -1


