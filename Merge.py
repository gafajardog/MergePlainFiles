__author__ = 'Director'

import os
import sys
import traceback
import sqlite3
import csv
from glob import glob; from os.path import expanduser

conn = sqlite3.connect('Mergefiles.db')
archivolog = '_LogConsolidacion.csv'

def Reporte(raiz):
    with open(raiz+'\\'+archivolog, 'w', newline='') as csvfile:
        cursor = conn.cursor()
        sql = """
        select PrefCarpeta,prefijo, sum(cantidad) Registros
        from log group by PrefCarpeta,prefijo
        """

        csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, dialect="excel")
        csv_writer.writerow(['Prefijo Carpeta',' Prefijo Archivo',' Registros'])
        for row in cursor.execute(sql):
            csv_writer.writerow(row)


        sql="""
        select archivo, cantidad from log
        where PrefCarpeta='Otras Carpetas'
        """
        csv_writer.writerow(['','',''])
        csv_writer.writerow(['Lista Archivos sin procesar','',''])
        csv_writer.writerow(['Nombre Archivo','Registros'])
        for row in cursor.execute(sql):
            csv_writer.writerow(row)


def LineasArchivo(archivo):
    with open(archivo) as f:
        lineas = sum(1 for _ in f)
    return lineas

def borrarLog():
    c = conn.cursor()
    c.execute("delete from log")
    conn.commit()

def InsertatILog(carpeta, prefijo, archivo, extension, cantidad):
    c = conn.cursor()
    sql="""INSERT INTO log values('{PrefCarpeta}','{prefijo}','{archivo}','{extension}','{cantidad}')
        """.format(PrefCarpeta=carpeta, prefijo=prefijo, archivo=archivo, extension=extension, cantidad=cantidad)
    c.execute(sql)
    conn.commit()


def creatTabla():
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE if not exists log
                 (PrefCarpeta text, Prefijo text, archivo text, extension text, cantidad text)''')
    conn.commit()


def ProcesarCarpeta(raiz,lsext,lspref,lscarp):
    try:
        creatTabla()
        borrarLog()
        lsArchivos = []

        lsvetadas = []
        lsvetadas.append(raiz)
        for prefijocarpeta in lscarp:
            lsvetadas.append(raiz.upper() + '\\'+prefijocarpeta.upper())

        for root, dirs, files in os.walk(raiz):
            tmppref = ''
            tmpext=''
            tmpprefcarp=''

            if root.upper()+'\n' not in lsvetadas:
                #Archivo
                for name in files:
                    dname = name.upper()
                    filename, file_extension = os.path.splitext(dname)

                    estapref=False
                    for prefijo in lspref:
                        prefijoo = prefijo.upper().strip()
                        if prefijoo==filename[0:len(prefijoo)]:
                            estapref = True
                            tmppref = prefijoo

                    estaext = False
                    for extension in lsext:
                        exto = extension.upper().strip()
                        if exto==file_extension[1:4]:
                            estaext = True
                            tmpext=exto

                    base = os.path.basename(root)
                    estaprefcarpeta = False
                    for prefijocarpeta in lscarp:
                        prefcarpeta = prefijocarpeta.upper().rstrip()
                        if prefcarpeta == base[0:len(prefcarpeta)]:
                            estaprefcarpeta=True
                            tmpprefcarp = prefcarpeta

                    if estaext == True  and estapref == True:
                        patharch = root+'\\'+dname
                        #Todos los archivos que cumplen con Prefijo y Extension
                        lsArchivos.append(patharch.upper())
                        regs = LineasArchivo(patharch.upper())

                        if estaprefcarpeta==False:
                            tmpprefcarp='Otras Carpetas'
                        else:
                            #Generar Archivos  Unificados
                            filenameorigen = root+'\\'+name
                            filenamedest = raiz+'\\'+ tmpprefcarp + '\\' + tmppref+'.TXT'
                            if filenameorigen != filenamedest:
                                #print(filenameorigen)
                                with open(filenameorigen, 'r') as fo:
                                    with open(filenamedest, 'a') as fd:
                                        for line in fo:
                                            linestr=line.rstrip('\n')
                                            fd.write(linestr+'\n')

                        InsertatILog(tmpprefcarp, tmppref, root+'\\'+name, tmpext, str(regs))


        return lsArchivos
    except:
        print("ERROR en " + sys._getframe().f_code.co_name + ': '+ traceback.format_exc())

def EvaluarLista(raiz, ls, lscarp, lspref):
    lsa = []
    lsn = []
    for name in ls:
        regs = LineasArchivo(name)
        path,file = os.path.split(name)
        prefcarp=''
        prefarch=''
        filename, file_extension = os.path.splitext(name)
        base=os.path.basename(path)
        estaprefcarpeta=False
        for prefijocarpeta in lscarp:
            prefcarpeta = prefijocarpeta.upper().rstrip()
            if prefcarpeta == base[0:len(prefcarpeta)]:
                estaprefcarpeta=True
                prefcarp = prefcarpeta

        if estaprefcarpeta:
            #lsa.append(name)

            for prefijo in lspref:

                prefijoo = prefijo.upper().strip()
                filenameorigen = name
                filenamedest = raiz+'\\'+prefcarpeta + '\\' + prefijoo+'.TXT'
                with open(filenameorigen, 'r') as fo:
                    with open(filenamedest, 'a') as fd:
                        for line in fo:
                            linestr=line.rstrip('\n')
                            fd.write(linestr+'\n')

        else:
            lsn.append(name)
    return lsa, lsn


def Principal():

    ls = []
    lsa = []
    pathini = "C:\\Vacia"
    os.chdir(pathini)
    #print(os.getcwd())

    aext = 'Extensiones.txt'
    apref = 'PrefArchivos.txt'
    acarp = 'PrefCarpetas.txt'

    if not os.path.isfile(pathini+'\\'+aext):
        print('Archivo: ' + aext + ' Faltante')
        sys.exit()

    if not os.path.isfile(pathini+'\\'+apref):
        print('Archivo: ' + apref + ' Faltante')
        sys.exit()

    if not os.path.isfile(pathini+'\\'+acarp):
        print('Archivo: ' + acarp + ' Faltante')
        sys.exit()

    with open(aext) as fext:
        lsext = fext.readlines()

    with open(apref) as fpref:
        lspref = fpref.readlines()

    with open(acarp) as fcarp:
        lscarp = fcarp.readlines()

    for prefijocarpeta in lscarp:
        if not os.path.isdir(pathini+'\\'+prefijocarpeta.strip()):
            os.makedirs(prefijocarpeta.strip())

        for prefijo in lspref:
            if os.path.isfile(pathini+'\\'+prefijocarpeta.rstrip('\n')+'\\'+prefijo.rstrip('\n')+'.txt'):
                os.remove(pathini+'\\'+prefijocarpeta.rstrip('\n')+'\\'+prefijo.rstrip('\n')+'.txt')

    if os.path.isdir(pathini+'\\'+archivolog):
        os.remove(pathini+'\\'+archivolog)

    ls = ProcesarCarpeta(pathini, lsext, lspref, lscarp)

    Reporte(pathini)


if __name__ == "__main__":
    #creatTabla()
    Principal()