# -*- coding: utf-8 -*-
""" Programacion POO para SqlServer
@author: Diego Diaz Marzano
"""
# PROGRAMACION POO PARA SQLSERVER

# Librerias
import pandas as pd
import pyodbc
import numpy as np

# Clase
class conexion():
    def __init__(self,dsn,db):
        self.descripcion='Conexion a Sql Server'
        self.db=db
        self.dsn=dsn
    def consulta (self,sentencia):
        con=pyodbc.connect(dsn=self.dsn)
        cur=con.cursor()
        cur.execute('use '+self.db)
        arr=cur.execute(sentencia).fetchall()
        con.commit()
        return arr
    def carga_tabla1(self,df,tabla):
        con=pyodbc.connect(dsn=self.dsn)
        cur=con.cursor()
        cur.execute('use '+self.db)
        for row in df.to_numpy():
            sentencia="insert into "+tabla+f" values('{row[0]}')"
            cur.execute(sentencia)
            con.commit()
    def carga_tabla2(self,df,tabla):
        con=pyodbc.connect(dsn=self.dsn)
        cur=con.cursor()
        cur.execute('use '+self.db)
        for row in df.to_numpy():
            sentencia="insert into "+tabla+f" values('{row[0]}','{row[1]}')"
            cur.execute(sentencia)
            con.commit()
    # Se agrega para tablas de n columnas
    def carga_tabla12(self,df,tabla):
        con=pyodbc.connect(dsn=self.dsn)
        cur=con.cursor()
        cur.execute('use '+self.db)
        for row in df.to_numpy():
            sentencia="insert into "+tabla+f" values('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}','{row[6]}',{row[7]},'{row[8]}','{row[9]}','{row[10]}','{row[11]}')"
            cur.execute(sentencia)
            con.commit()
            
# Ejemplo de uso
#con1=conexion('nombre_dsn','nombre_base_de_datos')

#con1.consulta('select * from nombre_tabla')
#con1.carga_tabla2('nombre_df','nombre_tala')



# 