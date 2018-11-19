%pyspark

from pyspark.sql.functions import desc

JOIN_COLUMN = 'rn'

#definicion de una funcion que aplica un join entre dos dataframes y devuelve
#otro data frame como resultado
def joinDataFrames(df, df2, joinCol) :
    df.registerTempTable('df1')
    df2.registerTempTable('df2')
    return sqlContext.sql('\
                SELECT \
                    df1.* \
                FROM df1 \
                INNER JOIN \
                df2 ON df1.rn=df2.rn'
                )
    
#origen de datos
data ="/data/slice_violations.csv"
#dataframe con los datos cargados
df = spark.read.format("csv").options(header='true', inferschema='true').load(data)
#df.printSchema()
#df.limit(1).show()


#registramos el data frame como una tabla temporal con el alias "df_Table"
df.registerTempTable('df_table')

#lanzamos una consulta SQL filtrando por personal_injury=true y agrupando por marca,modelo y color para hacer
# un COUNT()
dfRES = sqlContext.sql('\
                SELECT \
                    make, \
                    model, \
                    color, \
                    COUNT(color) AS num_accidents \
                FROM df_table \
                WHERE personal_injury=true \
                GROUP BY make, model, color \
                ORDER BY COUNT(color) DESC '
                )
                
#dfRES.show()

#añadimos al dataframe de los datos la columna "dr" aplicando un DENSE_RANK()
#por Count descendientemente
dfRES.registerTempTable('dfRES')
dfDataDR = sqlContext.sql('\
                SELECT \
                    *, \
                    DENSE_RANK() OVER  (ORDER BY num_accidents DESC) AS dr \
                FROM dfRES'
                )
#dfDataDR.show()

#añadimos al dataframe la columna "rn" aplicando un ROW_NUMBER() ordenando
#por Count de manera descendente y "marca -modelo-color" ascendente
dfDataRN = sqlContext.sql('\
                SELECT \
                    *, \
                    ROW_NUMBER() OVER  (ORDER BY num_accidents DESC) AS rn \
                FROM dfRES'
                )
#dfDataRN.show()

dfDataDR.registerTempTable('dfDataDR')

#aplicamos otro ROW_NUMBER, pero esta vez sobre el dataframe donde previamente hemos aplicado el DENSE_RANK()
# para seleccionar las primeras filas de cada conjunto de datos en el Count
dfData_RN_over_DR = sqlContext.sql('\
                SELECT \
                    *, \
                    ROW_NUMBER() OVER  (PARTITION BY dr ORDER BY dr DESC) AS rn \
                FROM dfDataDR'
                )
#dfData_RN_over_DR.show()


dfData_RN_over_DR.registerTempTable('dfData_RN_over_DR')

#nos quedamos solo con las filas donde rn sea igual a 1 para despreciar el resto
dfData_RN_over_DR_filtered = sqlContext.sql('\
                SELECT \
                    * \
                FROM dfData_RN_over_DR \
                WHERE rn = 1'
                ).drop('rn').drop('dr')
dfData_RN_over_DR_filtered.show()

#primeras ocurrencias de cada conjunto de número de accidentes:
#ejemplo de lo que debe mostrar:
#+-----+--------------------+---+---+
#|Count|Mark - Model - Color| dr| rn|
#+-----+--------------------+---+---+
#|    6|  [SUNNY,NINGBO,RED]|  1|  1|<---
#|    5|[ACURA,INTEGRA,BL...|  2|  1|<---
#|    5|[CHRYSLER,SEBRING...|  2|  2|
#|    5|[FORD,EXPLORER,GR...|  2|  3|
#|    5|  [HONDA,CIVIC,GRAY]|  2|  4|
#|    5|  [MITS,LANCER,GRAY]|  2|  5|
#|    4|[CHRYSLER,PACIFIC...|  3|  1|<---
#|    4|  [DODGE,DAKOTA,RED]|  3|  2|
#|    4|  [HONDA,PILOT,GRAY]|  3|  3|
#|    4|   [KIA,TRUCK,BLACK]|  3|  4|
#|    3|   [ACUR,RDX,SILVER]|  4|  1|<---
#|    3| [HONDA,ACCORD,BLUE]|  4|  2|
#|    3|[INFINITI,SEDAN,G...|  4|  3|
#|    3| [TOYO,CAMRY,SILVER]|  4|  4|
#|    3|[TOYOTA,COROLLA,S...|  4|  5|
#|    3|[TOYOTA,COROLLA,TAN]|  4|  6|
#|    2|     [CHEV,SU,BLACK]|  5|  1|<---
#|    2|[FORD,EXPEDITION,...|  5|  2|
#|    2|     [HOND,4S,BLACK]|  5|  3|
#|    2| [HONDA,CIVIC,BLACK]|  5|  4|
#+-----+--------------------+---+---+


#para establecer un ranking en base al numero de ocurrencias y poder mostrar despues
#del join los 3 primeros coches con mas accidentes
schema = StructType([StructField("rn", IntegerType(), True)])

dfRanking = spark.createDataFrame(sc.parallelize([1,2,3]).map(lambda x: (x,)),schema)
#dfRanking.show()


#hacemos el join de los dos dataframes llamando a la funcion joinDataFrames
#y cruzando por la columna "rn" para que nos saque los 3 coches con mas
#accidentes
DF_join = joinDataFrames(dfDataRN,dfRanking,JOIN_COLUMN)


#renombramos la columna
DF_joinRES = DF_join.orderBy(JOIN_COLUMN).withColumnRenamed(JOIN_COLUMN, "Ranking")
print(" *** RANKING ***")
DF_joinRES.show()

#ejemplo de lo que debe mostrar:
#+-----+--------------------+---+
#|Count|Mark - Model - Color| rn|
#+-----+--------------------+---+
#|    6|  [SUNNY,NINGBO,RED]|  1|<---
#|    5|[ACURA,INTEGRA,BL...|  2|<---
#|    5|[CHRYSLER,SEBRING...|  3|<---
#|    5|[FORD,EXPLORER,GR...|  4|
#|    5|  [HONDA,CIVIC,GRAY]|  5|
#|    5|  [MITS,LANCER,GRAY]|  6|



