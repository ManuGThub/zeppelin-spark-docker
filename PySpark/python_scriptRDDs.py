%pyspark #utilizamos el interprete de pyspark
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

JOIN_COLUMN = 'rn'

#definicion de una funcion que aplica un join entre dos dataframes y devuelve
#otro data frame como resultado
def joinDataFrames(df, df2, joinCol) :
    return df.join(df2, joinCol) 

    
    
    
            
#funcion para filtrar por los accidentes donde hubo heridos utilizando un iterador
def filterInjuried(RDDrows) :
    lista = []
    for i in RDDrows:
        if i[10] == True:
            lista.append(i)
    return lista

#funcion para el filtrado y formateo del dataframe resultado utilizando map partitions
def filterMapPartitions(df) : 
     dfRES = df.rdd.mapPartitions(filterInjuried). \
     map(lambda row : (row[21], row[22], row[23])). \
     toDF(["Mark", "Model", "Color"])
     return dfRES


#funcion para filtrar por accidentes donde hubo heridos y darle formato
def filterDF(df) : 
    dfRES = df.rdd.filter(lambda x: x['personal_injury'] == True) \
    .map(lambda row: (row['make'],row['model'],row['color'],row['personal_injury'])) \
    .toDF(['Make', 'Model', 'Color'])
    return dfRES
  
#origen de datos
data ="/data/slice_violations.csv"

#dataframe con los datos cargados
df = spark.read.format("csv").options(header='true', inferschema='true').load(data)
#df.printSchema()
#df.limit(1).show()

#RDD donde filtramos los campos donde haya habido gente herida y agrupamos por marca,modelo y color de coche.
#tambien hacemos un mapeo KEY - VALUE en el que asociamos marca,modelo y color como KEY y (1) como VALUE.
#despues hacemos un reduce by key, para que las que tengan las mismas key, ejecuten la funcion lambda declarada (a+b).
#por lo que el resultado sera el sumatorio de todos los VALUES (1).
# despues intercambiamos las columnas con un map, para que aparezca primero el conteo y despues la marca,modelo y color,
#y ordenamos mediante sortBy, primero x la primera columna en orden descendente (-1 * [0]) y despues por la segunda columna (x[1])

#dfFiltered=filterMapPartitions(df)  #filtrado utilizando un filter sobre el data frame 
dfFiltered=filterDF(df)  #filtrado utilizando map partitions sobre el data frame 
 
#mapeamos marca, modelo y color (KEY) a 1 (VALUE) 
groupByRDD = dfFiltered.rdd \
.map(lambda x: ((x[0],x[1],x[2]), 1)) \
.reduceByKey(lambda a, b: a + b) \
.map(lambda r:(r[1],r[0])) \
.sortBy(lambda x: (-1 * x[0], x[1]))

#generamos un dataframe final desde el RDD anterior con la cabecera
# con ".persist()" obligamos a que se caché la información del dataframe en memoria
# lo cual nos viene bien ya que vamos a hacer varias operaciones con el
dfRES = groupByRDD.toDF(['Count','Mark - Model - Color']).persist()

#mostramos el dataframe
#dfRES.show()

#añadimos al dataframe de los datos la columna "dr" aplicando un DENSE_RANK()
#por Count descendientemente
windowSpecDR = Window.orderBy(dfRES.Count.desc())
dfDataDR = dfRES.withColumn("dr", dense_rank().over(windowSpecDR))
#dfDataDR.show()

#añadimos al dataframe la columna "rn" aplicando un ROW_NUMBER() ordenando
#por Count de manera descendente y "marca -modelo-color" ascendente
windowSpecRN = Window.orderBy(dfRES.Count.desc(),dfRES['Mark - Model - Color'])
dfDataRN = dfRES.withColumn('rn', row_number().over(windowSpecRN))
#dfDataRN.show()

#aplicamos otro ROW_NUMBER, pero esta vez sobre el dataframe donde previamente hemos aplicado el DENSE_RANK()
# para seleccionar las primeras filas de cada conjunto de datos en el Count
window_RN_over_DR = Window.partitionBy(dfDataDR.dr).orderBy(dfDataDR.dr.desc())
dfData_RN_over_DR = dfDataDR.withColumn('rn', row_number().over(window_RN_over_DR))
#dfData_RN_over_DR.show()

#nos quedamos solo con las filas donde rn sea igual a 1 para despreciar el resto
dfData_RN_over_DR_filtered = dfData_RN_over_DR.where(col('rn') == 1).drop('rn').drop('dr')
print(" *** RANKING BY GROUP OF COUNT ***")
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

dfNew2 = spark.createDataFrame(sc.parallelize([1,2,3]).map(lambda x: (x,)),schema)
#dfNew2.show()

#hacemos el join de los dos dataframes llamando a la funcion joinDataFrames
#y cruzando por la columna "rn" para que nos saque los 3 coches con mas
#accidentes
DF_join = joinDataFrames(dfDataRN,dfNew2,JOIN_COLUMN)


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

