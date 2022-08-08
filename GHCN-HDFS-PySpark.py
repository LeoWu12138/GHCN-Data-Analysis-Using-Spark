#Q1
#read the file from /data/ghcnd/
$ hdfs dfs -ls -R -h /data/ghcnd/

#check how many year of the daily data
$ hdfs dfs -count -v /data/ghcnd/daily

# check the total file size
$ hdfs dfs -du -h /data/ghcnd/

#Read daily files with gzip
$ hdfs dfs -du -h -v /data/ghcnd/daily

#Read Metdata
$hdfs dfs -du -s -h -v -x /data/ghcnd/ghcnd-countries.txt
$hdfs dfs -du -s -h -v -x /data/ghcnd/ghcnd-inventory.txt
$hdfs dfs -du -s -h -v -x /data/ghcnd/ghcnd-states.txt
$hdfs dfs -du -s -h -v -x /data/ghcnd/ghcnd-stations.txt

#Processing
#Q2- (a)&(b)
import sys
import os
from pyspark import SparkContext
from pyspark.sql import Window
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

"""Required to allow the file to be submitted and run using spark-submit 
instead of using pyspark interactively"""

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M 

#DATA 
dailySchema = StructType([ \
    StructField("ID",StringType(),True), \
    StructField("DATE",DateType(),True), \
    StructField("ELEMENT",StringType(),True), \
    StructField("VALUE", FloatType(), True), \
    StructField("MFLAG", StringType(), True), \
    StructField("QFLAG", StringType(), True), \
    StructField("SFLAG", StringType(), True), \
    StructField("OBV_TIME", TimestampType(), True), \
  ])
   
daily = (spark.read.option("delimiter", ",")
         .option("header","false")
         .option("dateFormat", "yyyyMMdd")
         .option("timestampFormat", "hhmm")
         .schema(dailySchema)
         .csv("hdfs:///data/ghcnd/daily/2022.csv.gz")
         .limit(1000)
         )
daily.count() #1000
daily.cache()
daily.show()
+-----------+----------+-------+-----+-----+-----+-----+--------+
|         ID|      DATE|ELEMENT|VALUE|MFLAG|QFLAG|SFLAG|OBV_TIME|
+-----------+----------+-------+-----+-----+-----+-----+--------+
|AE000041196|2022-01-01|   TAVG|204.0|    H| null|    S|    null|
|AEM00041194|2022-01-01|   TAVG|211.0|    H| null|    S|    null|
|AEM00041218|2022-01-01|   TAVG|207.0|    H| null|    S|    null|
|AEM00041217|2022-01-01|   TAVG|209.0|    H| null|    S|    null|
|AG000060390|2022-01-01|   TAVG|121.0|    H| null|    S|    null|
|AG000060590|2022-01-01|   TAVG|151.0|    H| null|    S|    null|
|AG000060611|2022-01-01|   TAVG|111.0|    H| null|    S|    null|
|AGE00147708|2022-01-01|   TMIN| 73.0| null| null|    S|    null|
|AGE00147708|2022-01-01|   PRCP|  0.0| null| null|    S|    null|
|AGE00147708|2022-01-01|   TAVG|133.0|    H| null|    S|    null|
|AGE00147716|2022-01-01|   TMIN|107.0| null| null|    S|    null|
|AGE00147716|2022-01-01|   PRCP|  0.0| null| null|    S|    null|
|AGE00147716|2022-01-01|   TAVG|133.0|    H| null|    S|    null|
|AGE00147718|2022-01-01|   TMIN| 90.0| null| null|    S|    null|
|AGE00147718|2022-01-01|   TAVG|152.0|    H| null|    S|    null|
|AGE00147719|2022-01-01|   TMAX|201.0| null| null|    S|    null|
|AGE00147719|2022-01-01|   PRCP|  0.0| null| null|    S|    null|
|AGE00147719|2022-01-01|   TAVG|119.0|    H| null|    S|    null|
|AGM00060351|2022-01-01|   PRCP|  0.0| null| null|    S|    null|
|AGM00060351|2022-01-01|   TAVG|126.0|    H| null|    S|    null|
+-----------+----------+-------+-----+-----+-----+-----+--------+


#(c)
stationSchema = StructType([StructField('ID', StringType(), True), \
                     StructField('LATITUDE', FloatType(), True), \
                     StructField('LONGITUDE', FloatType(), True), \
                     StructField('ELEVATION', FloatType(), True), \
                     StructField('STATE', StringType(), True), \
                     StructField('NAME', StringType(), True), \
                     StructField('GFLAG', StringType(), True), \
                     StructField('HCFLAG', StringType(), True), \
                     StructField('WMOID', StringType(), True)])

                                              
station_text = (
    spark.read.text("hdfs:///data/ghcnd/ghcnd-stations.txt")
)

station = station_text.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(stationSchema['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(stationSchema['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(stationSchema['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 6)).alias('ELEVATION').cast(stationSchema['ELEVATION'].dataType),
    F.trim(F.substring(F.col('value'), 39, 2)).alias('STATE').cast(stationSchema['STATE'].dataType),
    F.trim(F.substring(F.col('value'), 42, 30)).alias('NAME').cast(stationSchema['NAME'].dataType),
    F.trim(F.substring(F.col('value'), 73, 3)).alias('GFLAG').cast(stationSchema['GFLAG'].dataType),
    F.trim(F.substring(F.col('value'), 77, 3)).alias('HCFLAG').cast(stationSchema['HCFLAG'].dataType),
    F.trim(F.substring(F.col('value'), 81, 5)).alias('WMOID').cast(stationSchema['WMOID'].dataType)
    )

station.count() # 118493
station.cache()
station.show(5)
+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GFLAG|HCFLAG|WMOID|
+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+
|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|     |      |     |
|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|     |      |     |
|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|  GSN|      |41196|
|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|     |      |41194|
|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|     |      |41217|
+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+

#How many stations do not have a WMO ID? 110407
station.filter(station.WMOID == '').count()
 
countrySchema = StructType([StructField('CODE', StringType(), True), \
                     StructField('COUNTRY_NAME', StringType(), True)])

country_text = (
    spark.read.text("hdfs:///data/ghcnd/ghcnd-countries.txt")
)

country = country_text.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(countrySchema['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('COUNTRY_NAME').cast(countrySchema['COUNTRY_NAME'].dataType)
    )

country.count()# 219
country.cache()
country.show(5)
+----+--------------------+
|CODE|        COUNTRY_NAME|
+----+--------------------+
|  AC| Antigua and Barbuda|
|  AE|United Arab Emirates|
|  AF|         Afghanistan|
|  AG|             Algeria|
|  AJ|          Azerbaijan|
+----+--------------------+


stateSchema = StructType([StructField('CODE', StringType(), True), \
                     StructField('STATE_NAME', StringType(), True)])

state_text = (
    spark.read.text("hdfs:///data/ghcnd/ghcnd-states.txt")
)


state = state_text.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(stateSchema['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('STATE_NAME').cast(stateSchema['STATE_NAME'].dataType)
    )
    
state.count() #74
state.cache()
state.show(5)
+----+--------------+
|CODE|    STATE_NAME|
+----+--------------+
|  AB|       ALBERTA|
|  AK|        ALASKA|
|  AL|       ALABAMA|
|  AR|      ARKANSAS|
|  AS|AMERICAN SAMOA|
+----+--------------+

    
invtSchema = StructType([StructField('ID', StringType(), True), \
                     StructField('LAT', FloatType(), True), \
                     StructField('LONG', FloatType(), True), \
                     StructField('ELEMENT', StringType(), True), \
                     StructField('FIRSTYEAR', IntegerType(), True), \
                     StructField('LASTYEAR', IntegerType(), True)])
    
invt_text = (
    spark.read.text("hdfs:///data/ghcnd/ghcnd-inventory.txt")
)

invt = invt_text.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(invtSchema['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LAT').cast(invtSchema['LAT'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONG').cast(invtSchema['LONG'].dataType),
    F.trim(F.substring(F.col('value'), 32, 4)).alias('ELEMENT').cast(invtSchema['ELEMENT'].dataType),
    F.trim(F.substring(F.col('value'), 37, 4)).alias('FIRSTYEAR').cast(invtSchema['FIRSTYEAR'].dataType),
    F.trim(F.substring(F.col('value'), 42, 4)).alias('LASTYEAR').cast(invtSchema['LASTYEAR'].dataType)
    )

invt.count()#704963
invt.cache()
invt.show(5)
+-----------+-------+--------+-------+---------+--------+
|         ID|    LAT|    LONG|ELEMENT|FIRSTYEAR|LASTYEAR|
+-----------+-------+--------+-------+---------+--------+
|ACW00011604|17.1167|-61.7833|   TMAX|     1949|    1949|
|ACW00011604|17.1167|-61.7833|   TMIN|     1949|    1949|
|ACW00011604|17.1167|-61.7833|   PRCP|     1949|    1949|
|ACW00011604|17.1167|-61.7833|   SNOW|     1949|    1949|
|ACW00011604|17.1167|-61.7833|   SNWD|     1949|    1949|
+-----------+-------+--------+-------+---------+--------+

#Q3-(a)
station_new = station.withColumn('COUNTRY_CODE', station.ID.substr(1, 2))
# station_new.cache()
# station_new.show()
+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+------------+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GFLAG|HCFLAG|WMOID|COUNTRY_CODE|
+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+------------+
|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|     |      |     |          AC|
|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|     |      |     |          AC|
|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|  GSN|      |41196|          AE|
|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|     |      |41194|          AE|
|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|     |      |41217|          AE|
|AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|     |      |41218|          AE|
|AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|  GSN|      |40930|          AF|
|AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|     |      |40938|          AF|
|AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|     |      |40948|          AF|
|AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|     |      |40990|          AF|
|AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|  GSN|      |60390|          AG|
|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|  GSN|      |60590|          AG|
|AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|  GSN|      |60611|          AG|
|AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|  GSN|      |60680|          AG|
|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|     |      |     |          AG|
|AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|     |      |     |          AG|
|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|     |      |     |          AG|
|AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|     |      |     |          AG|
|AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|     |      |     |          AG|
|AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|     |      |60395|          AG|
+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+------------+
#(b)
station_new = station_new.join(country.withColumnRenamed('CODE','COUNTRY_CODE').withColumnRenamed('NAME','COUNTRY_NAME'), on='COUNTRY_CODE', how="left")
# station_new.cache()
# station_new.show()
+------------+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+--------------------+
|COUNTRY_CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GFLAG|HCFLAG|WMOID|        COUNTRY_NAME|
+------------+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+--------------------+
|          AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|     |      |     | Antigua and Barbuda|
|          AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|     |      |     | Antigua and Barbuda|
|          AE|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|  GSN|      |41196|United Arab Emirates|
|          AE|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|     |      |41194|United Arab Emirates|
|          AE|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|     |      |41217|United Arab Emirates|
|          AE|AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|     |      |41218|United Arab Emirates|
|          AF|AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|  GSN|      |40930|         Afghanistan|
|          AF|AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|     |      |40938|         Afghanistan|
|          AF|AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|     |      |40948|         Afghanistan|
|          AF|AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|     |      |40990|         Afghanistan|
|          AG|AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|  GSN|      |60390|             Algeria|
|          AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|  GSN|      |60590|             Algeria|
|          AG|AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|  GSN|      |60611|             Algeria|
|          AG|AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|  GSN|      |60680|             Algeria|
|          AG|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|     |      |     |             Algeria|
|          AG|AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|     |      |     |             Algeria|
|          AG|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|     |      |     |             Algeria|
|          AG|AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|     |      |     |             Algeria|
|          AG|AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|     |      |     |             Algeria|
|          AG|AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|     |      |60395|             Algeria|
+------------+-----------+--------+---------+---------+-----+--------------------+-----+------+-----+--------------------+
#(c)
station_new = station_new.join(state.withColumnRenamed('CODE','STATE').withColumnRenamed('NAME','STATE_NAME'), on = 'STATE', how= "left")
# station_new.cache()
# station_new.show()
+-----+------------+-----------+--------+---------+---------+--------------------+-----+------+-----+--------------------+----------+
|STATE|COUNTRY_CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|                NAME|GFLAG|HCFLAG|WMOID|        COUNTRY_NAME|STATE_NAME|
+-----+------------+-----------+--------+---------+---------+--------------------+-----+------+-----+--------------------+----------+
|     |          AC|ACW00011604| 17.1167| -61.7833|     10.1|ST JOHNS COOLIDGE...|     |      |     | Antigua and Barbuda|      null|
|     |          AC|ACW00011647| 17.1333| -61.7833|     19.2|            ST JOHNS|     |      |     | Antigua and Barbuda|      null|
|     |          AE|AE000041196|  25.333|   55.517|     34.0| SHARJAH INTER. AIRP|  GSN|      |41196|United Arab Emirates|      null|
|     |          AE|AEM00041194|  25.255|   55.364|     10.4|          DUBAI INTL|     |      |41194|United Arab Emirates|      null|
|     |          AE|AEM00041217|  24.433|   54.651|     26.8|      ABU DHABI INTL|     |      |41217|United Arab Emirates|      null|
|     |          AE|AEM00041218|  24.262|   55.609|    264.9|         AL AIN INTL|     |      |41218|United Arab Emirates|      null|
|     |          AF|AF000040930|  35.317|   69.017|   3366.0|        NORTH-SALANG|  GSN|      |40930|         Afghanistan|      null|
|     |          AF|AFM00040938|   34.21|   62.228|    977.2|               HERAT|     |      |40938|         Afghanistan|      null|
|     |          AF|AFM00040948|  34.566|   69.212|   1791.3|          KABUL INTL|     |      |40948|         Afghanistan|      null|
|     |          AF|AFM00040990|    31.5|    65.85|   1010.0|    KANDAHAR AIRPORT|     |      |40990|         Afghanistan|      null|
|     |          AG|AG000060390| 36.7167|     3.25|     24.0|  ALGER-DAR EL BEIDA|  GSN|      |60390|             Algeria|      null|
|     |          AG|AG000060590| 30.5667|   2.8667|    397.0|            EL-GOLEA|  GSN|      |60590|             Algeria|      null|
|     |          AG|AG000060611|   28.05|   9.6331|    561.0|           IN-AMENAS|  GSN|      |60611|             Algeria|      null|
|     |          AG|AG000060680|    22.8|   5.4331|   1362.0|         TAMANRASSET|  GSN|      |60680|             Algeria|      null|
|     |          AG|AGE00135039| 35.7297|     0.65|     50.0|ORAN-HOPITAL MILI...|     |      |     |             Algeria|      null|
|     |          AG|AGE00147704|   36.97|     7.79|    161.0| ANNABA-CAP DE GARDE|     |      |     |             Algeria|      null|
|     |          AG|AGE00147705|   36.78|     3.07|     59.0|ALGIERS-VILLE/UNI...|     |      |     |             Algeria|      null|
|     |          AG|AGE00147706|    36.8|     3.03|    344.0|   ALGIERS-BOUZAREAH|     |      |     |             Algeria|      null|
|     |          AG|AGE00147707|    36.8|     3.04|     38.0|  ALGIERS-CAP CAXINE|     |      |     |             Algeria|      null|
|     |          AG|AGE00147708|   36.72|     4.05|    222.0|          TIZI OUZOU|     |      |60395|             Algeria|      null|
+-----+------------+-----------+--------+---------+---------+--------------------+-----+------+-----+--------------------+----------+

#(d) Based on inventory
#first and last year
#invt.orderBy(invt.LASTYEAR.desc()).show(1,False)
#invt.orderBy(invt.FIRSTYEAR.asc()).show(1,False)
#Aggrisive the first and last year that each station was active
station_years = (invt.groupBy('ID')
                 .agg(F.min(invt.FIRSTYEAR).alias('Firstyear'),F.max(invt.LASTYEAR).alias('Lastyear'))
                 .sort(F.col('ID')))
#station_years.cache()
#station_years.show()
+-----------+---------+--------+
|         ID|Firstyear|Lastyear|
+-----------+---------+--------+
|ACW00011604|     1949|    1949|
|ACW00011647|     1957|    1970|
|AE000041196|     1944|    2021|
|AEM00041194|     1983|    2021|
|AEM00041217|     1983|    2021|
|AEM00041218|     1994|    2021|
|AF000040930|     1973|    1992|
|AFM00040938|     1973|    2021|
|AFM00040948|     1966|    2021|
|AFM00040990|     1973|    2020|
|AG000060390|     1940|    2021|
|AG000060590|     1892|    2021|
|AG000060611|     1958|    2021|
|AG000060680|     1940|    2004|
|AGE00135039|     1852|    1966|
|AGE00147704|     1909|    1937|
|AGE00147705|     1877|    1938|
|AGE00147706|     1893|    1920|
|AGE00147707|     1878|    1879|
|AGE00147708|     1879|    2021|
+-----------+---------+--------+

#The number of different elements has each station collected overall
station_element = invt.groupBy("ID").agg(F.countDistinct("ELEMENT").alias("ELEMENT_NUMBER")).sort(F.col("ID"))
# station_element.cache()
# station_element.show()
+-----------+--------------+
|         ID|ELEMENT_NUMBER|
+-----------+--------------+
|ACW00011604|            11|
|ACW00011647|             7|
|AE000041196|             4|
|AEM00041194|             4|
|AEM00041217|             4|
|AEM00041218|             4|
|AF000040930|             5|
|AFM00040938|             5|
|AFM00040948|             5|
|AFM00040990|             5|
|AG000060390|             5|
|AG000060590|             4|
|AG000060611|             5|
|AG000060680|             4|
|AGE00135039|             3|
|AGE00147704|             3|
|AGE00147705|             3|
|AGE00147706|             3|
|AGE00147707|             3|
|AGE00147708|             5|
+-----------+--------------+

#List all elements for each station 
station_element_list = invt.groupBy("ID").agg(F.collect_set("ELEMENT").cast('string').alias("ELEMENT_LIST")).sort(F.col("ID"))
# station_element_list.cache()
# station_element_list.show()
+-----------+--------------------+
|         ID|        ELEMENT_LIST|
+-----------+--------------------+
|ACW00011604|[TMAX, TMIN, WSFG...|
|ACW00011647|[TMAX, TMIN, PRCP...|
|AE000041196|[TMAX, TMIN, PRCP...|
|AEM00041194|[TMAX, TMIN, PRCP...|
|AEM00041217|[TMAX, TMIN, PRCP...|
|AEM00041218|[TMAX, TMIN, PRCP...|
|AF000040930|[TMAX, TMIN, PRCP...|
|AFM00040938|[TMAX, TMIN, PRCP...|
|AFM00040948|[TMAX, TMIN, PRCP...|
|AFM00040990|[TMAX, TMIN, PRCP...|
|AG000060390|[TMAX, TMIN, PRCP...|
|AG000060590|[TMAX, TMIN, PRCP...|
|AG000060611|[TMAX, TMIN, PRCP...|
|AG000060680|[TMAX, TMIN, PRCP...|
|AGE00135039|  [TMAX, TMIN, PRCP]|
|AGE00147704|  [TMAX, TMIN, PRCP]|
|AGE00147705|  [TMAX, TMIN, PRCP]|
|AGE00147706|  [TMAX, TMIN, PRCP]|
|AGE00147707|  [TMAX, TMIN, PRCP]|
|AGE00147708|[TMAX, TMIN, PRCP...|
+-----------+--------------------+
#Count separately the number of core elements and the number of ”other” elements that each station has collected overall.
coreElements = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]

#Core element number
core_element = invt.filter(invt.ELEMENT.isin(coreElements)).groupBy("ID").agg(F.countDistinct("ELEMENT").alias('num_cElement'))
# core_element.cache()
# core_element.show()
+-----------+------------+
|         ID|num_cElement|
+-----------+------------+
|ALE00100939|           2|
|AQC00914873|           5|
|AR000000002|           1|
|AR000087374|           4|
|ARM00087480|           4|
|ARM00087509|           4|
|ASN00001020|           3|
|ASN00002033|           1|
|ASN00006036|           1|
|ASN00006080|           3|
|ASN00007039|           1|
|ASN00007187|           1|
|ASN00008093|           3|
|ASN00009073|           1|
|ASN00009186|           3|
|ASN00009603|           3|
|ASN00009802|           1|
|ASN00009916|           1|
|ASN00010146|           1|
|ASN00010529|           1|
+-----------+------------+

#Other element number
other_element = invt.filter(~invt.ELEMENT.isin(coreElements)).groupBy("ID").agg(F.countDistinct("ELEMENT").alias('num_nElement'))
# other_element.cache()
# other_element.show()
+-----------+------------+
|         ID|num_nElement|
+-----------+------------+
|US1COAR0249|           4|
|US1COBN0028|           2|
|US1COBO0165|           4|
|US1COBO0298|           4|
|US1COBO0477|           4|
|US1COCF0008|           4|
|US1COCF0015|           4|
|US1COCF0038|           2|
|US1COCF0043|           4|
|US1CODG0035|           4|
|US1CODG0059|           2|
|US1CODG0226|           4|
|US1COEG0017|           4|
|US1COEG0044|           3|
|US1COEP0416|           4|
|US1COFM0076|           4|
|US1COGF0049|           2|
|US1COHF0004|           4|
|US1COJF0060|           4|
|US1COJF0124|           4|
+-----------+------------+
#How many stations collect all five core elements? 20289
invt.filter(F.col('ELEMENT').isin(coreElements)).groupBy("ID").agg(F.countDistinct("ELEMENT").alias("count")).filter("count = 5").count()
#dropDuplicates(['ELEMENT']) need to confirm whether or not drop duplicats

#Aggresive the core and other elements
#count_cond = lambda cond: F.sum(F.when(cond,1).otherwise(0))
#invt_element = invt.groupBy('ID').agg(count_cond(invt.ELEMENT.isin(coreElements)), count_cond(~invt.ELEMENT.isin(coreElements))).sort(F.col('ID'))

#How many only collected precipitation? 16136
station_element_list.filter(F.col('ELEMENT_LIST') == '[PRCP]').count()

#(e)
station_new = station_new.join(station_years,'ID','left')
station_new = station_new.join(station_element,'ID','left')
station_new = station_new.join(station_element_list,'ID','left')
element_join = core_element.join(other_element,'ID','left')
station_new = station_new.join(element_join,'ID','left')
station_new = station_new.withColumnRenamed("ID", "STATION_ID")
#station_new.cache()
#station_new.show()
+-----------+-----+------------+--------+---------+---------+--------------------+-----+------+-----+--------------------+--------------+---------+--------+--------------+--------------------+------------+------------+
| STATION_ID|STATE|COUNTRY_CODE|LATITUDE|LONGITUDE|ELEVATION|                NAME|GFLAG|HCFLAG|WMOID|        COUNTRY_NAME|    STATE_NAME|Firstyear|Lastyear|ELEMENT_NUMBER|        ELEMENT_LIST|num_cElement|num_nElement|
+-----------+-----+------------+--------+---------+---------+--------------------+-----+------+-----+--------------------+--------------+---------+--------+--------------+--------------------+------------+------------+
|AGE00147719|     |          AG| 33.7997|     2.89|    767.0|            LAGHOUAT|     |      |60545|             Algeria|          null|     1888|    2021|             4|[TMAX, TMIN, PRCP...|           3|           1|
|AGM00060445|     |          AG|  36.178|    5.324|   1050.0|     SETIF AIN ARNAT|     |      |60445|             Algeria|          null|     1957|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|AJ000037679|     |          AJ|    41.1|     49.2|    -26.0|             SIASAN'|     |      |37679|          Azerbaijan|          null|     1959|    1987|             1|              [PRCP]|           1|        null|
|AJ000037831|     |          AJ|    40.4|     47.0|    160.0|          MIR-BASHIR|     |      |37831|          Azerbaijan|          null|     1955|    1987|             1|              [PRCP]|           1|        null|
|AJ000037981|     |          AJ|    38.9|     48.2|    794.0|            JARDIMLY|     |      |37981|          Azerbaijan|          null|     1959|    1987|             1|              [PRCP]|           1|        null|
|AJ000037989|     |          AJ|    38.5|     48.9|    -22.0|              ASTARA|  GSN|      |37989|          Azerbaijan|          null|     1936|    2017|             5|[TMAX, TMIN, PRCP...|           4|           1|
|ALE00100939|     |          AL| 41.3331|  19.7831|     89.0|              TIRANA|     |      |     |             Albania|          null|     1940|    2000|             2|        [TMAX, PRCP]|           2|        null|
|AM000037719|     |          AM|    40.6|    45.35|   1834.0|           CHAMBARAK|     |      |37719|             Armenia|          null|     1912|    1992|             5|[TMAX, TMIN, PRCP...|           4|           1|
|AM000037897|     |          AM|  39.533|   46.017|   1581.0|              SISIAN|     |      |37897|             Armenia|          null|     1936|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|AQC00914873|   AS|          AQ|  -14.35|-170.7667|     14.9|    TAPUTIMU TUTUILA|     |      |     |American Samoa [U...|AMERICAN SAMOA|     1955|    1967|            12|[WT03, TMAX, TMIN...|           5|           7|
|AR000000002|     |          AR|  -29.82|   -57.42|     75.0|            BONPLAND|     |      |     |           Argentina|          null|     1981|    2000|             1|              [PRCP]|           1|        null|
|AR000087007|     |          AR|   -22.1|    -65.6|   3479.0| LA QUIACA OBSERVATO|  GSN|      |87007|           Argentina|          null|     1956|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|AR000087374|     |          AR| -31.783|  -60.483|     74.0|         PARANA AERO|  GSN|      |87374|           Argentina|          null|     1956|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|AR000875850|     |          AR| -34.583|  -58.483|     25.0| BUENOS AIRES OBSERV|     |      |87585|           Argentina|          null|     1908|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|ARM00087022|     |          AR|  -22.62|  -63.794|    449.0|GENERAL ENRIQUE M...|     |      |87022|           Argentina|          null|     1973|    2021|             4|[TMAX, TMIN, PRCP...|           3|           1|
|ARM00087480|     |          AR| -32.904|  -60.785|     25.9|             ROSARIO|     |      |87480|           Argentina|          null|     1965|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|ARM00087509|     |          AR| -34.588|  -68.403|    752.9|          SAN RAFAEL|     |      |87509|           Argentina|          null|     1973|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|ARM00087532|     |          AR| -35.696|  -63.758|    139.9|        GENERAL PICO|     |      |87532|           Argentina|          null|     1973|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|ARM00087904|     |          AR| -50.267|   -72.05|    204.0|    EL CALAFATE AERO|     |      |87904|           Argentina|          null|     2003|    2021|             5|[TMAX, TMIN, PRCP...|           4|           1|
|ASN00001003|     |          AS|-14.1331| 126.7158|      5.0|        PAGO MISSION|     |      |     |           Australia|          null|     1909|    1940|             1|              [PRCP]|           1|        null|
+-----------+-----+------------+--------+---------+---------+--------------------+-----+------+-----+--------------------+--------------+---------+--------+--------------+--------------------+------------+------------+

#Save the new station to csv.gz
station_new.write.option("compression","gzip").csv("hdfs:///user/pwu17/outputs/ghcnd/enriched-states.csv.gz", mode = 'overwrite',header = True)

#(f)
"""Joining 1000rows subset of daily and new station output"""
daily_station = daily.join(station_new, daily.ID == station_new.STATION_ID, "leftanti") 
#daily_station.cache()
#daily_station.show()
# Any stations will be in subset of daily that are not in stations at all
daily_station.count() #how = left 1000, how = leftanti 0
"""
Because the subeset of daily data has 1000 rows, it is shown that the number of station rows would be matching with the daily subset rows. 
It would confirm that there is null any stations in the subest of daily that are not in stations at all. 
Just pick up the left part, we can find the count is zero which can confirm there is null any stations in the subest of daily that are not in stations at all.
"""

"""
The cost to know the question f by using `LEFT JOIN` is based on how many rows the station dataframe has. shuffling 
Another quicker option is make both the daily and station id as hash table(such as set in Python) and doing subset of them. **Set(daily_id) - Set(station_id)**
"""



#Analysis
"""
Increasing resources and use the new data:                                
start_pyspark_shell -e 4 -c 2 -w 4 -m 4
sc.getConf().getAll()                                
"""
"""
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()
N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M 
"""
#Q1-(a)
#How many stations are there in total? 118493
station_new.count()

#How many stations were active in 2021? 38284
station_new.filter(station_new.Lastyear ==2021).count()

#How many stations are in each of the GCOS Surface Network (GSN)? 991
station_new.filter(station_new.GFLAG == 'GSN').count()

#How many stations are in each of the US Historical Climatology Network (HCN)? 1218
station_new.filter(station_new.HCFLAG == 'HCN').count()

#How many stations are in each of the US Climate Reference Network (CRN)? 0
station_new.filter(station_new.HCFLAG == 'CRN').count()

#Are there any stations that are in more than one of these networks? 14
station_new.filter((station_new.GFLAG == 'GSN')&(station_new.HCFLAG == 'HCN') |(station_new.HCFLAG == 'CRN')).count()

#(b)
#Count the total number of stations in each country
num_station_country = station_new.groupBy(F.col('COUNTRY_CODE')).count()
station_num_each_country = country.join(num_station_country
                                        .withColumnRenamed('count', 'STATION_NUM_EACH_COUNTRY')
                                        .withColumnRenamed('COUNTRY_CODE','CODE'),
                                        on = 'CODE',
                                        how = 'left')

# station_num_each_country.cache()
# station_num_each_country.show()
+----+--------------------+------------------------+
|CODE|        COUNTRY_NAME|STATION_NUM_EACH_COUNTRY|
+----+--------------------+------------------------+
|  AU|             Austria|                      13|
|  BA|             Bahrain|                       1|
|  BB|            Barbados|                       1|
|  CQ|Northern Mariana ...|                      11|
|  DR|  Dominican Republic|                       5|
|  EU|Europa Island [Fr...|                       1|
|  EZ|      Czech Republic|                      12|
|  FR|              France|                     111|
|  MY|            Malaysia|                      16|
|  TI|          Tajikistan|                      62|
|  AJ|          Azerbaijan|                      66|
|  BG|          Bangladesh|                      10|
|  BL|             Bolivia|                      36|
|  CA|              Canada|                    8910|
|  IN|               India|                    3807|
|  IZ|                Iraq|                       1|
|  JM|             Jamaica|                       3|
|  MX|              Mexico|                    5249|
|  MZ|          Mozambique|                      19|
|  NI|             Nigeria|                      10|
+----+--------------------+------------------------+
#Store the output in country
station_num_each_country.write.csv("hdfs:///user/pwu17/outputs/ghcnd/station_num_each_country.csv",mode = 'overwrite', header = True)

#Same way for state and save a copy of each table to output directory
num_station_state = station_new.groupBy(F.col('STATE')).count()
station_num_state = state.join(num_station_state
                                        .withColumnRenamed('count', 'STATION_NUM_STATE')
                                        .withColumnRenamed('STATE','CODE'),
                                        on = 'CODE',
                                        how = 'left')
# station_num_state.cache()
# station_num_state.show()                                     
+----+--------------------+-----------------+
|CODE|          STATE_NAME|STATION_NUM_STATE|
+----+--------------------+-----------------+
|  IL|            ILLINOIS|             1889|
|  NJ|          NEW JERSEY|              739|
|  NT|NORTHWEST TERRITO...|              137|
|  PI|     PACIFIC ISLANDS|             null|
|  CA|          CALIFORNIA|             2879|
|  IN|             INDIANA|             1741|
|  OK|            OKLAHOMA|             1018|
|  WY|             WYOMING|             1211|
|  CT|         CONNECTICUT|              350|
|  MN|           MINNESOTA|             1557|
|  WV|       WEST VIRGINIA|              516|
|  MT|             MONTANA|             1240|
|  ND|        NORTH DAKOTA|              545|
|  NH|       NEW HAMPSHIRE|              431|
|  OH|                OHIO|             1179|
|  WI|           WISCONSIN|             1102|
|  AZ|             ARIZONA|             1534|
|  ID|               IDAHO|              794|
|  MB|            MANITOBA|              722|
|  SD|        SOUTH DAKOTA|             1035|
+----+--------------------+-----------------+

#Store the output in state
station_num_state.write.csv("hdfs:///user/pwu17/outputs/ghcnd/station_num_state.csv", mode = 'overwrite', header = True)

#(c) How many stations are there in the Northern Hemisphere only? 93156
station_new.filter(station_new.LATITUDE >= 0).count()

#How many stations are there in total in the territories of the United States around the world, excluding the United States itself? 339
territory_US = (
                station_num_each_country
                .filter((F.col('COUNTRY_NAME')
                .contains('United States')) & (~F.col('COUNTRY_NAME')
                .startswith('United States'))))
#territory_US.cache()
#territory_US.show()
+----+--------------------+------------------------+
|CODE|        COUNTRY_NAME|STATION_NUM_EACH_COUNTRY|
+----+--------------------+------------------------+
|  CQ|Northern Mariana ...|                      11|
|  WQ|Wake Island [Unit...|                       1|
|  AQ|American Samoa [U...|                      21|
|  LQ|Palmyra Atoll [Un...|                       3|
|  GQ|Guam [United States]|                      21|
|  JQ|Johnston Atoll [U...|                       4|
|  MQ|Midway Islands [U...|                       2|
|  VQ|Virgin Islands [U...|                      54|
|  RQ|Puerto Rico [Unit...|                     222|
+----+--------------------+------------------------+
territory_US.select(F.sum('STATION_NUM_EACH_COUNTRY')).show()
+-----------------------------+
|sum(STATION_NUM_EACH_COUNTRY)|
+-----------------------------+
|                          339|
+-----------------------------+

#Q2-(a)
#Computes the geographical distance between two stations using their latitude and longitude as arguments.
"""
method 1st
Install geopy library
pip install geopy
from geopy.distance import geodestic
def stations_distance(dis1,dis2):
    return geodestic(dis1,dis2).km
"""
import math
 
def stations_distance(lat1,lon1,lat2,lon2):
    """point1: loca1 -- tuple of float (lat,lon) 
    and point2: loca2 ---tuple of float (lat,lon)
    """
    radius = 6371  #km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)  
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    
    circle = 2 * math.asin(math.sqrt(a))
    distance = radius * circle
    return distance   
    
# Wrapping the function using udf 
udf_distance = F.udf(stations_distance,DoubleType())

#test the function by using CROSS JOIN on a small subset of stations to generate with two stations in each row.
# set up two subset according to the different filter
subset = (station_new.filter(station_new.LONGITUDE < 25).limit(10).select(F.col('STATION_ID'),
         F.col('LATITUDE'),F.col('LONGITUDE')))
#subset.cache()
#subset.show()
+-----------+--------+---------+
| STATION_ID|LATITUDE|LONGITUDE|
+-----------+--------+---------+
|AGE00147718|   34.85|     5.72|
|AGM00060417|  36.383|    3.883|
|AGM00060421|  35.867|    7.117|
|AGM00060531|  35.017|    -1.45|
|AGM00060555|  33.068|    6.089|
|AO000066447| -15.833|    20.35|
|AQC00914594|-14.3333|-170.7667|
|AQW00061705|-14.3306|-170.7136|
|AR000087828|   -43.2|  -65.266|
|AR000087925| -51.617|  -69.283|
+-----------+--------+---------+

subset_new = (station_new.filter(station_new.LONGITUDE > 30 ).limit(10).select(F.col('STATION_ID').alias('STATION_ID_NEW'),
              F.col('LATITUDE').alias('LATITUDE_NEW'),
              F.col('LONGITUDE').alias('LONGITUDE_NEW')))
#subset_new.cache()
#subset_new.show()
+--------------+------------+-------------+
|STATION_ID_NEW|LATITUDE_NEW|LONGITUDE_NEW|
+--------------+------------+-------------+
|   AJ000037679|        41.1|         49.2|
|   AJ000037831|        40.4|         47.0|
|   AJ000037981|        38.9|         48.2|
|   AJ000037989|        38.5|         48.9|
|   AM000037719|        40.6|        45.35|
|   AM000037897|      39.533|       46.017|
|   ASN00001003|    -14.1331|     126.7158|
|   ASN00001020|      -14.09|     126.3867|
|   ASN00002031|    -17.0103|     128.4669|
|   ASN00002033|       -17.8|        128.3|
+--------------+------------+-------------+
#test the function by using CROSS JOIN on a small subset of stations to generate with two stations in each row.
#test1
subsets_cross_test1 = (subset
                       .crossJoin(subset)
                       .toDF('STATION_ID_A', 'LATITUDE_A', 'LONGITUDE_A',
                             'STATION_ID_A1', 'LATITUDE_A1', 'LONGITUDE_A1'))
                             
# Remove repeated rows    
subsets_cross_test1 = (subsets_cross_test1.filter(subsets_cross_test1.STATION_ID_A != subsets_cross_test1.STATION_ID_A1))
#subsets_cross_test1.show()
pair_distance1 = subsets_cross_test1.withColumn('Distance/km', udf_distance(F.col('LATITUDE_A'),F.col('LONGITUDE_A'),F.col('LATITUDE_A1'),F.col("LONGITUDE_A1")))
pair_distance1.show()
+------------+----------+-----------+-------------+-----------+------------+------------------+
|STATION_ID_A|LATITUDE_A|LONGITUDE_A|STATION_ID_A1|LATITUDE_A1|LONGITUDE_A1|       Distance/km|
+------------+----------+-----------+-------------+-----------+------------+------------------+
| AGE00147718|     34.85|       5.72|  AGM00060417|     36.383|       3.883| 237.9625309815391|
| AGE00147718|     34.85|       5.72|  AGM00060421|     35.867|       7.117| 169.8130914156512|
| AGE00147718|     34.85|       5.72|  AGM00060531|     35.017|       -1.45| 653.7369539465011|
| AGE00147718|     34.85|       5.72|  AGM00060555|     33.068|       6.089|201.04990595869236|
| AGE00147718|     34.85|       5.72|  AO000066447|    -15.833|       20.35| 5843.749552413994|
| AGE00147718|     34.85|       5.72|  AQC00914594|   -14.3333|   -170.7667|17706.723155600444|
| AGE00147718|     34.85|       5.72|  AQW00061705|   -14.3306|   -170.7136|17705.608418388274|
| AGE00147718|     34.85|       5.72|  AR000087828|      -43.2|     -65.266|11266.138895690467|
| AGE00147718|     34.85|       5.72|  AR000087925|    -51.617|     -69.283|12056.386865502114|
| AGM00060417|    36.383|      3.883|  AGE00147718|      34.85|        5.72| 237.9625309815391|
| AGM00060417|    36.383|      3.883|  AGM00060421|     35.867|       7.117| 296.0612631656579|
| AGM00060417|    36.383|      3.883|  AGM00060531|     35.017|       -1.45| 504.8698926894565|
| AGM00060417|    36.383|      3.883|  AGM00060555|     33.068|       6.089| 420.1070853594153|
| AGM00060417|    36.383|      3.883|  AO000066447|    -15.833|       20.35| 6058.442382575106|
| AGM00060417|    36.383|      3.883|  AQC00914594|   -14.3333|   -170.7667|17506.229431953736|
| AGM00060417|    36.383|      3.883|  AQW00061705|   -14.3306|   -170.7136|17504.810594290204|
| AGM00060417|    36.383|      3.883|  AR000087828|      -43.2|     -65.266| 11271.99553876459|
| AGM00060417|    36.383|      3.883|  AR000087925|    -51.617|     -69.283|12084.214472603207|
| AGM00060421|    35.867|      7.117|  AGE00147718|      34.85|        5.72| 169.8130914156512|
| AGM00060421|    35.867|      7.117|  AGM00060417|     36.383|       3.883| 296.0612631656579|
+------------+----------+-----------+-------------+-----------+------------+------------------+

#test2
subsets_cross_test2 = (subset_new.crossJoin(subset_new)
                       .toDF('STATION_ID_B', 'LATITUDE_B', 'LONGITUDE_B',
                             'STATION_ID_B1', 'LATITUDE_B1', 'LONGITUDE_B1'))
# Remove repeated rows    
subsets_cross_test2 = (subsets_cross_test2.filter(subsets_cross_test2.STATION_ID_B != subsets_cross_test2.STATION_ID_B1))                 
pair_distance2 = subsets_cross_test2.withColumn('Distance/km', udf_distance(F.col('LATITUDE_B'),F.col('LONGITUDE_B'),F.col('LATITUDE_B1'),F.col("LONGITUDE_B1")))
pair_distance2.show()
#subsets_cross_test2.show()
#pair_distance2.show()
+------------+----------+-----------+-------------+-----------+------------+------------------+
|STATION_ID_B|LATITUDE_B|LONGITUDE_B|STATION_ID_B1|LATITUDE_B1|LONGITUDE_B1|       Distance/km|
+------------+----------+-----------+-------------+-----------+------------+------------------+
| AJ000037679|      41.1|       49.2|  AJ000037831|       40.4|        47.0| 200.9966179871759|
| AJ000037679|      41.1|       49.2|  AJ000037981|       38.9|        48.2|259.02881661710325|
| AJ000037679|      41.1|       49.2|  AJ000037989|       38.5|        48.9| 290.2397826660386|
| AJ000037679|      41.1|       49.2|  AM000037719|       40.6|       45.35|328.53500871456635|
| AJ000037679|      41.1|       49.2|  AM000037897|     39.533|      46.017|321.19611401121165|
| AJ000037679|      41.1|       49.2|  ASN00001003|   -14.1331|    126.7158|10023.772778988232|
| AJ000037679|      41.1|       49.2|  ASN00001020|     -14.09|    126.3867| 9994.430241013553|
| AJ000037679|      41.1|       49.2|  ASN00002031|   -17.0103|    128.4669|10377.978436765612|
| AJ000037679|      41.1|       49.2|  ASN00002033|      -17.8|       128.3| 10423.75403335335|
| AJ000037831|      40.4|       47.0|  AJ000037679|       41.1|        49.2| 200.9966179871759|
| AJ000037831|      40.4|       47.0|  AJ000037981|       38.9|        48.2|195.88996209128086|
| AJ000037831|      40.4|       47.0|  AJ000037989|       38.5|        48.9|266.91029995137035|
| AJ000037831|      40.4|       47.0|  AM000037719|       40.6|       45.35| 141.2721421837874|
| AJ000037831|      40.4|       47.0|  AM000037897|     39.533|      46.017|127.71697264012418|
| AJ000037831|      40.4|       47.0|  ASN00001003|   -14.1331|    126.7158|10175.833886213934|
| AJ000037831|      40.4|       47.0|  ASN00001020|     -14.09|    126.3867| 10146.07327143163|
| AJ000037831|      40.4|       47.0|  ASN00002031|   -17.0103|    128.4669|10527.670730756303|
| AJ000037831|      40.4|       47.0|  ASN00002033|      -17.8|       128.3|10571.799543614088|
| AJ000037981|      38.9|       48.2|  AJ000037679|       41.1|        49.2|259.02881661710325|
| AJ000037981|      38.9|       48.2|  AJ000037831|       40.4|        47.0|195.88996209128086|
+------------+----------+-----------+-------------+-----------+------------+------------------+

#(b)
#Apply the above function to compute the pairwise distances between all stations in New Zealand, and save the result to your output directory.
station_NZ = station_new.filter(F.col('COUNTRY_CODE') == 'NZ')
station_NZ.count() #15

station_NZ_1 = station_NZ.select(F.col('COUNTRY_CODE').alias('COUNTRY_CODE1'),
                                 F.col('STATION_ID').alias('ID_1'),
                                 F.col('NAME').alias('NAME_1'),
                                 F.col('LATITUDE').alias('LAT_1'),
                                 F.col('LONGITUDE').alias('LON_1'))
station_NZ_pairs = (
    station_NZ.join(station_NZ_1, station_NZ.COUNTRY_CODE == station_NZ_1.COUNTRY_CODE1)
    .select(station_NZ.COUNTRY_CODE,
            F.col('STATION_ID').alias('ID'),
            F.col('NAME'),
            F.col('LATITUDE').alias('LAT'),
            F.col('LONGITUDE').alias('LON'),
            F.col('ID_1'),F.col('NAME_1'),F.col('LAT_1'),F.col('LON_1') 
            ).where(F.col('ID')<F.col('ID_1')))
           
# cross_join method
"""
station_NZ = station_NZ.select(F.col('STATION_ID'),F.col('NAME'),F.col('LATITUDE'),F.col('LONGITUDE'))
nz_station_pairs = (station_NZ.crossJoin(station_NZ).toDF(
'STATION_ID', 'STATION_NAME', 'LATITUDE', 'LONGITUDE',
'STATION_ID1', 'STATION_NAME1','LATITUDE1', 'LONGITUDE1').where(F.col('STATION_ID')<F.col('STATION_ID1')))

nz_station_pairs = (nz_station_pairs.filter(
nz_station_pairs.STATION_ID!= nz_station_pairs.STATION_ID1))

nz_pairs_distance = (nz_station_pairs.withColumn('DISTANCE/km', udf_distance(
nz_station_pairs.LONGITUDE, nz_station_pairs.LATITUDE,
nz_station_pairs.LONGITUDE1, nz_station_pairs.LATITUDE1)))

+-----------+------------------+--------+---------+-----------+-------------------+---------+----------+------------------+
| STATION_ID|      STATION_NAME|LATITUDE|LONGITUDE|STATION_ID1|      STATION_NAME1|LATITUDE1|LONGITUDE1|       DISTANCE/km|
+-----------+------------------+--------+---------+-----------+-------------------+---------+----------+------------------+
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZM00093110|  AUCKLAND AERO AWS|    -37.0|     174.8| 760.2041153505343|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZM00093678|           KAIKOURA|  -42.417|     173.7|303.91928614759865|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZ000937470|         TARA HILLS|  -44.517|     169.9| 231.2066931075592|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZ000939870|CHATHAM ISLANDS AWS|   -43.95|  -176.567|1391.1062826690936|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|192.05151538644918|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZ000939450|CAMPBELL ISLAND AWS|   -52.55|   169.167| 1095.695676736767|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZM00093929| ENDERBY ISLAND AWS|  -50.483|     166.3|  993.701216407711|
|NZ000936150|HOKITIKA AERODROME| -42.717|  170.983|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8| 451.0580473229305|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983|1936.6161987416351|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZM00093110|  AUCKLAND AERO AWS|    -37.0|     174.8|1181.9079286521144|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZM00093678|           KAIKOURA|  -42.417|     173.7|1733.7078465926768|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZ000937470|         TARA HILLS|  -44.517|     169.9|2166.0828544899127|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZ000939870|CHATHAM ISLANDS AWS|   -43.95|  -176.567|1639.5103359819034|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZM00093781|  CHRISTCHURCH INTL|  -43.489|   172.532|1903.5083175384607|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZ000939450|CAMPBELL ISLAND AWS|   -52.55|   169.167| 2950.701275794127|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZM00093929| ENDERBY ISLAND AWS|  -50.483|     166.3|2925.8257858870465|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZM00093439|WELLINGTON AERO AWS|  -41.333|     174.8|1567.5541198067197|
|NZ000093994|RAOUL ISL/KERMADEC|  -29.25| -177.917|NZ000933090|   NEW PLYMOUTH AWS|  -39.017|   174.183|1395.7115542309687|
|NZ000093012|           KAITAIA|   -35.1|  173.267|NZ000936150| HOKITIKA AERODROME|  -42.717|   170.983| 876.5139825414557|
|NZ000093012|           KAITAIA|   -35.1|  173.267|NZ000093994| RAOUL ISL/KERMADEC|   -29.25|  -177.917|1175.8309386605417|
+-----------+------------------+--------+---------+-----------+-------------------+---------+----------+------------------+

"""

station_NZ_pairs.show()
station_NZ_pairs.count() #105, unique pair validated

NZ_stations_distance = station_NZ_pairs.withColumn('Distance/km', udf_distance(F.col('LAT'),F.col('LON'),F.col('LAT_1'),F.col("LON_1")))
NZ_stations_distance.show()
+------------+-----------+------------------+-------+--------+-----------+-------------------+-------+--------+------------------+
|COUNTRY_CODE|         ID|              NAME|    LAT|     LON|       ID_1|             NAME_1|  LAT_1|   LON_1|       Distance/km|
+------------+-----------+------------------+-------+--------+-----------+-------------------+-------+--------+------------------+
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093110|  AUCKLAND AERO AWS|  -37.0|   174.8| 714.1268050318199|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093678|           KAIKOURA|-42.417|   173.7|224.98088249003268|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZ000937470|         TARA HILLS|-44.517|   169.9| 218.3091932907015|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZ000939870|CHATHAM ISLANDS AWS| -43.95|-176.567|1015.2499467565266|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093781|  CHRISTCHURCH INTL|-43.489| 172.532|152.25804856764034|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZ000939450|CAMPBELL ISLAND AWS| -52.55| 169.167| 1101.719032102152|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093929| ENDERBY ISLAND AWS|-50.483|   166.3| 934.2477752798079|
|          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093439|WELLINGTON AERO AWS|-41.333|   174.8|350.79606843225486|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000936150| HOKITIKA AERODROME|-42.717| 170.983|1796.3613283746295|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093110|  AUCKLAND AERO AWS|  -37.0|   174.8|1095.8224733430584|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093678|           KAIKOURA|-42.417|   173.7|1645.5647898351037|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000937470|         TARA HILLS|-44.517|   169.9|  2008.88592155291|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000939870|CHATHAM ISLANDS AWS| -43.95|-176.567| 1638.937300349759|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093781|  CHRISTCHURCH INTL|-43.489| 172.532|1796.5560036960667|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000939450|CAMPBELL ISLAND AWS| -52.55| 169.167| 2799.175956993722|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093929| ENDERBY ISLAND AWS|-50.483|   166.3|2705.4207241687313|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093439|WELLINGTON AERO AWS|-41.333|   174.8|1495.9413873304925|
|          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000933090|   NEW PLYMOUTH AWS|-39.017| 174.183| 1305.703712037678|
|          NZ|NZ000093012|           KAITAIA|  -35.1| 173.267|NZ000936150| HOKITIKA AERODROME|-42.717| 170.983| 869.6235232640119|
|          NZ|NZ000093012|           KAITAIA|  -35.1| 173.267|NZ000093994| RAOUL ISL/KERMADEC| -29.25|-177.917|1053.5275724206938|
+------------+-----------+------------------+-------+--------+-----------+-------------------+-------+--------+------------------+

# Save the result to your output directory
NZ_stations_distance.write.csv("hdfs:///user/pwu17/outputs/ghcnd/NZ_stations_distance.csv", mode = 'overwrite', header = True)

# What two stations are geographically the closest together in New Zealand? 50.53
NZ_stations_distance.sort(F.col('DISTANCE/km')).show(1)


# Q3-(a)
#Exploring all of the daily climate summaries in more detail
#How many blocks are required for the daily climate summaries for the year 2022?

View the blocks for the specific file 2022.csv.gz
$ hdfs dfs -ls /data/ghcnd/daily/2022.csv.gz  #25985757 bytes
$ hdfs getconf -confKey "dfs.blocksize" #134217728 bytes default block size
$ hdfs fsck /data/ghcnd/daily/2022.csv.gz -files -blocks
Showing:  /data/ghcnd/daily/2022.csv.gz 25985757 bytes, replicated: replication=8, 1 block(s):  OK
          0. BP-700027894-132.181.129.68-1626517177804:blk_1073769759_28939 len=25985757 Live_repl=8
          Total blocks (validated):      1 (avg. block size 25985757 B)

#What about the year 2021?

View the blocks for the specific file 2021.csv.gz
$ hdfs dfs -ls /data/ghcnd/daily/2021.csv.gz #146936025 bytes
$ hdfs fsck /data/ghcnd/daily/2021.csv.gz -files -blocks
Showing: /data/ghcnd/daily/2021.csv.gz 146936025 bytes, replicated: replication=8, 2 block(s):  OK
         0. BP-700027894-132.181.129.68-1626517177804:blk_1073769757_28937 len=134217728 Live_repl=8
         1. BP-700027894-132.181.129.68-1626517177804:blk_1073769758_28938 len=12718297 Live_repl=8
         Total blocks (validated):      2 (avg. block size 73468012 B)


# (b)
# Load and count the number of observations in 2021 and then separately in 2022.
daily_2021 = spark.read.csv("/data/ghcnd/daily/2021.csv.gz")
daily_2021.count() #34657282
daily_2022 = spark.read.csv("/data/ghcnd/daily/2022.csv.gz")
daily_2022.count() #5971307
# How many tasks were executed by each stage of each job?

2021.csv.gz has 1 task for csv loading and 2 tasks for count
2022.csv.gz has 1 task for csv loading and 2 tasks for count

#Did the number of tasks executed correspond to the number of blocks in each input? 
#No, we can think about this question from spark process, namely:inputsplit, inputformat, shuffling, tasks, blocks and jobs etc. aspects to explian.


# (c)
# Load and count the number of observations from 2014 to 2022
daily_1422 =  (spark.read.option("delimiter", ",")
                 .option("header","false")
                 .option("inferSchema","false")
                 .option("dateFormat", "yyyyMMdd")
                 .option("timestampFormat", "hhmm")
                 .schema(dailySchema)
                 .csv("hdfs:///data/ghcnd/daily/20{1[4-9],2[0-2]}.csv.gz")
                )
daily_1422 = daily_1422.withColumn('COUNTRY_CODE', daily_1422.ID.substr(1, 2))
daily_1422.count() #284918108
daily_1422.show()
+-----------+----------+-------+------+-----+-----+-----+-------------------+------------+
|         ID|      DATE|ELEMENT| VALUE|MFLAG|QFLAG|SFLAG|           OBV_TIME|COUNTRY_CODE|
+-----------+----------+-------+------+-----+-----+-----+-------------------+------------+
|US1FLSL0019|2016-01-01|   PRCP|   3.0| null| null|    N|               null|          US|
|NOE00133566|2016-01-01|   TMAX|  95.0| null| null|    E|               null|          NO|
|NOE00133566|2016-01-01|   TMIN|  23.0| null| null|    E|               null|          NO|
|NOE00133566|2016-01-01|   PRCP|  10.0| null| null|    E|               null|          NO|
|USS0018D08S|2016-01-01|   TMAX| -25.0| null| null|    T|               null|          US|
|USS0018D08S|2016-01-01|   TMIN|-177.0| null| null|    T|               null|          US|
|USS0018D08S|2016-01-01|   TOBS| -61.0| null| null|    T|               null|          US|
|USS0018D08S|2016-01-01|   PRCP|   0.0| null| null|    T|               null|          US|
|USS0018D08S|2016-01-01|   SNWD| 279.0| null| null|    T|               null|          US|
|USS0018D08S|2016-01-01|   TAVG|-104.0| null| null|    T|               null|          US|
|USS0018D08S|2016-01-01|   WESD| 610.0| null| null|    T|               null|          US|
|USC00141761|2016-01-01|   TMAX|  22.0| null| null|    7|1970-01-01 07:00:00|          US|
|USC00141761|2016-01-01|   TMIN| -89.0| null| null|    7|1970-01-01 07:00:00|          US|
|USC00141761|2016-01-01|   TOBS| -89.0| null| null|    7|1970-01-01 07:00:00|          US|
|USC00141761|2016-01-01|   PRCP|   0.0| null| null|    7|1970-01-01 07:00:00|          US|
|USC00141761|2016-01-01|   SNOW|   0.0| null| null|    7|               null|          US|
|USC00141761|2016-01-01|   SNWD|   0.0| null| null|    7|1970-01-01 07:00:00|          US|
|MXM00076423|2016-01-01|   TMAX| 262.0| null| null|    S|               null|          MX|
|MXM00076423|2016-01-01|   TMIN|  68.0| null| null|    S|               null|          MX|
|MXM00076423|2016-01-01|   PRCP|   0.0| null| null|    S|               null|          MX|
+-----------+----------+-------+------+-----+-----+-----+-------------------+------------+

# How many tasks were executed by each stage, and how does this number correspond to your input?
"""
There are one jobs in the load and count the data range seperately. 
Count part includes 2 stages, 10 tasks. 
Show part includes 1 stage, 1 task

"""

# Q4-(a) start_pyspark_shell -e 4 -c 2 -w 4 -m 4
import sys
import os
from pyspark import SparkContext
from pyspark.sql import Window
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
dailySchema = StructType([ \
    StructField("ID",StringType(),True), \
    StructField("DATE",DateType(),True), \
    StructField("ELEMENT",StringType(),True), \
    StructField("VALUE", FloatType(), True), \
    StructField("MFLAG", StringType(), True), \
    StructField("QFLAG", StringType(), True), \
    StructField("SFLAG", StringType(), True), \
    StructField("OBV_TIME", TimestampType(), True), \
])
 
all_daily = (spark.read.option("delimiter", ",")
         .option("header","false")
         .option("dateFormat", "yyyyMMdd")
         .option("timestampFormat", "hhmm")
         .schema(dailySchema)
         .csv("hdfs:///data/ghcnd/daily/"))         
         
all_daily = all_daily.withColumn('COUNTRY_CODE', all_daily.ID.substr(1, 2))
all_daily.count() #3000243596
all_daily.show()

+-----------+----------+-------+------+-----+-----+-----+-------------------+------------+
|         ID|      DATE|ELEMENT| VALUE|MFLAG|QFLAG|SFLAG|           OBV_TIME|COUNTRY_CODE|
+-----------+----------+-------+------+-----+-----+-----+-------------------+------------+
|CA002303986|2010-01-01|   TMAX| 205.0| null|    G|    C|               null|          CA|
|CA002303986|2010-01-01|   TMIN|-300.0| null| null|    C|               null|          CA|
|CA002303986|2010-01-01|   PRCP|   4.0| null| null|    C|               null|          CA|
|CA002303986|2010-01-01|   SNOW|   4.0| null| null|    C|               null|          CA|
|CA002303986|2010-01-01|   SNWD|   0.0| null|    I|    C|               null|          CA|
|US1FLSL0019|2010-01-01|   PRCP|   0.0|    T| null|    N|               null|          US|
|ASN00037003|2010-01-01|   PRCP|   0.0| null| null|    a|               null|          AS|
|US1AZMR0019|2010-01-01|   PRCP|   0.0| null| null|    N|               null|          US|
|US1AZMR0019|2010-01-01|   SNOW|   0.0| null| null|    N|               null|          US|
|USC00178998|2010-01-01|   TMAX|   0.0| null| null|    0|1800-01-01 00:00:00|          US|
|USC00178998|2010-01-01|   TMIN| -56.0| null| null|    0|1800-01-01 00:00:00|          US|
|USC00178998|2010-01-01|   TOBS| -56.0| null| null|    0|1800-01-01 00:00:00|          US|
|USC00178998|2010-01-01|   PRCP|  43.0| null| null|    0|1970-01-01 07:00:00|          US|
|USC00178998|2010-01-01|   SNOW|  46.0| null| null|    0|               null|          US|
|USC00178998|2010-01-01|   SNWD| 102.0| null| null|    0|               null|          US|
|NOE00133566|2010-01-01|   TMAX|   2.0| null| null|    E|               null|          NO|
|NOE00133566|2010-01-01|   TMIN| -84.0| null| null|    E|               null|          NO|
|NOE00133566|2010-01-01|   PRCP|  85.0| null| null|    E|               null|          NO|
|NOE00133566|2010-01-01|   SNWD| 490.0| null| null|    E|               null|          NO|
|USC00242347|2010-01-01|   TMAX|  33.0| null| null|    0|1970-01-01 08:00:00|          US|
+-----------+----------+-------+------+-----+-----+-----+-------------------+------------+



#(b)
coreElements = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]
all_daily_coreE = all_daily.filter(F.col('ELEMENT').isin(coreElements))
#How many observations are there for each of the five core elements? 2565647147 
all_daily_coreE.count()
#Which element has the most observations?
all_daily_coreE.groupBy('ELEMENT').agg({'ELEMENT':'count'}).show()
+-------+--------------+
|ELEMENT|count(ELEMENT)|
+-------+--------------+
|   SNOW|     341985067|
|   SNWD|     289981374|
|   PRCP|    1043785667|
|   TMIN|     444271327|
|   TMAX|     445623712|
+-------+--------------+

#(c)
#How many observations of TMIN do not have a corresponding observation of TMAX?
#Using filter to get the TMAX and TMIN
"""
TMAX_daily = all_daily_coreE.filter(F.col('ELEMENT') == 'TMAX')
TMIN_daily = all_daily_coreE.filter(F.col('ELEMENT') == 'TMIN')
diff_TMINMAX = TMIN_daily.select('ID','DATE').subtract(TMAX_daily.select('ID','DATE'))
diff_TMINMAX.count() #8808805
"""
daily_TT = (all_daily_coreE
            .filter((F.col('ELEMENT') == 'TMAX') | (F.col('ELEMENT') == 'TMIN'))
            .groupBy(F.col('ID'),F.col('DATE'))
            .agg(F.collect_set("ELEMENT").cast('string').alias("TMIN_TMAX")))
daily_TT.filter(F.col('TMIN_TMAX') == '[TMIN]').count() #8808805

#How many different stations contributed to these observations? 
daily_TT.filter(F.col('TMIN_TMAX') == '[TMIN]').select('ID').distinct().count() #27650


#(d) 
#Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand.
daily_TTNZ = all_daily.filter((F.col('COUNTRY_CODE')=='NZ') & ((F.col('ELEMENT') == 'TMAX') | (F.col('ELEMENT') == 'TMIN')))
#daily_TTNZ.count()
#daily_TTNZ.cache()
#daily_TTNZ.show()
+-----------+----------+-------+-----+-----+-----+-----+--------+------------+
|         ID|      DATE|ELEMENT|VALUE|MFLAG|QFLAG|SFLAG|OBV_TIME|COUNTRY_CODE|
+-----------+----------+-------+-----+-----+-----+-----+--------+------------+
|NZ000936150|2010-01-01|   TMAX|324.0| null| null|    S|    null|          NZ|
|NZM00093110|2010-01-01|   TMAX|215.0| null| null|    S|    null|          NZ|
|NZM00093110|2010-01-01|   TMIN|153.0| null| null|    S|    null|          NZ|
|NZM00093678|2010-01-01|   TMAX|242.0| null| null|    S|    null|          NZ|
|NZM00093678|2010-01-01|   TMIN| 94.0| null| null|    S|    null|          NZ|
|NZ000093292|2010-01-01|   TMAX|297.0| null| null|    S|    null|          NZ|
|NZ000093292|2010-01-01|   TMIN| 74.0| null| null|    S|    null|          NZ|
|NZM00093781|2010-01-01|   TMAX|324.0| null| null|    S|    null|          NZ|
|NZM00093439|2010-01-01|   TMAX|204.0| null| null|    S|    null|          NZ|
|NZM00093439|2010-01-01|   TMIN|134.0| null| null|    S|    null|          NZ|
|NZ000093844|2010-01-01|   TMAX|232.0| null| null|    S|    null|          NZ|
|NZ000093844|2010-01-01|   TMIN| 96.0| null| null|    S|    null|          NZ|
|NZ000093417|2010-01-01|   TMAX|180.0| null| null|    S|    null|          NZ|
|NZ000093417|2010-01-01|   TMIN|125.0| null| null|    S|    null|          NZ|
|NZ000933090|2010-01-01|   TMAX|197.0| null| null|    S|    null|          NZ|
|NZ000933090|2010-01-01|   TMIN| 82.0| null| null|    S|    null|          NZ|
|NZM00093110|2010-01-02|   TMAX|241.0| null| null|    S|    null|          NZ|
|NZM00093110|2010-01-02|   TMIN|153.0| null| null|    S|    null|          NZ|
|NZM00093678|2010-01-02|   TMAX|289.0| null| null|    S|    null|          NZ|
|NZ000093292|2010-01-02|   TMAX|302.0| null| null|    S|    null|          NZ|
+-----------+----------+-------+-----+-----+-----+-----+--------+------------+

#Save the result to your output directory
daily_TTNZ.sort('ID','DATE').write.csv("hdfs:///user/pwu17/outputs/ghcnd/daily_TMAX_TMIN_NZ", mode = 'overwrite', header = True)

# How many observations are there? 472271
daily_TTNZ.count()
# copy the output from HDFS to local home directory
$ hdfs dfs -copyToLocal hdfs:///user/pwu17/outputs/ghcnd/daily_TMAX_TMIN_NZ/ /users/home/pwu17
# count the rows by bash 472287
wc -l `find /users/home/pwu17/daily_TMAX_TMIN_NZ/*.csv -type f`

daily_TTNZ_V = daily_TTNZ.groupBy('ID','DATE','ELEMENT').agg(F.avg('VALUE').alias('AVERAGE'))
YEAR_SUB = F.substring(F.col('DATE'),1,4)
daily_TTNZ_V = daily_TTNZ_V.withColumn('YEAR',YEAR_SUB)
#daily_TTNZ_V.show()
+-----------+----------+-------+-------+----+
|         ID|      DATE|ELEMENT|AVERAGE|YEAR|
+-----------+----------+-------+-------+----+
|NZM00093678|2010-01-01|   TMIN|   94.0|2010|
|NZM00093781|2010-01-03|   TMAX|  294.0|2010|
|NZ000093844|2010-01-03|   TMAX|  180.0|2010|
|NZM00093781|2010-01-06|   TMAX|  277.0|2010|
|NZ000093844|2010-01-09|   TMIN|   64.0|2010|
|NZ000933090|2010-01-09|   TMAX|  190.0|2010|
|NZ000936150|2010-01-10|   TMAX|  234.0|2010|
|NZ000093292|2010-01-10|   TMIN|   71.0|2010|
|NZ000093844|2010-01-10|   TMAX|  140.0|2010|
|NZ000936150|2010-01-11|   TMAX|  191.0|2010|
|NZ000933090|2010-01-12|   TMIN|   74.0|2010|
|NZ000093292|2010-01-14|   TMIN|  105.0|2010|
|NZM00093439|2010-01-14|   TMAX|  229.0|2010|
|NZ000936150|2010-01-19|   TMIN|  137.0|2010|
|NZ000093292|2010-01-19|   TMIN|  133.0|2010|
|NZM00093781|2010-01-19|   TMIN|  137.0|2010|
|NZ000093417|2010-01-19|   TMAX|  230.0|2010|
|NZ000933090|2010-01-19|   TMAX|  220.0|2010|
|NZ000093844|2010-01-20|   TMAX|  242.0|2010|
|NZM00093781|2010-01-23|   TMAX|  152.0|2010|
+-----------+----------+-------+-------+----+


# write to store: 
daily_TTNZ_V.coalesce(1).write.csv("hdfs:///user/pwu17/outputs/ghcnd/daily_TMAX_TMIN_NZ_VALUEAVE", mode = 'overwrite', header = True)

# How many years are covered by the observations? 83
daily_TTNZ_V.select('YEAR').distinct().count()

$ hdfs dfs -copyToLocal /user/pwu17/outputs/ghcnd/daily_TMAX_TMIN_NZ_VALUEAVE/ /users/home/pwu17
wc -l `find /users/home/pwu17/daily_TMAX_TMIN_NZ_VALUEAVE/*.csv -type f`  #472272

#(e)
rainfall = all_daily.filter(F.col('ELEMENT')=='PRCP')
rainfall = (rainfall
            .select('ID','DATE','ELEMENT','VALUE')
            .withColumn('COUNTRY_CODE',F.substring(F.col('ID'),1,2))
            .withColumn('YEAR',F.substring(F.col('DATE'),1,4)))
# Compute the average rainfall in each year for each country, and save this result to your output directory.
rainfall = rainfall.groupBy('COUNTRY_CODE','YEAR').agg(F.avg('VALUE').alias('AVERAGE'))
rainfall_new = rainfall.join(country.withColumnRenamed('CODE','COUNTRY_CODE'),on = 'COUNTRY_CODE',how = 'left')
# rainfall_new.show()
+------------+----+------------------+--------------------+
|COUNTRY_CODE|YEAR|           AVERAGE|        COUNTRY_NAME|
+------------+----+------------------+--------------------+
|          CA|2011| 24.72889263978373|              Canada|
|          GM|2011|19.855456219912764|             Germany|
|          FR|2011|18.202402716113866|              France|
|          CQ|2011| 71.41894892672094|Northern Mariana ...|
|          MT|2011| 16.23076923076923|               Malta|
|          MJ|2011|149.61073825503357|          Montenegro|
|          BG|2011| 68.68304498269896|          Bangladesh|
|          EN|2006| 13.78490093372808|             Estonia|
|          UK|2006|25.741099863512282|      United Kingdom|
|          TU|2006|16.429674029066426|              Turkey|
|          NH|2006| 96.22813688212928|             Vanuatu|
|          SE|2006| 61.54179566563467|          Seychelles|
|          AQ|2006| 135.6931506849315|American Samoa [U...|
|          NS|2006| 75.01689189189189|            Suriname|
|          CS|2006| 95.29468599033817|          Costa Rica|
|          FK|2006| 18.99003322259136|Falkland Islands ...|
|          TO|2006| 71.11070110701107|                Togo|
|          NF|2006| 37.46666666666667|Norfolk Island [A...|
|          GV|2006|            157.75|              Guinea|
|          RQ|2005| 57.45583824019553|Puerto Rico [Unit...|
+------------+----+------------------+--------------------+
rainfall_new.coalesce(1).write.csv("hdfs:///user/pwu17/outputs/ghcnd/rainfall",mode = 'overwrite',header = True)
#Which country has the highest average rainfall in a single year across the entire dataset? EK
rainfall_new.orderBy(F.col('AVERAGE').desc()).show(1,False)
+------------+----+-------+-----------------+
|COUNTRY_CODE|YEAR|AVERAGE|COUNTRY_NAME     |
+------------+----+-------+-----------------+
|EK          |2000|4361.0 |Equatorial Guinea|
+------------+----+-------+-----------------+


$ hdfs dfs -copyToLocal /user/pwu17/outputs/ghcnd/rainfall/ /users/home/pwu17
















    





                                   
                                   
                                   
                                   
                                   
                                   
                                
                                   
                                   








