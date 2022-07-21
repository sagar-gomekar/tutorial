from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
if __name__ == '__main__':
    SparkConf=SparkConf().setAppName("spark context in it").setMaster("local[*]")
    sc=SparkContext(conf=SparkConf)
    # print(sc)

    spark = SparkSession.builder.appName("spark context in it").master("local[*]").getOrCreate()

    lst=[1,2,3,4,5,6,7]
    rdd1=spark.sparkContext.parallelize(lst)
    # print(rdd1.collect())

    maprdd=rdd1.map(lambda x:(x,1))

    # SparkConf=SparkConf().setAppName("jksjdkfjl").setMaster("local[*]")
    # SparkContext=SparkContext(conf=SparkConf)

    # spark=SparkSession.builder.master("local[*]").appName("skfjdj").getOrCreate()

    # create rdd
    # using parallelise collection

    lstq={1:"apple",2:"mango",3:"banana"}
    # rdd2=spark.sparkContext.parallelize(lstq)
    # print(rdd2.collect())

    # from another rdd
    rdds=rdd1.map(lambda x:(x,1))
    # print(rdds.collect())

    # from external file
    inputfilerdd=spark.sparkContext.textFile(r"C:\Users\Admin\Desktop\test.txt")

    # print(inputfilerdd.collect())

    # rddfromcsv=spark.read.format("csv").option("header","False").load(r"C:\Users\Admin\Desktop\sdf.xlsx.csv")
    # print(rddfromcsv)

    df=maprdd.toDF()
    # print(df)
    # df.show()

    # df.printSchema()

    data=[
        (1,"Yash","Patil",29,"M"),
        (2,"ash","Patil",23,"M"),
        (3,"jaYash","Patil",33,"F"),
        ]
    from pyspark.sql.types import *
    dataschema=StructType([StructField(name="id",dataType=IntegerType()),
                           StructField(name="sName",dataType=StringType()),
                           StructField(name="Name",dataType=StringType()),
                           ])
    # inputdf2=spark.createDataFrame(data,schema=dataschema)
    # inputdf2.show()
    # inputdf2.printSchema()

    
