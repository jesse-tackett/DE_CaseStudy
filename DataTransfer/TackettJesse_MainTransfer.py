#Imports for MairaDB to MongoDB
import findspark
findspark.init('C:/spark')
import pymongo as mongo
import time
findspark.find()
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row
conf_new = pyspark.SparkConf().setAppName('appName').setMaster('local[*]')
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

#---------------------------------------------------------------------------
#MARIA TRANSFER-------------------------------------------------------------
#---------------------------------------------------------------------------
def mariaTransfer():
    #####For loop that grabs data from mariaDB###################################
    spark = SparkSession.builder.getOrCreate()
    print('---Loading Data from MariaDB---')
    def importFromMaria():
        tables = ["CDW_SAPP_BRANCH", "CDW_SAPP_CREDITCARD", "CDW_SAPP_CUSTOMER"]
        totalData = []
        for k in tables:
            df = spark.read.format("jdbc").options(
                url      = "jdbc:mysql://localhost:3306/cdw_sapp",
                driver   = "com.mysql.cj.jdbc.Driver",
                dbtable  = k,
                user     = "root",
                password = "root"
            ).load()
            totalData.append(df)
        return(totalData)
    print('---Data Loaded into DataFrames---')
    ############################here we'll transform the dataframes###############

    #Branch Transformation--------------------------------------------------------
    #Giving the import a better name in case of later use
    branch_DF = importFromMaria()[0]
    print('Branch of CDW_SAPP transformation started')

    #Transform functions
    phoneNum = udf(lambda x:  "(" + str(x)[0:3] + ")" + str(x)[3:6] + "-" + str(x)[6:10])
    zipCheck = udf(lambda x: '9999' if x is 'Null' else x)

    #Place and Replace columns -- Phone Number Fix
    branch_cleanPhone = branch_DF.withColumn('BRANCH_PHONE_fix',phoneNum('BRANCH_PHONE'))
    branch_cleanedPhone = branch_cleanPhone.drop('BRANCH_PHONE').withColumnRenamed('BRANCH_PHONE_fix', 'BRANCH_PHONE')

    #Place and Replace columns -- Zip Code Check
    branch_cleanZip = branch_cleanedPhone.withColumn('BRANCH_ZIP_check',zipCheck('BRANCH_ZIP'))
    branch_final = branch_cleanZip.drop('BRANCH_ZIP').withColumnRenamed('BRANCH_ZIP_check','BRANCH_ZIP') 

    print('Here is a single row sample of the Branch transfer')
    branch_final.show(1)


    #CreditCard Transformation====================================================
    #Giving the import a better name in case of later use
    creditCard_DF = importFromMaria()[1]
    print('Credit Card of CDW_SAPP transformation started')

    #Transform functions
    zerosInsert = udf(lambda x: '0'+ str(x) if len(str(x)) == 1 else str(x))

    #Place and Replace columns -- merging timeID like frankenstein
    timeID = creditCard_DF.select(concat(creditCard_DF.YEAR, lit('-'), zerosInsert(creditCard_DF.MONTH), lit('-'), zerosInsert(creditCard_DF.DAY)).alias("TIMEID"))
    timeID = timeID.withColumn("id", monotonically_increasing_id())
    creditCard_DF = creditCard_DF.withColumn('id', monotonically_increasing_id())
    merged = creditCard_DF.join(timeID, "id", "outer")
    creditCard_final = merged.drop('YEAR','MONTH','DAY','id')

    print('Here is a single row sample of the Credit Card transfer')
    creditCard_final.show(1)


    #Customer Transformation++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #Giving the import a better name in case of later use
    customer_DF = importFromMaria()[2]
    print('Customer of CDW_SAPP transformation started')

    #Transform functions
    titleCase = udf(lambda x: x.title())
    lowerCase = udf(lambda x: x.lower())
    phoneNumSmall = udf(lambda x: str(x)[0:3] + "-" + str(x)[3:7])

    #Place and Replace columns -- first name to Title Case
    customer_cleanFirst = customer_DF.withColumn('FIRST_NAME_fix',titleCase('FIRST_NAME'))
    customer_cleanedFirst = customer_cleanFirst.drop('FIRST_NAME').withColumnRenamed('FIRST_NAME_fix', 'FIRST_NAME')

    #Place and Replace columns -- middle name to lower case
    customer_cleanMiddle = customer_cleanedFirst.withColumn('MIDDLE_NAME_fix',lowerCase('MIDDLE_NAME'))
    customer_cleanedMiddle = customer_cleanMiddle.drop('MIDDLE_NAME').withColumnRenamed('MIDDLE_NAME_fix', 'MIDDLE_NAME')

    #Place and Replace columns -- last name to Title Case
    customer_cleanLast = customer_cleanedMiddle.withColumn('LAST_NAME_fix',titleCase('LAST_NAME'))
    customer_cleanedLast = customer_cleanLast.drop('LAST_NAME').withColumnRenamed('LAST_NAME_fix', 'LAST_NAME')

    #Place and Replace columns -- convert Zip to int
    customer_intZip = customer_cleanedLast.withColumn("CUST_ZIP_fix", customer_cleanedLast.CUST_ZIP.cast(IntegerType()))
    customer_zip = customer_intZip.drop("CUST_ZIP").withColumnRenamed("CUST_ZIP_fix", "CUST_ZIP")

    #Place and Replace columns -- transform small Phone Number
    customer_smallPhone = customer_zip.withColumn('CUST_PHONE_fix',phoneNumSmall('CUST_PHONE'))
    customer_cleanedSmallPhone = customer_smallPhone.drop('CUST_PHONE').withColumnRenamed('CUST_PHONE_fix', 'CUST_PHONE')

    #Unholy abomination that makes the joins possible
    customer_hold = customer_cleanedSmallPhone.select(concat(customer_cleanedSmallPhone.APT_NO, lit(', '), customer_cleanedSmallPhone.STREET_NAME).alias('CUST_STREET'))
    customer_hold = customer_hold.withColumn("id", monotonically_increasing_id())
    customer_cold = customer_cleanedSmallPhone.withColumn("id", monotonically_increasing_id())
    merged = customer_cold.join(customer_hold, "id", "outer")
    customer_final = merged.drop('APT_NO','STREET_NAME', 'id')

    print('Here is a single row sample of the Customer transfer')
    customer_final.show(1)


    '''
    ##########here we'll load the transformed dataframes into mongo###############
    #Drop database as to not manually reset every time
    client = mongo.MongoClient()
    client.drop_database('CDW_SAPP')
    client.close()
    '''

    #Loading here
    #For loop that puts the data frames into MongoDB
    print('Now Loading transformed data into MongoDB')
    collections = ['BRANCH', 'CUSTOMER','CREDITCARD']
    dataFrames = [branch_final, customer_final,creditCard_final]
    for k in range(len(collections)):
        uri = "mongodb://127.0.0.1/CD_SAPP.dbs"
        spark_mongodb = SparkSession.builder.config("spark.mongodb.input.uri", uri).config("spark.mongodb.output.uri",uri).getOrCreate()
        dataFrames[k].write.format("com.mongodb.spark.sql.DefaultSource").mode('append').option('database','CDW_SAPP').option('collection', collections[k]).save()
    print('---Transformed Data Loaded---')

#---------------------------------------------------------------------------
#PLAN ATTRIBUTES PULL------------------------------------------------------- # This fucntion has the thought process comments
#--------------------------------------------------------------------------- # Since it is very similar to the other 7 data tansfer functions I'll save time not writing on all of them
def planAttributesPull():

	#As this is a repeating type of code through out the program here I will put the comment explainations here

	#Function to make kafka planAttributes topic
    def kafka_prod_planAttributes():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')#set producers conneciton
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/PlanAttributes.csv")#pull data from github
       data_list = [data for data in response.text.splitlines()[1:]]#take away header and read each line and put into list
       
       for data in data_list:
           producer.send('PlanAttributes', data.encode('utf-8'))#for each item in the list(which is a string line) submit as message in kafka
       producer.flush()#close kafka sending

    #Function to connect spark to kafka and make data frames
    def spark_kafka_planAttribues():
       spark = SparkSession.builder.getOrCreate()#make spark session
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'PlanAttributes').option("startingOffsets", "earliest").load()
       # ^ subscribe to the topic of the github pull and input everything from the earliest message and put into a one column dataframe
       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")#Make sure everything is put into a string
       output_query = kafka_value_df.writeStream.queryName("PlanAttributes").format("memory").start()#make into query-able dataframe

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from PlanAttributes")#take all from other earlier dataframe
       value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))#split things from value_df dataframe and make more columns
       value_row_rdd = value_rdd.map(lambda i: Row(AttributesID=int(i[0]), \
                                                   BeginPrimaryCareCostSharingAfterNumberOfVisits=int(i[1]), \
                                                   BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays=int(i[2]), \
                                                   BenefitPackageId=int(i[3]), \
                                                   BusinessYear=int(i[4]), \
                                                   ChildOnlyOffering=i[5], \
                                                   CompositeRatingOffered=i[6], \
                                                   CSRVariationType=i[7], \
                                                   DentalOnlyPlan=i[8], \
                                                   DiseaseManagementProgramsOffered=i[9], \
                                                   FirstTierUtilization=i[10], \
                                                   HSAOrHRAEmployerContribution=i[11], \
                                                   HSAOrHRAEmployerContributionAmount=i[12], \
                                                   InpatientCopaymentMaximumDays=int(i[13]), \
                                                   IsGuaranteedRate=i[14], \
                                                   IsHSAEligible=i[15], \
                                                   IsNewPlan=i[16], \
                                                   IsNoticeRequiredForPregnancy=i[17], \
                                                   IsReferralRequiredForSpecialist=i[18], \
                                                   IssuerId=int(i[19]), \
                                                   MarketCoverage=i[20], \
                                                   MedicalDrugDeductiblesIntegrated=i[21], \
                                                   MedicalDrugMaximumOutofPocketIntegrated=i[22], \
                                                   MetalLevel=i[23], \
                                                   MultipleInNetworkTiers=i[24], \
                                                   NationalNetwork=i[25], \
                                                   NetworkId=i[26], \
                                                   OutOfCountryCoverage=i[27], \
                                                   OutOfServiceAreaCoverage=i[28], \
                                                   PlanEffectiveDate=i[29], \
                                                   PlanExpirationDate=i[30], \
                                                   PlanId=i[31], \
                                                   PlanLevelExclusions=i[32], \
                                                   PlanMarketingName=i[33], \
                                                   PlanType=i[34], \
                                                   QHPNonQHPTypeId=i[35], \
                                                   SecondTierUtilization=i[36], \
                                                   ServiceAreaId=i[37], \
                                                   sourcename=i[38], \
                                                   SpecialtyDrugMaximumCoinsurance=i[39], \
                                                   StandardComponentId=i[40], \
                                                   StateCode=i[41], \
                                                   WellnessProgramOffered=i[42]))  #take each row item and apply column name like key to the value
       
       df = spark.createDataFrame(value_row_rdd)#make final dataframe from rdd above

       print("Here is a small sample of Plan Attributes Data")
       df.show(2)#show the dataframe for user to see that it was made cleanly


       df.printSchema()#show shchema of the dataframe
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'PlanAttributes') \
           .option('uri', "mongodb://127.0.0.1/HealthInsuranceMarketplace.dbs") \
           .save()#write the data frame onto MongoDB
    
    kafka_prod_planAttributes()
    spark_kafka_planAttribues()

#---------------------------------------------------------------------------
#NETWORKING PULL -----------------------------------------------------------
#---------------------------------------------------------------------------
def networkingPull():
    def kafka_prod_networkingPull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/Network.csv")
       data_list = [data for data in response.text.splitlines()[1:]]
       
       for data in data_list:
           producer.send('Network', data.encode('utf-8'))

       producer.flush()

    def spark_kafka_networkingPull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'Network').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("Network").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from Network")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split(","))
       value_row_rdd = value_rdd.map(lambda i: Row(BusinessYear=int(i[0]), \
                                                   StateCode=i[1], \
                                                   IssuerId=int(i[2]), \
                                                   SourceName=i[3], \
                                                   VersionNum=int(i[4]), \
                                                   ImportDate=i[5], \
                                                   IssuerId2=int(i[6]), \
                                                   StateCode2=i[7], \
                                                   NetworkName=i[8], \
                                                   NetworkId=i[9], \
                                                   NetworkURL=i[10], \
                                                   RowNumber=i[11], \
                                                   MarketCoverage=i[12], \
                                                   DentalOnlyPlan=i[13]))

       df = spark.createDataFrame(value_row_rdd)
       print("Here is a small sample of Network Data")
       df.show(2)
       df.printSchema()
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'Network') \
           .option('uri', "mongodb://127.0.0.1/HealthInsuranceMarketplace.dbs") \
           .save()

    kafka_prod_networkingPull()
    spark_kafka_networkingPull()

#---------------------------------------------------------------------------
#SERVICE AREA PULL ---------------------------------------------------------
#---------------------------------------------------------------------------
def serviceAreaPull():
    def kafka_prod_serviceAreaPull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/ServiceArea.csv")
       data_list = [data for data in response.text.splitlines()[1:]]
       #print(data_list)
       for data in data_list:
           #print(data)
           producer.send('ServiceArea', data.encode('utf-8'))
       producer.flush()

    def spark_kafka_serviceAreaPull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'ServiceArea').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("ServiceArea").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from ServiceArea")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split(","))
       value_row_rdd = value_rdd.map(lambda i: Row(BusinessYear=int(i[0]), \
                                                   StateCode=i[1], \
                                                   IssuerId=int(i[2]), \
                                                   SourceName=i[3], \
                                                   VersionNum=i[4], \
                                                   ImportDate=i[5], \
                                                   IssuerId2=i[6], \
                                                   StateCode2=i[7], \
                                                   ServiceAreaId=i[8], \
                                                   ServiceAreaName=i[9], \
                                                   CoverEntireState=i[10], \
                                                   County=i[11], \
                                                   PartialCounty=i[12], \
                                                   ZipCodes=i[13], \
                                                   PartialCountyJustification=i[14], \
                                                   RowNumber=i[15], \
                                                   MarketCoverage=i[16], \
                                                   DentalOnlyPlan=i[17]))


       df = spark.createDataFrame(value_row_rdd)
       print("Here is a small sample of Service Area Data")
       df.show(2)
       df.printSchema()
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'ServiceArea') \
           .option('uri', "mongodb://127.0.0.1/HealthInsuranceMarketplace.dbs") \
           .save()
    
    kafka_prod_serviceAreaPull()
    spark_kafka_serviceAreaPull()

#---------------------------------------------------------------------------
#INSURANCE PULL ------------------------------------------------------------
#---------------------------------------------------------------------------
def insurancePull():
    def kafka_prod_insurancePull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/insurance.txt")
       data_list = [data for data in response.text.splitlines()[1:]]
       #print(data_list)
       for data in data_list:
           #print(data)
           producer.send('insurance', data.encode('utf-8'))
       producer.flush()

    def spark_kafka_insurancePull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'insurance').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("insurance").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from insurance")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
       value_row_rdd = value_rdd.map(lambda i: Row(age=int(i[0]), \
                                                   sex=i[1], \
                                                   bmi=float(i[2]), \
                                                   children=int(i[3]), \
                                                   smoker=i[4], \
                                                   region=i[5], \
                                                   charges=float(i[6])))


       df = spark.createDataFrame(value_row_rdd)
       print("Here is a small sample of Insurance Data")
       df.show(2)
       df.printSchema()
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'Insurance') \
           .option('uri', "mongodb://127.0.0.1/HealthInsuranceMarketplace.dbs") \
           .save()
    
    kafka_prod_insurancePull()
    spark_kafka_insurancePull()

#---------------------------------------------------------------------------
#BENEFITS COST SHARING Pt 1 ------------------------------------------------
#---------------------------------------------------------------------------
def benefitsCostSharing_partOnePull():
    def kafka_prod_benefitsCostSharing_partOnePull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partOne.txt")
       data_list = [data for data in response.text.splitlines()[1:]]
       #print(data_list)
       for data in data_list:
           #print(data)
           producer.send('BenefitsCostSharing_partOne', data.encode('utf-8'))
       producer.flush()

    def spark_kafka_benefitsCostSharing_partOnePull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'BenefitsCostSharing_partOne').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("BenefitsCostSharing_partOne").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from BenefitsCostSharing_partOne")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
       value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                                   BusinessYear=i[1], \
                                                   EHBVarReason=i[2], \
                                                   IsCovered=i[3], \
                                                   IssuerId=i[4], \
                                                   LimitQty=i[5], \
                                                   LimitUnit=i[6], \
                                                   MinimumStay=i[7], \
                                                   PlanId=i[8], \
                                                   SourceName=i[9], \
                                                   StateCode=i[10]))
       df = spark.createDataFrame(value_row_rdd)
       print("--- Importing Part One ---")
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'BenefitsCostSharing') \
           .option('uri', "mongodb://127.0.0.1/test_db.dbs") \
           .save()

    kafka_prod_benefitsCostSharing_partOnePull()
    spark_kafka_benefitsCostSharing_partOnePull()

#---------------------------------------------------------------------------
#BENEFITS COST SHARING Pt 2 ------------------------------------------------
#---------------------------------------------------------------------------
def benefitsCostSharing_partTwoPull():
    def kafka_prod_benefitsCostSharing_partTwoPull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partTwo.txt")
       data_list = [data for data in response.text.splitlines()[1:]]
       #print(data_list)
       for data in data_list:
           #print(data)
           producer.send('BenefitsCostSharing_partTwo', data.encode('utf-8'))
       producer.flush()

    def spark_kafka_benefitsCostSharing_partTwoPull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'BenefitsCostSharing_partTwo').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("BenefitsCostSharing_partTwo").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from BenefitsCostSharing_partTwo")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
       value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                                   BusinessYear=i[1], \
                                                   EHBVarReason=i[2], \
                                                   IsCovered=i[3], \
                                                   IssuerId=i[4], \
                                                   LimitQty=i[5], \
                                                   LimitUnit=i[6], \
                                                   MinimumStay=i[7], \
                                                   PlanId=i[8], \
                                                   SourceName=i[9], \
                                                   StateCode=i[10]))
       df = spark.createDataFrame(value_row_rdd)
       print("--- Importing Part Two ---")
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'BenefitsCostSharing') \
           .option('uri', "mongodb://127.0.0.1/test_db.dbs") \
           .save()

    kafka_prod_benefitsCostSharing_partTwoPull()
    spark_kafka_benefitsCostSharing_partTwoPull()

#---------------------------------------------------------------------------
#BENEFITS COST SHARING Pt 3 ------------------------------------------------
#---------------------------------------------------------------------------
def benefitsCostSharing_partThreePull():
    def kafka_prod_benefitsCostSharing_partThreePull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partThree.txt")
       data_list = [data for data in response.text.splitlines()[1:]]
       #print(data_list)
       for data in data_list:
           #print(data)
           producer.send('BenefitsCostSharing_partThree', data.encode('utf-8'))
       producer.flush()

    def spark_kafka_benefitsCostSharing_partThreePull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'BenefitsCostSharing_partThree').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("BenefitsCostSharing_partThree").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from BenefitsCostSharing_partThree")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
       value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                                   BusinessYear=i[1], \
                                                   EHBVarReason=i[2], \
                                                   IsCovered=i[3], \
                                                   IssuerId=i[4], \
                                                   LimitQty=i[5], \
                                                   LimitUnit=i[6], \
                                                   MinimumStay=i[7], \
                                                   PlanId=i[8], \
                                                   SourceName=i[9], \
                                                   StateCode=i[10]))
       df = spark.createDataFrame(value_row_rdd)
       print("--- Importing Part Three ---")
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'BenefitsCostSharing') \
           .option('uri', "mongodb://127.0.0.1/test_db.dbs") \
           .save()

    kafka_prod_benefitsCostSharing_partThreePull()
    spark_kafka_benefitsCostSharing_partThreePull()

#---------------------------------------------------------------------------
#BENEFITS COST SHARING Pt 4 ------------------------------------------------
#---------------------------------------------------------------------------
def benefitsCostSharing_partFourPull():
    def kafka_prod_benefitsCostSharing_partFourPull():
       producer = KafkaProducer(bootstrap_servers='localhost:9092')
       response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partFour.txt")
       data_list = [data for data in response.text.splitlines()[1:]]
       #print(data_list)
       for data in data_list:
           #print(data)
           producer.send('BenefitsCostSharing_partFour', data.encode('utf-8'))
       producer.flush()

    def spark_kafka_benefitsCostSharing_partFourPull():
       
       spark = SparkSession.builder.getOrCreate()
       raw_kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", 'BenefitsCostSharing_partFour').option("startingOffsets", "earliest").load()

       kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")
       output_query = kafka_value_df.writeStream.queryName("BenefitsCostSharing_partFour").format("memory").start()

       output_query.awaitTermination(10)
       
       value_df = spark.sql("select * from BenefitsCostSharing_partFour")
       value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
       value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                                   BusinessYear=i[1], \
                                                   EHBVarReason=i[2], \
                                                   IsCovered=i[3], \
                                                   IssuerId=i[4], \
                                                   LimitQty=i[5], \
                                                   LimitUnit=i[6], \
                                                   MinimumStay=i[7], \
                                                   PlanId=i[8], \
                                                   SourceName=i[9], \
                                                   StateCode=i[10]))
       df = spark.createDataFrame(value_row_rdd)
       print("--- Importing Part Four ---")
       print("Here is a small sample of Benefits Cost Sharing Data")
       df.show(2)
       df.printSchema()
       df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .mode('append') \
           .option('database','HealthInsuranceMarketplace') \
           .option('collection', 'BenefitsCostSharing') \
           .option('uri', "mongodb://127.0.0.1/test_db.dbs") \
           .save()

    kafka_prod_benefitsCostSharing_partFourPull()
    spark_kafka_benefitsCostSharing_partFourPull()

def main():
    
    spark = SparkSession.builder.getOrCreate()#to make sure the spark session is ready

    #---------------------------------------------------------------------------------------------------------------------------
    # So this is a pretty simple nested while/if loop selection screen: whatever the user chooses it runs that specific function
    #---------------------------------------------------------------------------------------------------------------------------

    print("Hello and welcome to CDW_SAPP Data and HealthInsuranceMarketplace Data Transfer selection")
    while True:
        print("Would you like to impot CDW_SAPP Data or HealthInsuranceMarketplace Data into MongoDB?")
        print("1) CDW_SAPP")
        print("2) HealthInsuranceMarketplace Data")
        print("3) Quit")

        startSelection = int(input("Selection: "))
        
        if startSelection == 1:
            print("Starting full transfer from Maria DB to MongoDB.")
            mariaTransfer()
            print("MongoDB now has the Transformed MariaDB Data.")
        elif startSelection == 2:
            while True:
                print("Witch part of the HealthInsuranceMarketplace Data do you want to import into MongoDB?")
                print("1) Plan Attributes Table")
                print("2) Networking Table")
                print("3) Service Area Table")
                print("4) Insurance Table")
                print("5) Benefits Cost Sharing Table")
                print("6) Back to earlier menu")
                innerSelection = int(input("Selection: "))

                if innerSelection == 1:
                    print("Getting Plan Attributes Table into MongoDB")
                    planAttributesPull()
                    print("--- Plan Attributes now in MongoDB ---")
                elif innerSelection == 2:
                    print("Getting Networking Table into MongoDB")
                    networkingPull()
                    print("--- Networking Table in MonogDB ---")
                elif innerSelection == 3:
                    print("Getting Service Area Table into MongoDB")
                    serviceAreaPull()
                    print("--- Service Area Table in MongoDB ---")
                elif innerSelection == 4:
                    print("Getting Insurance Table into MongoDB")
                    insurancePull()
                    print("--- Insurance Table in MongoDB ---")
                elif innerSelection == 5:
                    print("===Benefits Cost Sharing table is large so it must be done in for parts, please wait for input when needed===")
                    for i in range(4):
                        if i == 0:
                            print("---Benefits Cost Sharing Table Part One---")
                            benefitsCostSharing_partOnePull()
                            print("---Benefits Cost Sharing Table Part One in MongoDB---")
                            time.sleep(25)
                        elif i == 1:
                            print("---Benefits Cost Sharing Table Part Two---")
                            benefitsCostSharing_partTwoPull()
                            print("---Benefits Cost Sharing Table Part Two in MongoDB---")
                            time.sleep(25)
                        elif i == 2:
                            print("---Benefits Cost Sharing Table Part Three---")
                            benefitsCostSharing_partThreePull()
                            print("---Benefits Cost Sharing Table Part Three in MongoDB---")
                            time.sleep(25)
                        elif i == 3:
                            print("---Benefits Cost Sharing Table Part Four---")
                            benefitsCostSharing_partFourPull()
                            print("---Benefits Cost Sharing Table Part Four in MongoDB---")
                        else:
                            break
                elif innerSelection ==6:
                    print("---Ok backing out---")
                    break
                else:
                    print("That is not a valid option try again.")


        elif startSelection == 3:
            print("Thank you for your time.")
            break
        else:
            print('That is not a valid option try again.')

if __name__ == "__main__":
    main()
