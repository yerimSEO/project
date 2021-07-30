# Databricks notebook source
# MAGIC %md
# MAGIC # Yelp Project
# MAGIC 
# MAGIC **Group 1**:
# MAGIC - SEO Yerim
# MAGIC - TIRUMALE LAKSHMANA RAO Kiran
# MAGIC - ZHENG Bin

# COMMAND ----------

# MAGIC %md
# MAGIC Using the 6 datasets provided by Yelp, we as a team worked on building a prediction model to forecast which businesses will start doing delivery/takeout after the first lockdown in the North American region.
# MAGIC 
# MAGIC In order to help businesses sustain during this Covid-19 pandemic, it's important to undestand which key factors contributed to businesses sustaining themselves post the first wave of covid 19.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Project Pipeline:
# MAGIC 
# MAGIC - **Reading in the Data** - Understanding all 6 datasets provided by Covid
# MAGIC - **Dropping Unnecessary Columns** - Dropping features that wouldn't contribute to the model's performance, and also dropping complex features.
# MAGIC - **Joining all 6 datasets** - forming a basetable - Joining all datasets, after transforming them, in order to obtain 1 observation per business
# MAGIC - **Preprocessing the basetable** - Cleaning the basetable, handling missing values, pre-processing categorical features and text features for modeling
# MAGIC - **Modeling** - Modeling using 4 different algorithms using hyper parameter tuning, also finding the important features

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Reading in the Data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading in the Yelp Data
# MAGIC https://www.yelp.com/dataset/documentation/main
# MAGIC 
# MAGIC Undertanding the all the datasets provided by covid, and doing performing an initial analysis

# COMMAND ----------

# Listing all the datasets available to us
import os
print(os.listdir("./data4"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Dataset

# COMMAND ----------

# Business Data - Contains business data including location data, attributes, and categories.
business = spark.read.json("/FileStore/tables/Yelp_Data_Group/parsed_business_sample.json")

# Viewing the data
print(business.columns)

# Re-checking the shape of the review user dataset loaded from json
print((business.count(), len(business.columns)))

# COMMAND ----------

#replace . in the column name into _  and remove spaces in columns name

for name in business.schema.names:
  business = business.withColumnRenamed(name, name.replace('.', '_'))

# COMMAND ----------

# Checking the schema as well as confirming the renaming of all columns
business.printSchema()

# COMMAND ----------

# Checking the unique values of business_id
business.select('business_id').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkin Dataset

# COMMAND ----------

# Checkin Data - Checkins on a business.
checkin = spark.read.json("/FileStore/tables/Yelp_Data_Group/parsed_checkin_sample.json")

# Viewing the data
print(checkin.columns)

# Re-checking the shape of the review user dataset loaded from json
print((checkin.count(), len(checkin.columns)))

# COMMAND ----------

checkin.show(10, False)

# COMMAND ----------

# Checking the unique values of business_id
checkin.select('business_id').distinct().count()

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

checkin=checkin.withColumn('date', regexp_replace('date', '&#39', ''))

checkin=checkin.select("business_id",to_timestamp(checkin.date, 'yyyy-MM-dd HH:mm:ss').alias('date'))

# COMMAND ----------

checkin.show(10, False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Dataset

# COMMAND ----------

# Review Data - Contains full review text data including the user_id that wrote the review and the business_id the review is written for.
review = spark.read.json("/FileStore/tables/Yelp_Data_Group/parsed_review_sample.json")

# Viewing the data
print(review.columns)

# Re-checking the shape of the review user dataset loaded from json
print((review.count(), len(review.columns)))

# COMMAND ----------

review.printSchema()

# COMMAND ----------

# Checking the unique values of business_id
review.select('business_id').distinct().count()

# COMMAND ----------

# Checking the unique values of user_id
review.select('user_id').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tip Dataset

# COMMAND ----------

# Tip Data - Tips written by a user on a business. Tips are shorter than reviews and tend to convey quick suggestions.
tip = spark.read.json("/FileStore/tables/Yelp_Data_Group/parsed_tip_sample.json")

# Viewing the data
print(tip.columns)

# Re-checking the shape of the review user dataset loaded from json
print((tip.count(), len(tip.columns)))

# COMMAND ----------

tip.printSchema()

# COMMAND ----------

# Checking the unique values of business_id
tip.select('business_id').distinct().count()

# COMMAND ----------

# Checking the unique values of business_id
tip.select('user_id').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Dataset

# COMMAND ----------

# User Data - User data including the user's friend mapping and all the metadata associated with the user.
user = spark.read.json("/FileStore/tables/Yelp_Data_Group/parsed_user_sample.json")

# Viewing the data
print(user.columns)

# Re-checking the shape of the review user dataset loaded from json
print((user.count(), len(user.columns)))

# COMMAND ----------

user.printSchema()

# COMMAND ----------

# Checking the unique values of user_id
user.select('user_id').distinct().count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Covid Dataset

# COMMAND ----------

# Reading in the Covid Features Data
covid = spark.read.json("/FileStore/tables/Yelp_Data_Group/parsed_covid_sample.json")

# Viewing the data
print(covid.columns)

# Re-checking the shape of the review user dataset loaded from json
print((covid.count(), len(covid.columns)))

# COMMAND ----------

covid.printSchema()

# COMMAND ----------

# Checking the unique values of business_id
covid.select('business_id').distinct().count()

# COMMAND ----------

# Drop duplicates
covid = covid.dropDuplicates(["business_id"])

# COMMAND ----------

# Check distinct values of the column
covid.select('delivery or takeout').distinct().show(truncate = False)

### Get count of both null and missing values for the column 'delivery or takeout'
from pyspark.sql.functions import isnan, when, count, col
covid.select([count(when(isnan('delivery or takeout'),True))]).show()

# COMMAND ----------

# Checking the count of both false and trues in the column 'delivery or takeout'
covid.select('delivery or takeout').groupBy('delivery or takeout').count().show()

# COMMAND ----------

# Checking for duplicate rows in the covid dataset
import pyspark.sql.functions as f
covid.groupBy(covid.columns)\
    .count()\
    .where(f.col('count') > 1)\
    .select(f.sum('count'))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dropping Unnecessary Columns

# COMMAND ----------

# MAGIC %md
# MAGIC Before merging all above datasets into a single basetable, let us drop columns that we will not require for our machine learning algorithms, or in other words, columns that we feel will not contribute in improving the prediction capabilities of the algorithms

# COMMAND ----------

# List of columns of each dataset to be dropped

business_drop = ['address',  'name',  'attributes_Ambience', 'attributes_BYOBCorkage', 'attributes_BestNights', 'attributes_BusinessParking', 'attributes_DietaryRestrictions', 'attributes_GoodForMeal', 'attributes_HairSpecializesIn', 'attributes_Music', 'attributes_NoiseLevel', 'attributes_RestaurantsAttire', 'attributes_Smoking', 'attributes_WiFi', 'hours_Monday', 'hours_Tuesday', 'hours_Wednesday', 'hours_Thursday', 'hours_Friday', 'hours_Saturday', 'hours_Sunday', 'attributes_RestaurantsPriceRange2', 'latitude', 'longitude']
# 'city', 'categories', 'postal_code', 'state', 'attributes_AgesAllowed', 'attributes_Alcohol', 'latitude', 'longitude'

review_drop = ['date', 'review_id', 'text']

tip_drop = ['text', 'date']

user_drop = ['elite', 'friends', 'name']

covid_drop = ['highlights', 'Temporary Closed Until']
# 'Virtual Services Offered', 'Covid Banner'

# COMMAND ----------

# Dropping the columns from the dataframes
business = business.drop(*business_drop)
review = review.drop(*review_drop)
tip = tip.drop(*tip_drop)
user = user.drop(*user_drop)
covid = covid.drop(*covid_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining the tables to form the `basetable`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the `Review` and `User` Dataframes into `review_user`

# COMMAND ----------

# First renaming the `cool` and `funny`columns from the Review dataframe
review = review.withColumnRenamed("cool", "review_cool")
review = review.withColumnRenamed("funny", "review_funny")
review = review.withColumnRenamed("useful", "review_useful")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Performing an inner joining between the 2 dataframes

# COMMAND ----------

# performing an inner join of the review and user dataframes
review_user = review.join(user,review.user_id ==  user.user_id,"inner").drop(user.user_id)

# COMMAND ----------

# checking the shape of the joined dataframe
print((review_user.count(), len(review_user.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: grouping by and aggregating the values by business_id

# COMMAND ----------

# Now we can drop the user_id column
review_user = review_user.drop("user_id")

# COMMAND ----------

review_user = review_user.groupBy("business_id").mean()

# COMMAND ----------

# checking the shape of the joined dataframe
print((review_user.count(), len(review_user.columns)))

# COMMAND ----------

# Checking the unique values of business_id
review_user.select('business_id').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the `review_user` dataframe with `business` into `yelp_datamart`

# COMMAND ----------

# performing an inner join of the review_user and business_flatten dataframes
yelp_datamart = business.join(review_user, business.business_id ==  review_user.business_id,"inner").drop(review_user.business_id)

# COMMAND ----------

# checking the shape of the joined dataframe
print((yelp_datamart.count(), len(yelp_datamart.columns)))

# COMMAND ----------

# Checking the unique values of business_id
yelp_datamart.select('business_id').distinct().count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the `tip` dataframe to the `yelp_datamart`

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Processing the `tip` data, groupby business_id, and aggregating by sum

# COMMAND ----------

tip = tip.select("business_id", "compliment_count").groupBy("business_id").sum()

# COMMAND ----------

tip.select("business_id").distinct().count()

# COMMAND ----------

# Renaming the column back to compliment_count
tip = tip.withColumnRenamed("sum(compliment_count)", "compliment_count")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Performing a left join
# MAGIC 
# MAGIC Given that we have only 12578 business_id with compliment counts, we shall perform a left join

# COMMAND ----------

# performing an inner join of the review_user and business_flatten dataframes
yelp_datamart = yelp_datamart.join(tip, yelp_datamart.business_id ==  tip.business_id,"left").drop(tip.business_id)

# COMMAND ----------

# checking the shape of the joined dataframe
print((yelp_datamart.count(), len(yelp_datamart.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the `checkin` dataframe to the `yelp_datamart`

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Processing the `checkin` data, groupby `business_id`, and aggregating by `count`

# COMMAND ----------

checkin = checkin.groupBy("business_id").count()

# COMMAND ----------

# Renaming the column back to compliment_count
checkin = checkin.withColumnRenamed("count", "checkin_count")

# COMMAND ----------

# Checking number of unique business ids
checkin.select("business_id").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Performing a left join
# MAGIC 
# MAGIC Given that we have only 16244 business_id with checkin counts, we shall perform a left join

# COMMAND ----------

# performing an inner join of the review_user and business_flatten dataframes
yelp_datamart = yelp_datamart.join(checkin, yelp_datamart.business_id ==  checkin.business_id,"left").drop(checkin.business_id)

# COMMAND ----------

# checking the shape of the joined dataframe
print((yelp_datamart.count(), len(yelp_datamart.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the `covid` dataframe to the `yelp_datamart`

# COMMAND ----------

# performing an inner join of the yelp_datamart and covid dataframes
yelp_datamart = yelp_datamart.join(covid, yelp_datamart.business_id ==  covid.business_id,"inner").drop(covid.business_id)

# COMMAND ----------

# checking the shape of the joined dataframe
print((yelp_datamart.count(), len(yelp_datamart.columns)))

# COMMAND ----------



# COMMAND ----------

yelp_datamart.select('delivery or takeout').groupBy('delivery or takeout').count().show()

# COMMAND ----------

basetable = yelp_datamart

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-Processing the Basetable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking for missing values

# COMMAND ----------

# checking the shape of the basetable
print((basetable.count(), len(basetable.columns)))

# COMMAND ----------

# Checking for missing values in the Basetable
from pyspark.sql.functions import when, count, col
basetable.select([count(when(col(c).isNull(), c)).alias(c) for c in 
           basetable.columns]).toPandas().T

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping columns with over 80% missing values

# COMMAND ----------

# MAGIC %md
# MAGIC 80% missing values of a basetable of 19018 rows is 15214 missing value per column. 
# MAGIC 
# MAGIC Hence, if any column has more than 15200 missing values, we will are dropping that column!

# COMMAND ----------

# columns with over 15200 missing values:
missing_cols = ['attributes_AcceptsInsurance', 'attributes_AgesAllowed', 'attributes_BYOB', 'attributes_BusinessAcceptsBitcoin', 'attributes_Caters', 'attributes_CoatCheck', 'attributes_Corkage', 'attributes_DogsAllowed', 'attributes_DriveThru', 'attributes_GoodForDancing', 'attributes_HappyHour', 'attributes_Open24Hours', 'attributes_RestaurantsCounterService', 'attributes_RestaurantsTableService', 'attributes_WheelchairAccessible']

# COMMAND ----------

# Dropping all above columns that have over 80% missing values
basetable = basetable.drop(*missing_cols)

# COMMAND ----------

# Replacing all missing values with -1
basetable = basetable.na.fill(-1)

# COMMAND ----------

# checking the shape of the basetable
print((basetable.count(), len(basetable.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Converting boolean columns to integer values

# COMMAND ----------

# MAGIC %md
# MAGIC We have a list of columns that should only contain boolean values (True (1) and False(0)). Let us pre-process first these columns, by converting all Trues to 1 and all Falses to 0, and any other values to -1 indicating a missing value

# COMMAND ----------

# List of boolean columns
cols=['attributes_BikeParking','attributes_BusinessAcceptsCreditCards','attributes_ByAppointmentOnly','attributes_GoodForKids','attributes_HasTV','attributes_OutdoorSeating','attributes_RestaurantsDelivery','attributes_RestaurantsGoodForGroups','attributes_RestaurantsReservations','attributes_RestaurantsTakeOut', 'Call To Action enabled','Grubhub enabled','Request a Quote Enabled','delivery or takeout']

# COMMAND ----------

for col_name in cols:
  basetable = basetable.withColumn(col_name, when(basetable[col_name] == "True", 1)
                                 .when(basetable[col_name] == "TRUE", 1)
                                 .when(basetable[col_name] == "False", 0)
                                 .when(basetable[col_name] == "FALSE", 0)
                                 .otherwise(-1))

# COMMAND ----------

# Confirming the above change, by checking the delivery or takeout column
basetable.select('delivery or takeout').groupBy('delivery or takeout').count().show()

# COMMAND ----------

# Checking for missing values in the cleaned and processed Basetable
from pyspark.sql.functions import when, count, col
basetable.select([count(when(col(c).isNull(), c)).alias(c) for c in 
           basetable.columns]).toPandas().T

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handling the `Virtual Services Offered` column

# COMMAND ----------

# Checking the unique values of the virtual services offered column
basetable.select("Virtual Services Offered").distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Given that we have only False, and the rest of the values indicate a True, meaning virtual services were offered.
# MAGIC Let's replace all values other than False with True = 1

# COMMAND ----------

basetable = basetable.withColumn("Virtual Services Offered", when(basetable["Virtual Services Offered"] == "FALSE", 0)
                                 .when(basetable["Virtual Services Offered"] == "False", 0)
                                 .otherwise(1))

# COMMAND ----------

# Now confirming the above changes made to the virtual services offered column
basetable.select("Virtual Services Offered").distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting the Target column to Double format

# COMMAND ----------

# MAGIC %md
# MAGIC Converting the target column `delivery or takeout` to double format and renaming it to `label`

# COMMAND ----------

basetable = basetable.withColumn("delivery or takeout", basetable["delivery or takeout"].cast("double"))\
                     .withColumnRenamed("delivery or takeout","label")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 1: The following categorical columns need to be processed using `StringIndexer`**
# MAGIC 
# MAGIC - `attributes_Alcohol`
# MAGIC - `city`
# MAGIC - `postal_code`
# MAGIC - `state`

# COMMAND ----------

# Checking for missing values for above 5 columns

cat_cols = ['attributes_Alcohol', 'city', 'postal_code', 'state']

from pyspark.sql.functions import when, count, col
basetable.select([count(when(col(c).isNull(), c)).alias(c) for c in 
           cat_cols]).toPandas().T

# COMMAND ----------

# Replacing the missing vavlues for the above column with -1
basetable = basetable.fillna( {'attributes_Alcohol':-1 } )

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline


# attributes_Alcohol
alcoholIndxr = StringIndexer().setInputCol("attributes_Alcohol").setOutputCol("Alcohol_Ind")

# city
cityIndxr = StringIndexer().setInputCol("city").setOutputCol("city_Ind")

# postal_code
postal_codeIndxr = StringIndexer().setInputCol("postal_code").setOutputCol("postal_code_Ind")

# state
stateIndxr = StringIndexer().setInputCol("state").setOutputCol("state_Ind")


pipe_catv = Pipeline(stages=[alcoholIndxr, cityIndxr, postal_codeIndxr, stateIndxr])
basetable = pipe_catv.fit(basetable).transform(basetable)
basetable = basetable.drop("attributes_Alcohol", "city", "postal_code", "state" )

# COMMAND ----------

# Seeing the above changes
basetable.select('Alcohol_Ind', 'city_Ind', 'postal_code_Ind', 'state_Ind').show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2: Let us now onehot encode all boolean columns we cleaned earlier.**
# MAGIC 
# MAGIC Given that we have a lot of columns in this boolean method, let's apply the onehot encoding method using a loop

# COMMAND ----------

bool_cols=['attributes_BikeParking','attributes_BusinessAcceptsCreditCards','attributes_ByAppointmentOnly','attributes_GoodForKids','attributes_HasTV','attributes_OutdoorSeating','attributes_RestaurantsDelivery','attributes_RestaurantsGoodForGroups','attributes_RestaurantsReservations','attributes_RestaurantsTakeOut','is_open', 'Call To Action enabled','Grubhub enabled','Request a Quote Enabled','Virtual Services Offered']

# COMMAND ----------

# first applying the stringindexer, then onehotencoding in a loop for all columns from bool_cols
for my_col in bool_cols:
    my_col_ind = my_col + "Ind"
    my_col_dum = my_col + "_dum"
    model = StringIndexer().setInputCol(my_col).setOutputCol(my_col_ind)
    ohe = OneHotEncoder(inputCols=[my_col_ind],outputCols=[my_col_dum])
    pipe = Pipeline(stages=[model, ohe])
    basetable = pipe.fit(basetable).transform(basetable)
    basetable = basetable.drop(my_col, my_col_ind)

# COMMAND ----------

# Checking the above changes on the first 3 columns updated
basetable.select('attributes_BikeParking_dum','attributes_BusinessAcceptsCreditCards_dum','attributes_ByAppointmentOnly_dum').show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the text column

# COMMAND ----------

# MAGIC %md
# MAGIC We have only 2 text columns 
# MAGIC - `categories`
# MAGIC - `Covid Banner`

# COMMAND ----------

# Checking for missing values of above 2 columns
text_cols = ['categories', 'Covid Banner']

from pyspark.sql.functions import when, count, col
basetable.select([count(when(col(c).isNull(), c)).alias(c) for c in 
           text_cols]).toPandas().T

# COMMAND ----------

# Replacing the missing vavlues for the above 2 columns with -1
basetable = basetable.fillna( { 'categories':-1 } )

# COMMAND ----------

from pyspark.ml.feature import HashingTF, Tokenizer, CountVectorizer
# Tokenizing
tok_categories = Tokenizer(inputCol="categories", outputCol="categories_words")
tok_covid_banner = Tokenizer(inputCol="Covid Banner", outputCol="covid_banner_words")

# Note We do not need to remove stop words in our case

# # Using Hashing Method
# hashingTF_categories = HashingTF(inputCol="categories_words", outputCol="categories_txt")
# hashingTF_covid_banner = HashingTF(inputCol="covid_banner_words", outputCol="covid_banner_txt")

# Using CountVectorizer method
cv_categories = CountVectorizer(inputCol="categories_words", outputCol="categories_txt")
cv_covid_banner = CountVectorizer(inputCol="covid_banner_words", outputCol="covid_banner_txt")

# Defining the pipeline
pipeline = Pipeline(stages=[tok_categories, cv_categories, tok_covid_banner, cv_covid_banner])

#Fit the pipeline to training documents.
basetable = pipeline.fit(basetable).transform(basetable)

# Dropping the columns
basetable = basetable.drop("categories", "categories_words", "Covid Banner", "covid_banner_words")

# COMMAND ----------

# confirming the above changes
basetable.select("categories_txt", "covid_banner_txt").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a backup of the final basetable

# COMMAND ----------

# Renaming few columns from the basetable to export to parquet file as backup
basetable = basetable.withColumnRenamed('avg(review_cool)','avg_review_cool')
basetable = basetable.withColumnRenamed('avg(review_funny)','avg_review_funny')
basetable = basetable.withColumnRenamed('avg(stars)','avg_stars')
basetable = basetable.withColumnRenamed('avg(review_useful)','avg_review_useful')
basetable = basetable.withColumnRenamed('avg(average_stars)','avg_average_stars')
basetable = basetable.withColumnRenamed('avg(compliment_cool)','avg_compliment_cool')
basetable = basetable.withColumnRenamed('avg(compliment_cute)','avg_compliment_cute')
basetable = basetable.withColumnRenamed('avg(compliment_funny)','avg_compliment_funny')
basetable = basetable.withColumnRenamed('avg(compliment_hot)','avg_compliment_hot')
basetable = basetable.withColumnRenamed('avg(compliment_list)','avg_compliment_list')
basetable = basetable.withColumnRenamed('avg(compliment_more)','avg_compliment_more')
basetable = basetable.withColumnRenamed('avg(compliment_note)','avg_compliment_note')
basetable = basetable.withColumnRenamed('avg(compliment_photos)','avg_compliment_photos')
basetable = basetable.withColumnRenamed('avg(compliment_plain)','avg_compliment_plain')
basetable = basetable.withColumnRenamed('avg(compliment_profile)','avg_compliment_profile')
basetable = basetable.withColumnRenamed('avg(compliment_writer)','avg_compliment_writer')
basetable = basetable.withColumnRenamed('avg(cool)','avg_cool')
basetable = basetable.withColumnRenamed('avg(fans)','avg_fans')
basetable = basetable.withColumnRenamed('avg(funny)','avg_funny')
basetable = basetable.withColumnRenamed('avg(review_count)','avg_review_count')
basetable = basetable.withColumnRenamed('avg(useful)','avg_useful')

# COMMAND ----------

basetable = basetable.withColumnRenamed('Call To Action enabled_dum','Call_To_Action_enabled_dum')
basetable = basetable.withColumnRenamed('Grubhub enabled_dum','Grubhub_enabled_dum')
basetable = basetable.withColumnRenamed('Request a Quote Enabled_dum','Request_a_Quote_Enabled_dum')
basetable = basetable.withColumnRenamed('Virtual Services Offered_dum','Virtual_Services_Offered_dum')

# COMMAND ----------



# COMMAND ----------

# Writing the yelp_datamart dataframe to csv
basetable.repartition(1).write.format("parquet")\
  .mode('overwrite')\
  .option("header", True)\
  .save("/FileStore/tables/Yelp_Data_Group/basetable_final_withOHE.parquet")

# COMMAND ----------

# Checking the shape of the final basetable
print((basetable.count(), len(basetable.columns)))


# COMMAND ----------

basetable_final = basetable

# COMMAND ----------

# MAGIC %md
# MAGIC # Modeling

# COMMAND ----------

# Reading the backup file
# Reloading and checking
basetable_final = spark.read.format("parquet").load("/FileStore/tables/Yelp_Data_Group/basetable_final_withOHE.parquet")

# COMMAND ----------

# Checking the shape of the loaded basetable
print((basetable_final.count(), len(basetable_final.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deleting highly correlated columns

# COMMAND ----------

# dropping columns that are highly correlated and are "hacking" the AUC
basetable_final = basetable_final.drop('attributes_RestaurantsDelivery_dum', 'attributes_RestaurantsTakeOut_dum', 'Grubhub_enabled_dum')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basetable transformation for modeling

# COMMAND ----------

#Transform the tables in a table of label, features format using the RFormula method
from pyspark.ml.feature import RFormula

basetable_final = RFormula(formula="label ~ . - business_id").fit(basetable_final).transform(basetable_final)

print("basetable_final dataset no. of obs: " + str(basetable_final.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Train and Test set

# COMMAND ----------

#Create a train and test set with a 70% train, 30% test split
train, test = basetable_final.randomSplit([0.7, 0.3],seed=123)

print(train.count())
print(test.count())

# COMMAND ----------

# Selecting only the features and the label columns for modeling
train = train.select('features', 'label')
train.show(3)

# COMMAND ----------

# Selecting only the features and the label columns for modeling
test = test.select('features', 'label')
test.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling using Random Forest

# COMMAND ----------

#Hyperparameter tuning for pipeline logistic regression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier

#Define pipeline
rf = RandomForestClassifier(maxBins=5000)
pipe_rf = Pipeline().setStages([rf])

#Set param grid
rf_params = ParamGridBuilder()\
  .addGrid(rf.numTrees, [150, 300, 500])\
  .build()

# Evaluator: uses the max(AUC) by default for final model
evaluator = BinaryClassificationEvaluator()

#Cross-validation of entire pipeline
cv_rf = CrossValidator()\
  .setEstimator(pipe_rf)\
  .setEstimatorParamMaps(rf_params)\
  .setEvaluator(evaluator)\
  .setNumFolds(10) # 10-fold cross validation

#Run cross-validation. Spark automatically saves the best solution as the main model.
cv_rf_model = cv_rf.fit(train)

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

# Performing the predictionon the test set and subsetting data to just prediction and label columns
preds = cv_rf_model.transform(test).select("prediction", "label")
preds.show(10)

#Get model performance on test set
out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR) # area under precision/recall curve
print(metrics.areaUnderROC) # area under Receiver Operating Characteristic curve

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(preds)
print("Test Error = %g " % (1.0 - accuracy))

# COMMAND ----------

# ROC curve on the train data

display(cv_rf_model, train, "ROC")

# COMMAND ----------

# ROC curve on the test data

display(cv_rf_model, test, "ROC")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculating Feature Importance using Random Forest

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(numTrees=20, maxDepth=30, labelCol="label", seed=42, maxBins=5000)
model = rf.fit(train)
predictions = model.transform(test)

# COMMAND ----------

#Function for extracing features 

def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))

# COMMAND ----------

import pandas as pd
features_list = ExtractFeatureImp(model.featureImportances, predictions, rf.getFeaturesCol())

# COMMAND ----------

features_list.head(10)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling with Logistic Regression

# COMMAND ----------

#Hyperparameter tuning for pipeline logistic regression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression

#Define pipeline
lr = LogisticRegression()
pipe_lr = Pipeline().setStages([lr])

#Set param grid
lr_params = ParamGridBuilder()\
  .addGrid(lr.regParam, [0.1, 0.01])\
  .addGrid(lr.maxIter, [50, 100,150])\
  .build()

# Evaluator: uses the max(AUC) by default for final model
evaluator = BinaryClassificationEvaluator()

#Cross-validation of entire pipeline
cv_lr = CrossValidator()\
  .setEstimator(pipe_lr)\
  .setEstimatorParamMaps(lr_params)\
  .setEvaluator(evaluator)\
  .setNumFolds(10) # 10-fold cross validation

#Run cross-validation. Spark automatically saves the best solution as the main model.
cv_lr_model = cv_lr.fit(train)

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

# Performing the predictionon the test set and subsetting data to just prediction and label columns
preds = cv_lr_model.transform(test).select("prediction", "label")
preds.show(10)

#Get model performance on test set
out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR) # area under precision/recall curve
print(metrics.areaUnderROC) # area under Receiver Operating Characteristic curve

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(preds)
print("Test Error = %g " % (1.0 - accuracy))

# COMMAND ----------

# ROC curve on the train data

display(cv_lr_model, train, "ROC")

# COMMAND ----------

# ROC curve on the test data

display(cv_lr_model, test, "ROC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling with Decision Tree

# COMMAND ----------

#Hyperparameter tuning for pipeline Decision Tree
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier

#Define pipeline
dt = DecisionTreeClassifier(maxBins=5000)
pipe_dt = Pipeline().setStages([dt])

#Set param grid
dt_params = ParamGridBuilder()\
  .addGrid(dt.maxDepth, [10, 20, 30])\
  .build()

# Evaluator: uses the max(AUC) by default for final model
evaluator = BinaryClassificationEvaluator()

#Cross-validation of entire pipeline
cv_dt = CrossValidator()\
  .setEstimator(pipe_dt)\
  .setEstimatorParamMaps(dt_params)\
  .setEvaluator(evaluator)\
  .setNumFolds(10) #10-fold cross validation

#Run cross-validation. Spark automatically saves the best solution as the main model.
cv_dt_model = cv_dt.fit(train)

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

# Performing the predictionon the test set and subsetting data to just prediction and label columns
preds = cv_dt_model.transform(test).select("prediction", "label")
preds.show(10)

#Get model performance on test set
out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR) # area under precision/recall curve
print(metrics.areaUnderROC) # area under Receiver Operating Characteristic curve

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(preds)
print("Test Error = %g " % (1.0 - accuracy))

# COMMAND ----------

# ROC curve on the train data

display(cv_dt_model, train, "ROC")

# COMMAND ----------

# ROC curve on the test data

display(cv_dt_model, test, "ROC")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling with Gradient Boosting

# COMMAND ----------

#Hyperparameter tuning for pipeline Decision Tree
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import GBTClassifier

#Define pipeline
gbt = GBTClassifier(maxBins=5000)
pipe_gbt = Pipeline().setStages([gbt])

#Set param grid
gbt_params = ParamGridBuilder()\
             .build()

# gbt_params = ParamGridBuilder()\
#              .addGrid(gbt.maxDepth, [10, 20])\
#              .addGrid(gbt.maxIter, [50, 100])\
#              .build()


# Evaluator: uses the max(AUC) by default for final model
evaluator = BinaryClassificationEvaluator()

#Cross-validation of entire pipeline
cv_gbt = CrossValidator()\
  .setEstimator(pipe_gbt)\
  .setEstimatorParamMaps(gbt_params)\
  .setEvaluator(evaluator)\
  .setNumFolds(10) #10-fold cross validation



#Run cross-validation. Spark automatically saves the best solution as the main model.
cv_gbt_model = cv_gbt.fit(train)

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

# Performing the predictionon the test set and subsetting data to just prediction and label columns
preds = cv_gbt_model.transform(test).select("prediction", "label")
preds.show(10)

#Get model performance on test set
out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR) # area under precision/recall curve
print(metrics.areaUnderROC) # area under Receiver Operating Characteristic curve

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(preds)
print("Test Error = %g " % (1.0 - accuracy))

# COMMAND ----------

# ROC curve on the train data

display(cv_gbt_model, train, "ROC")

# COMMAND ----------

# ROC curve on the test data

display(cv_gbt_model, test, "ROC")

# COMMAND ----------


