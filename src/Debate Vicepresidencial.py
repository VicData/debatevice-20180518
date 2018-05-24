# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC from watson_developer_cloud.natural_language_understanding_v1 import *
# MAGIC import re
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC class WatsonUnderstandingWrapper:
# MAGIC   
# MAGIC   #Change according to Watson API
# MAGIC   LAST_VERSION='2018-03-16'
# MAGIC   
# MAGIC   def __init__(self, user,passwd):
# MAGIC         self.user = user
# MAGIC         self.password =passwd
# MAGIC         self.nlu=NaturalLanguageUnderstandingV1(version=self.LAST_VERSION,  username=self.user,password=self.password)
# MAGIC   
# MAGIC   def printCategories(self,texto):  
# MAGIC       response=self.nlu.analyze(text=texto,features=Features(categories=CategoriesOptions()),language='es')
# MAGIC       return response['categories'][0] if len(response['categories'])>0 else None
# MAGIC   
# MAGIC   def printSemantics(self,texto):
# MAGIC       response=self.nlu.analyze(text=texto,features=Features(    semantic_roles=SemanticRolesOptions(entities=True)),language='es')
# MAGIC       return response['semantic_roles']if len(response['semantic_roles'])>0 else None
# MAGIC     
# MAGIC   def printSentiment(self,texto,target):
# MAGIC       response=self.nlu.analyze(text=texto, features=Features(sentiment=SentimentOptions(targets=['angelamrobledo'])),language='es')
# MAGIC       return response['sentiment']if len(response['sentiment'])>0 else None
# MAGIC     
# MAGIC   def printEntities(self,texto):
# MAGIC       response=self.nlu.analyze(text=texto, features=Features(entities=EntitiesOptions(sentiment=True,emotion=True)),language='es')
# MAGIC       return response['entities']if len(response['entities'])>0 else None  
# MAGIC   
# MAGIC   schemanaly = StructType([
# MAGIC                 StructField('score'     , FloatType()   , False),
# MAGIC                 StructField('label'  , StringType()    , False)
# MAGIC             ])
# MAGIC   
# MAGIC   
# MAGIC   schemasem = ArrayType(StructType([
# MAGIC                 StructField('subject' , StructType([ StructField('text'  , StringType()    , True)])   , True),
# MAGIC                 StructField('sentence', StringType()    , True),
# MAGIC                 StructField('object'  , StructType([ StructField('text'  , StringType()    , True)])   , True),    
# MAGIC                 StructField('action', StructType([StructField('verb', StructType([StructField('text', StringType(), True),StructField('tense', StringType(), True)]),True),StructField('text', StringType(), True),StructField('normalized', StringType(), True)]),True)  
# MAGIC ]))
# MAGIC   
# MAGIC   #[{'type': 'TwitterHandle', 'text': '@angelamrobledo', 'relevance': 0.978347, 'count': 1, 'sentiment': {'label': 'negative', 'score': -0.91139}}]
# MAGIC   schemaent = ArrayType(StructType([
# MAGIC       StructField('type',StringType(),True),
# MAGIC       StructField('text',StringType(),True),
# MAGIC       StructField('relevance',StringType(),True),
# MAGIC       StructField('count',LongType(),True),
# MAGIC       StructField('sentiment',StructType([StructField('label',StringType(),True),StructField('score',FloatType(),True)]),True)
# MAGIC   ])
# MAGIC   )
# MAGIC   

# COMMAND ----------

describe table debatevice

# COMMAND ----------

select user  `@ twitter`,count(*) tweets from(
select explode(entities.user_mentions.screen_name) user,case when retweeted_status is null then 'No' else 'Si' end Es_RT	from debatevice where  size(entities.user_mentions)>0

)
where user in ('angelamrobledo','mluciaramirez','ClaudiaLopez','PinzonBueno','ClaraLopezObre')
group by user
order by 2 desc

# COMMAND ----------

SELECT case t.text when '@ClaudiaLopez' then 'Claudia L.' when '@mluciaramirez' then 'Marta L.' when '@angelamrobledo' then 'Ángela R.'  when '@PinzonBueno' then 'Pinzón' when '@ClaraLopezObre' then 'Clara L.'else 'otro' end entity,window(CAST (from_unixtime(unix_timestamp(created_at,'EEE MMM d HH:mm:ss Z yyyy'))AS TIMESTAMP) ,"5 minutes").start fecha,sum(t.sentiment.score*t.relevance) sum_score
--SELECT CAST(T.SENTIMENT.RELEVANC AS LONG)
FROM(
select ID,EXPLODE(SENTIMENT) T from vicentiment
where array_contains(sentiment.type,'Person') 
OR ARRAY_CONTAINS(sentiment.type,'TwitterHandle')
  ) Q1 JOIN DEBATEVICE D ON Q1.ID=D.ID
  WHERE    T.relevance>=0.6 AND from_unixtime(unix_timestamp(created_at,'EEE MMM d HH:mm:ss Z yyyy')-18000)>='2018-05-18 22:00:00'
  AND from_unixtime(unix_timestamp(created_at,'EEE MMM d HH:mm:ss Z yyyy')-18000)<='2018-05-19 00:35:00'
  AND  T.TYPE ='TwitterHandle' AND SUBSTRING(T.TEXT,2,LENGTH(T.TEXT)-1) in ('angelamrobledo','mluciaramirez','ClaudiaLopez','PinzonBueno','ClaraLopezObre')
  --group by from_unixtime(unix_timestamp(created_at,'EEE MMM d HH:mm:00 Z yyyy')-18000)
  group by t.text,window(CAST (from_unixtime(unix_timestamp(created_at,'EEE MMM d HH:mm:ss Z yyyy'))AS TIMESTAMP) ,"5 minutes")
  ORDER BY 2

# COMMAND ----------


