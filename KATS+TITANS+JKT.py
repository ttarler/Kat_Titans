
# coding: utf-8

# In[1]:


import findspark
findspark.init('/usr/hdp/2.6.3.0-235/spark2')

import pyspark
sc = pyspark.SparkContext(appName="KATS2")


# In[2]:



text_file = sc.textFile("hdfs:///user/hdfs/BaseData.txt")

from pyspark.rdd import RDD
isinstance(text_file, RDD)


word_list = text_file.map(lambda x: x.lower().replace('>',' ').replace(',',' ').replace('<',' ').replace('"',' ').split())
word_list.take(5)

test = word_list.flatMap(lambda list: [(element, list[0]) for element in list[1:]] )
test.take(20)


# In[126]:


word_list.take(5)


# In[3]:


test2 = test.reduceByKey(lambda a, b: a + ',' + b)

test2.take(5)

 



# In[11]:


test3 = test2.collectAsMap()
test4 = []
test5 = []
for  tag in test3:
   test4.extend((tag, test3[tag].split(',')))


# In[12]:


test4

