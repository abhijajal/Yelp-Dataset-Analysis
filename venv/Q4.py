#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime, timedelta
from datetime import date
from pyspark.sql import SQLContext
from pyspark.sql import Row

def MF_reduce(values):
    cnt = 0
    sum = 0
    for value in values:
        cnt=cnt+1

    return cnt

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <bussiness> <review> <users>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonMutualFriendsCount")\
        .getOrCreate()
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    # bussiness [1], review [2]

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    mapped = lines.map(lambda line: line.split("::"))
    # Filtering business which are in NY
    business = mapped.filter(lambda line: " NY " in line[1])
    business = business.map(lambda line:(line[0],(line[1],line[2])))
    review = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])
    review = review.map(lambda line: line.split("::"))
    review = review.map(lambda line: (line[2], line[3]))
    # Joining business with reviews based on the business ids
    joinedBusinessReview = business.join(review, 1)
    # Converting each element into a row to execute sql query
    joinedBusinessReview = joinedBusinessReview.map(lambda line: Row(business_id=line[0], stars=float(line[1][1])))
    rev_df = sqlContext.createDataFrame(joinedBusinessReview)
    rev_df.registerTempTable("review")
    # Executing sql query for calculating average ratings for each business id
    rev_res = sqlContext.sql("select business_id, avg(stars) as avg from review group by business_id")
    # Sorts the results based on the avg rating in ascending order.
    r = rev_res.rdd.map(lambda line: list(line)).repartition(1).sortBy(lambda a: a[1],ascending=True)
    # adds the address and category to each business, and gets the 20 worst business based on avg ratings
    r=r.join(business,1).distinct().map(lambda data: data[0]+"\t"+data[1][1][0]+"\t"+data[1][1][1]+"\t"+str(data[1][0]))
    top20worst = r.take(20)
    result = "\n".join(top20worst)
    file = open("output4.txt", "w+")
    file.write(result)
    spark.stop()
