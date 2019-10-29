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

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: wordcount <bussiness> <review> <users>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("yelpDatasetUsers")\
        .getOrCreate()
    # business [1], review [2] and user [3]

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    mapped = lines.map(lambda line: line.split("::"))
    # Filters the data and gets only those business which has category 'Colleges & Universities'
    business = mapped.filter(lambda line: "Colleges & Universities" in line[2]).map(lambda line: (line[0],0))
    review = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])
    review = review.map(lambda line: line.split("::"))
    review = review.map(lambda line: (line[2],(line[1],line[3])))
    # Joining business with the reviews based on business ids
    joinedBusinessReview = business.join(review, 1)
    users = spark.read.text(sys.argv[3]).rdd.map(lambda r: r[0])
    users = users.map(lambda line: line.split("::"))
    joinedBusinessReview=joinedBusinessReview.map(lambda data: (data[1][1][0],data[1][1][1]))
    # Joining business review with users based on user ids
    userReviews = joinedBusinessReview.join(users,1).distinct()
    userReviews = userReviews.map(lambda  data: data[0]+"\t"+data[1][1]+"\t"+data[1][0])
    userReviews.saveAsTextFile("userReviews")
    spark.stop()