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


def MF_map(line):
    #Splits the line and converts it into (friendIdPair,friendIDList)
    line = line.split("\n")
    data = []
    for value in line:
        value = value.split("\t")
        if len(value) == 2 and value[1].strip() != "":
            userId = int(value[0])
            friendIds = value[1].split(",")
            for friend in friendIds:
                friendId=int(friend)
                if userId < int(friend):
                    key = str(userId) + "," + str(friendId)
                else:
                    key = str(friendId) + "," + str(userId)
                data.append((key, value[1]))
    return data

def MF_reduce(values1, values2):
    #Finds the count of mutual friends among the same friendIDPair key
    count = 0
    set1 = values1.split(",")
    set2 = values2.split(",")

    for friend in set1:
        if friend in set2:
            count += 1
    return count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonMutualFriendsCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    mapped = lines.flatMap(MF_map)
    res = mapped.reduceByKey(MF_reduce)
    #Filters the friendIDPairs which doesn't have any mutual friends
    res = res.filter(lambda  x : x[1]>0).map(lambda x: str(x[0])+"\t"+str(x[1]))
    res.saveAsTextFile("output1")
    spark.stop()