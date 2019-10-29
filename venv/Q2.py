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

def UD_map(Line):
    #Loads the data from userData.txt and appends the data to the exisitng key,value pair
    eachValue = Line.split('\t')[1].split(',')
    file = open(userData,'r')
    line = file.readline()
    city1=city2=name1=name2=age1=age2="none"


    while line:
        dataItems=line.split(',')
        if(name1 != "none" and name2!= "none"):
            break
        if eachValue[0]  == dataItems[0]:
            name1= dataItems[1]
            city1= dataItems[4]
            today = date.today()
            birthDate = datetime.strptime(dataItems[9].strip(), '%m/%d/%Y')
            age1 = today.year - birthDate.year - ((today.month, today.day) <  (birthDate.month, birthDate.day))

        elif eachValue[1] == dataItems[0]:
            name2 = dataItems[1]
            city2 = dataItems[4]
            today = date.today()
            birthDate = datetime.strptime(dataItems[9].strip(), '%m/%d/%Y')
            age2 = today.year - birthDate.year - ((today.month, today.day) <  (birthDate.month, birthDate.day))
        line = file.readline()
    return Line.split('\t')[0]+"\t"+name1+"\t"+city1+"\t"+str(age1)+"\t"+name2+"\t"+city2+"\t"+str(age2)


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
    if len(sys.argv) != 3:
        print("Usage: wordcount <mf file> <userData>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonMutualFriendsCountTop10")\
        .getOrCreate()

    userData= sys.argv[2]
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    mapped = lines.flatMap(MF_map)
    res = mapped.reduceByKey(MF_reduce)
    #Filters the friendIDPairs which doesn't have any mutual friends, sorts in decending order,
    #appends the extra data about the name,age and city of the user, takes only the top 10 results
    res = res.filter(lambda  x : x[1]>0).sortBy(lambda x: x[1],False).map(lambda x: str(x[1]) + "\t" + str(x[0])
).map(UD_map).take(10)
    result="\n".join(res)
    file = open("output2.txt", "w+")
    file.write(result)
    spark.stop()