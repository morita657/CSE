# Hot-Spot-Analysis
This is one of the projects of CSE-511 at Arizona State University.

## Outline
This projects consist of two projects. The first project is to find out the whole of the New York City taxi locations which is within the targeted area. The second project is to find out hot zones using the collected New York City taxi data from January 2009 to June 2015. [ACM SIGSPATIAL Cup 2016](http://sigspatial2016.sigspatial.org/giscup2016/) details the second project problem definition, which is to identify a list of the fifty most significant hot spot cells in time and space with the Getix-Ord Gi* statistic.

## Requirements
* Java8:
Java should be installed on your system PATH, or the JAVA_HOME environment
variable pointing to a Java installation.
* Scala:
Scala(www.scala-lang.org) whose version is compatible with your Spark should be
installed on your system.
* sbtoranyotherScalaIDE:
To compile this source code, SBT(www.scala-sbt.org) or any other Scala IDE is
needed.
* Spark:
Spark (spark.apache.org) should be installed on your system. Generally latest version is recommended and this code was compiled and tested on the Spark 2.4.5

## Files
* build.sbt: configuration file 
* SparkSQL.scala: query loader file
* SpatialQuery.scala: data processing file

## Main Functions
### 1. ST_Contains

Input: pointString:String, queryRectangle:String

Output: Boolean (true or false)

Definition: Parse the pointString (e.g., "-88.331492,32.324142") and queryRectangle (e.g., "-155.940114,19.081331,-155.618917,19.5307") to a format. Then check whether the queryRectangle fully contains the point considering on-boundary point.

### 2. ST_Within

Input: pointString1:String, pointString2:String, distance:Double

Output: Boolean (true or false)

Definition: Parse the pointString1 (e.g., "-88.331492,32.324142") and pointString2 (e.g., "-88.331492,32.324142") to a format. Then check whether the two points are within the given distance. Consider on-boundary point. To simplify the problem, assume all coordinates are on a planar space and calculate their Euclidean distance.

## Queries
* Range query: Given a query rectangle R and a set of points P, find all the points within R.
```
select * 
from point 
where ST_Contains(point._c0,'-155.940114,19.081331,-155.618917,19.5307')
```

* Range join query: Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs such that the point is within the rectangle.
```
select * 
from rectangle,point 
where ST_Contains(rectangle._c0,point._c0)
```

* Distance query: Given a point location P and distance D in km, find all points that lie within a distance D from P
```
select * 
from point 
where ST_Within(point._c0,'-88.331492,32.324142',10)
```

* Distance join query: Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1, s2) pairs such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).
```
select * 
from point p1, point p2 
where ST_Within(p1._c0, p2._c0, 10)
```

### Usage
If you are using the Scala template, note that:
* The main function in this template takes **dynamic length of parameters** as follows:
	* Output file path (**Mandatory**): ```/Users/ubuntu/Downloads/output```
	* Range query data file path, query window: ```rangequery /Users/ubuntu/Downloads/arealm.csv -155.940114,19.081331,-155.618917,19.5307```
	* Range join query data file path, range join query window data file path: ```rangejoinquery /Users/ubuntu/Downloads/arealm.csv /Users/ubuntu/Downloads/zcta510.csv```
	* Distance query data file path, query point, distance: ```distancequery /Users/ubuntu/Downloads/arealm.csv -88.331492,32.324142 10```
	* Distance join query data A file path, distance join query data B file path, distance: ```distancejoinquery /Users/ubuntu/Downloads/arealm.csv /Users/ubuntu/Downloads/arealm.csv 10```
* Run using `spark-submit CSE512-Project-Phase2-Template-assembly-0.1.0.jar result/output rangequery src/resources/arealm10000.csv -93.63173,33.0183,-93.359203,33.219456 rangejoinquery src/resources/arealm10000.csv src/resources/zcta10000.csv distancequery src/resources/arealm10000.csv -88.331492,32.324142 1 distancejoinquery src/resources/arealm10000.csv src/resources/arealm10000.csv 0.1`