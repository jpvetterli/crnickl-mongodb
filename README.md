crnickl-mongodb : MongoDB implementation of the CrNiCKL Database 
================================================================

	Copyright 2012-2013 Hauser Olsson GmbH.
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
    	http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

*** 

This is a MongoDB implementation of the CrNiCKL (chronicle) database. 
CrNiCKL (pronounced "chronicle") is a database for time series written in 
Java running on top of SQL and NoSQL systems.

Distribution
------------

The distribution consists of a binary JAR with 
compiled classes, of a javadoc JAR and of a source JAR. This is the first 
version:

	crnickl-mongodb-1.0.0.jar
	crnickl-mongodb-1.0.0-javadoc.jar
	crnickl-mongodb-1.0.0-sources.jar

For Maven users
---------------

The software is available from the <a 
href="http://repo.maven.apache.org/maven2/ch/agent/crnickl-mongodb/">Maven central 
repository</a>. To use version x.y.z, insert the following dependency into your 
`pom.xml` file:

    <dependency>
      <groupId>ch.agent</groupId>
      <artifactId>crnickl-mongodb</artifactId>
      <version>x.y.z</version>
      <scope>compile</scope>
    </dependency>

Building the software
---------------------

The recommended way is to use [git](http://git-scm.com) for accessing the
source and [maven](<http://maven.apache.org/>) for building. The procedure 
is easy, as maven takes care of locating and downloading dependencies:

	$ git clone https://github.com/jpvetterli/crnickl-mongodb.git
	$ cd crnickl-jdbc
	$ mvn install

This builds and installs the distribution JARs in your local maven
repository. They can also be found in the `target` directory.

When building the software by other means, the following dependencies must be
addressed:

- `crnickl-<version>.jar` [CrNiCKL database](http://agent.ch/timeseries/crnickl/)
- `t2-<version>.jar` [Time2 Library](http://agent.ch/timeseries/t2/)  
- `mongo-java-driver-<version>.jar` [MongoDB](http://www.mongodb.org)  

Versions numbers can be found in the <q>POM</q> file included in the binary 
JAR:

	/META-INF/maven/ch.agent/crnickl-mongodb/pom.xml

Unit tests
----------

The following command runs unit tests:

	$ mvn -Dmaven.test.skip=false test

Browsing the source code
------------------------

The source is available on GitHub at 
<http://github.com/jpvetterli/crnickl-mongodb.git>.

Finding more information
------------------------

More information on CrNiCKL is available at 
<http://agent.ch/timeseries/crnickl/>.

<small>Updated: 2013-01-08/jpv</small>

<link rel="stylesheet" type="text/css" href="README.css"/>

