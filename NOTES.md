                    Copyright 2018 ABSA Group Limited
                  
      Licensed under the Apache License, Version 2.0 (the "License");
      you may not use this file except in compliance with the License.
                You may obtain a copy of the License at
               http://www.apache.org/licenses/LICENSE-2.0
            
     Unless required by applicable law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
                      limitations under the License.

## Installation

(mostly) Windows related remarks

* Have Spark with Hadoop support, still need to have Hadoop as its own separate installation
* [Installing Hadoop on Windows](https://exitcondition.com/install-hadoop-windows/) or 
[How to Install Hadoop on Windows](https://exitcondition.com/install-hadoop-windows/)
* Environment variables:

  * `CATALINA_BASE`
  * `CATALINA_HOME`  
  * `HADOOP`
  * `HADOOP_BIN`
  * `HADOOP_CONF_DIR`
  * `HADOOP_HOME`
  * `JAVA_HOME`

* use slashes in paths except when specifying the jar file for Standardization/Conformance
* Menas _REST_ uri is with `/api` suffix after the _war_ file - e.g. `http://localhost:8080/enceladus_menas_war/api`
  
## Common errors

| Hadoop | Startup |
| --- | --- |

##### Error:

Spin up of Hadoop fails with: 

`'-classpath' is not recognized as an internal or external command, operable program or batch file.`

##### Solution:

The paths for Java and Hadoop env variables ought not to have any space (or other white characters) within. Eg. use `JAVA_HOME=C:\Progra~1\Java\jdk1.8.0_191` instead of `JAVA_HOME=C:\Program Files\Java\jdk1.8.0_191`

| Hadoop | Startup |
| --- | --- |

##### Error:

Spin up of Hadoop fails with: 
```
FATAL org.apache.hadoop.hdfs.server.namenode.NameNode: 
Exception in namenode join java.net.BindException: Port in use: 0.0.0.0:50070`
```

##### Solution:

Set Hadoop _dfs.http.address_ in **core-site.xml** and  _dfs.namenode.http-address_ in **hdfs-site.xml** to different values. Eg.:

###### core-site.xml

  ```
    <property>
        <name>dfs.http.address</name>
        <value>localhost:50170</value>
    </property>
  ```

###### hdfs-site.xml

  ```
    <property>
      <name>dfs.namenode.http-address</name>
      <value>localhost:50070</value>
      <description>
        The address and the base port where the dfs namenode web ui will listen on.
      </description>
    </property>
  ```

| Hdfs | Command-line |
| --- | --- |

##### Error:

On a freshly formatted node upon `ls`:
```
hdfs dfs -ls ls: `.': No such file or directory
```

##### Solution:

The default directory for the connected user is `/user/%USERNAME%` where `%USERNAME%` is of the connected user. But this 
directory does not exist by default. Execute: `hdfs dfs -mkdir /user/%USERNAME%` to create the home directory - if that 
is needed and/or use absolute path to access the files, e.g. `hdfs dfs -ls /`
