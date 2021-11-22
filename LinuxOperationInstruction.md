# Linux Operation Instruction

## AsterixDB Installation
### Configuring SSH
Refer to https://asterixdb.apache.org/docs/0.9.2/install.html
```shell

luyao@Luyaos-MacBook-Pro Versions % ssh 127.0.0.1
ssh: connect to host 127.0.0.1 port 22: Connection refused

luyao@Luyaos-MacBook-Pro Versions % sudo launchctl load -w /System/Library/LaunchDaemons/ssh.plist
luyao@Luyaos-MacBook-Pro Versions % sudo launchctl list | grep ssh                                
-	0	com.openssh.sshd
luyao@Luyaos-MacBook-Pro Versions % ssh 127.0.0.1
The authenticity of host '127.0.0.1 (127.0.0.1)' can't be established.
ECDSA key fingerprint is SHA256:eRJVk42fS845ov/p443nZ8svOMWFJ33VHh33cFjo1FOkqg.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added '127.0.0.1' (ECDSA) to the list of known hosts.
Password:
Last login: Sat Nov 20 22:28:48 2021

luyao@Luyaos-MBP ~ % sudo launchctl list | grep ssh
Password:
-	0	com.openssh.sshd
17744	0	com.openssh.sshd.33267223119-4D74-4D48-8C44-DFFD7BA967C8

luyao@Luyaos-MBP ~ % ssh 127.0.0.1
Password:
Last login: Sat Nov 20 23:15:24 2021
luyao@Luyaos-MBP ~ % exit
Connection to 127.0.0.1 closed.
```

### Startup AsterixDB
Refer to https://asterixdb.apache.org/docs/0.9.7/ncservice.html
```shell
luyao@Luyaos-MBP apache-asterixdb-0.9.7 % pwd
/Users/luyao/Workspace/AsterixDB/apache-asterixdb-0.9.7
luyao@Luyaos-MBP apache-asterixdb-0.9.7 % cd opt/local/bin 
luyao@Luyaos-MBP bin % ls
start-sample-cluster.bat	stop-sample-cluster.bat
start-sample-cluster.sh		stop-sample-cluster.sh
luyao@Luyaos-MBP bin % ./start-sample-cluster.sh 
CLUSTERDIR=/Users/luyao/Workspace/AsterixDB/apache-asterixdb-0.9.7/opt/local
INSTALLDIR=/Users/luyao/Workspace/AsterixDB/apache-asterixdb-0.9.7
LOGSDIR=/Users/luyao/Workspace/AsterixDB/apache-asterixdb-0.9.7/opt/local/logs

Using Java version: 16.0.2
INFO: Starting sample cluster...
Using Java version: 16.0.2
INFO: Waiting up to 90 seconds for cluster 127.0.0.1:19002 to be available.
INFO: Cluster started and is ACTIVE.
```
AsterixDB UI: http://localhost:19001/

### AsterixDB SQL primer document
https://asterixdb.apache.org/docs/0.9.2/sqlpp/primer-sqlpp.html

### AsterixDB HTTP API
https://asterixdb.apache.org/docs/0.9.2/api.html

### Python wrapper for AsterixDB HTTP API
https://github.com/j-goldsmith/asterixdb-python

## Spark docker command line
1. Startup a spark cluster of single working node in standlone mode

```shell
docker-compose up -d

Starting docker-spark_master_1 ... done
Starting docker-spark_worker_1 ... done
````
SparkUI:
http://localhost:8080/
http://localhost:8081/

2. Check process status
```shell
docker-compose ps

        Name                       Command               State                  Ports               
----------------------------------------------------------------------------------------------------
docker-spark_master_1   bin/spark-class org.apache ...   Up      0.0.0.0:4040->4040/tcp,            
                                                                 0.0.0.0:6066->6066/tcp, 7001/tcp,  
                                                                 7002/tcp, 7003/tcp, 7004/tcp,      
                                                                 7005/tcp, 0.0.0.0:7077->7077/tcp,  
                                                                 0.0.0.0:8080->8080/tcp             
docker-spark_worker_1   bin/spark-class org.apache ...   Up      7012/tcp, 7013/tcp, 7014/tcp,      
                                                                 7015/tcp, 0.0.0.0:8081->8081/tcp,  
                                                                 8881/tcp    
```
```shell
docker-compose ps

        Name                      Command               State     Ports
------------------------------------------------------------------------
dockerspark_master_1   /etc/bootstrap.sh bash /us ...   Up      ...
dockerspark_worker_1   /etc/bootstrap.sh bash /us ...   Up      ...
dockerspark_worker_2   /etc/bootstrap.sh bash /us ...   Up      ...

```

3. Stop container
```shell
docker-compose stop
```

4. Delete container
```shell
docker-compose rm
```

5. Command line login master node of spark
```shell
docker exec -it [containerid] /bin/bash
docker exec -it 08c9e81dce49 /bin/bash
root@worker:/usr/spark-2.4.1# 
```

6. Capacity expansion/reduction
```shell
docker-compose scale worker=2
```


## Spark task submit
1. spark-submit
/usr/local/spark/bin/spark-submit --master spark://master:7077 --class org.apache.spark.examples.SparkPi /usr/local/spark/lib/spark-examples-1.6.0-hadoop2.6.0.jar 1000

2. spark-submit提交 $SPARK_HOME/bin/spark-submit --class "com.path.**.类名" --master local[] "jar包"


## python
### New virtual env
pyenv activate env_name     
pyenv deactivate

### ipython notebook
pip install ipython notebook        
ipython notebook --ip 0.0.0.0

Every cell could be inputed a group of command, press shift + enter to execute.

Notification:
Show plot need ```%matplotlib inline```     
Magic instruction:      
Start with ```%``` is line magic, it works for single line code。     
Start with ```%%``` is cell magic, it works for code block。
(注意：输入%%time后即进入多行操作，要用shift+enter多行输出）
(1)%time,%timeit
IPython提供两个魔术函数来计算代码执行时间
%time一次执行一条语句，然后报告总体执行时间
