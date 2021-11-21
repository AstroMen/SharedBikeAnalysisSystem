# Linux Operation Instruction

## spark docker command line
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
