## Solution
*pyspark_job.py* - script

*output_data/result/part-00000-a4970e75-05fc-4fa7-a748-ed30f9d918f3-c000.snappy.parquet* - file with results

## To launch my solution you should: 
copy folder **pyspark_project** to server or computer or docker container which has installed pyspark

You should be convinced that spark job can write to output_data folder. 
If it’s not so, you should grant permissions to this folder. E.g. executing:
```
sudo chmod 777 -R output_data
```

Inside folder pyspark_project execute a command:
```
$SPARK_HOME/bin/spark-submit \
pyspark_job.py \
--master local \
--deploy-mode client \
--files input_data
```



## I did my solution in docker using apache/spark-py image

To check solution with this docker image you should:
copy folder pyspark_project to docker host and inside this folder:

execute  this:
```
sudo chmod 777 -R output_data
```

and execute  this:
```
docker run \
--name pyspark_task \
--rm \
-v "${PWD}":/opt/spark/work-dir \
apache/spark-py \
/opt/spark/bin/spark-submit \
pyspark_job.py \
--master local \
--deploy-mode client \
--files input_data
```
