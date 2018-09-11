**cdc-in-aws-glue**
==============
Source change detection (CDC) implemented using AWS Glue and PySpark:  

cdc-in-aws-glue.py is an AWS Glue script written in Python which accepts a set of input arguments and reads config from a JSON document (see test/config in this project)
. The script is executed by an AWS Glue Job which compares an incoming CSV dataset ins S3 with current versions of the same object stored as Parquet file(s) in S3.  Changes 
are detected by comparing an tuple hash of the natural key and non key columns respectively with equivalent hashes from existing records.  The resultant detected changes 
are written to a new Parquet formatted object with additional meta columns (conformant to the Type 2 SCD specification):

    keyhash, nonkeyhash, operation, eff_start_date, eff_end_date
    e.g.
    .., 4501294689844871168, -404260490002337973, 'I', '2018-08-01', None 
    .., 4501294689844871168, -404260490002337973, 'U', '2018-08-01', '2018-08-02' 
    .., 4501294689844871168, 7110980960449481338, 'U', '2018-08-02', None 	
    .., 4501294689844871168, 7110980960449481338, 'D', '2018-08-02', '2018-08-03' 	
    .., 3454310847678873606, -4750952506199758578, 'X', '2018-08-01', None 	
    
Current records are identified by `eff_end_date is null` or `eff_end_date is None` 

A generalisation of the approach is:  

*Compare INCOMING to CURRENT using DATASETCONFIG at EFFECTIVETIME save as OUTPUT*  

This approach can easily be adapted to any source dataset and can not only react to changes in the data but can handle changes in the structure of incoming data with respect 
to the pre-existing object, eg the addition of a new column would be handled seamlessly as an update to (all) records in the dataset.

**So this is effectively CDC for NoSQL sources as well as conventional SQL sources**
	
Dependencies
--------------
- AWS Glue

cdc-in-aws-glue.py Usage
--------------
The `cdc-in-aws-glue.py` script is designed to be imported into AWS Glue and used as the custom script for an AWS Glue Job.  More information can be found at [Providing Your Own Custom Scripts to AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/console-custom-created.html). 
Run the AWS Glue Job you created using the custom script (`cdc-in-aws-glue.py`) supplying runtime arguments of: `['JOB_NAME','config_bucket','config_key','source_file','this_date','previous_date']`
						   
