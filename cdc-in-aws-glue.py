import sys, json, boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import * 
glueContext = GlueContext(SparkContext.getOrCreate())
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("CDC in AWS Glue") \
    .getOrCreate()

#
# Job Configuration
#

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'config_bucket',
                           'config_key',
						   'source_file',
						   'this_date',
						   'previous_date'])
						   
config_bucket = args['config_bucket']						   
config_key = args['config_key']
incoming_data = {"paths": [args['source_file']]}
this_date = args['this_date']
previous_date = args['previous_date']
						   
print "Job config bucket: ", config_bucket
print "Job config key: ", config_key

#
# Source Job Configuration
#

print "Reading config..."

s3 = boto3.client('s3')
obj = s3.get_object(Bucket=config_bucket, Key=config_key)
config = json.loads(obj['Body'].read())

#
# Support Functions
#

def maptypes(sourcecols,targetcols):
	output = []
	for i in range(len(sourcecols)):
		type_map = (sourcecols[i]["name"],
					sourcecols[i]["type"],
					targetcols[i]["name"],
					targetcols[i]["type"]
					)
		output.append(type_map)
	return output
	
def hashcols(collist, rowobj):
	rowdict = rowobj.asDict()
	outputcollist = []
	for colname in collist:
		outputcollist.append(rowdict[colname])
	cols = tuple(outputcollist)
	hashdigest = hash(cols)
	return hashdigest

def retfinalrowdict(rowdict, keyhash, nonkeyhash):
	output = rowdict
	output["keyhash"] = keyhash
	output["nonkeyhash"] = nonkeyhash
	return output	

def gentgtschema(cols):
	collist = []
	for col in cols:
		if col["type"] == "string":
			collist.append(StructField(col["name"], StringType(), True))
		elif col["type"] == "long":
			collist.append(StructField(col["name"], LongType(), True))
		elif col["type"] == "double":
			collist.append(StructField(col["name"], DoubleType(), True))			
	# add meta cols
	collist.append(StructField("keyhash", LongType(), True))
	collist.append(StructField("nonkeyhash", LongType(), True))
	collist.append(StructField("operation", StringType(), True))
	collist.append(StructField("eff_start_date", StringType(), True))
	collist.append(StructField("eff_end_date", StringType(), True))
	return StructType(collist)

def putmetadata(rowdict, operation, eff_start_date, eff_end_date):
	output = rowdict
	output["operation"] = operation
	output["eff_start_date"] = eff_start_date
	output["eff_end_date"] = eff_end_date
	return output
	
def detectchanges(obj, effdate):
	output = []
	if obj[1][1] is None: 
		# INSERTs, new key hashes
		output.append(putmetadata(obj[1][0], "I", effdate, None))
	elif obj[1][0] is None:
		# DELETEs, missing key hashes
		output.append(putmetadata(obj[1][1], "D", obj[1][1]["eff_start_date"], effdate))
	elif obj[1][0]["nonkeyhash"] != obj[1][1]["nonkeyhash"]:
		# UPDATEs, new value hashes
		output.append(putmetadata(obj[1][0], "U", effdate, None)) # new image
		output.append(putmetadata(obj[1][1], "U", obj[1][1]["eff_start_date"], effdate)) # old image
	else:
		# NOCHANGE, matching key and value hashes
		output.append(putmetadata(obj[1][1], 'X', obj[1][1]["eff_start_date"], None))
	return output	

#
# Read Incoming Data
#

print "Beginning processing..."

newdata_DyF = glueContext.create_dynamic_frame_from_options(
	connection_type = "s3", 
	connection_options = incoming_data,
	format= config['source']['format'], 
	format_options={
		"separator": config['source']['format_options']['separator'], 
		"quoteChar": '"', 
		"multiline": config['source']['format_options']['multiline'], 
		"withHeader": config['source']['format_options']['withHeader'], 
		"writeHeader": config['source']['format_options']['writeHeader'], 
		"skipFirst": config['source']['format_options']['skipFirst']	
	}, 
	transformation_ctx = config['source']['transformation_ctx'])
	
#
# Map Incoming Data to Target Data Types
#

typemapping = maptypes(config['source']['cols'],config['target']['cols'])
newdata_typed_DyF = newdata_DyF.apply_mapping(typemapping)	
newdata_typed_rdd = newdata_typed_DyF.toDF().rdd

#
# Get Hash for Key and Non Key Cols
#

newdata_rdd = newdata_typed_rdd.map(lambda x: (x.asDict(),hashcols(config['target']['key_cols'],x),hashcols(config['target']['non_key_cols'],x))) \
							.map(lambda x: retfinalrowdict(x[0],x[1],x[2]))

#
# Get Previous State
#

previous_path = "s3://" + config['target']["destination_bucket"] + '/' + config['target']["destination_path"] + 'bus_eff_date=' + previous_date

response = s3.list_objects_v2(
    Bucket=config['target']["destination_bucket"],
    Prefix=config['target']["destination_path"] + 'bus_eff_date=' + previous_date
)
	
if response["KeyCount"] == 0:
	# no previous data, everything is treated as an INSERT
	final_rdd = newdata_rdd.map(lambda x: putmetadata(x, "I", this_date, None))
else:
	# Compare current state with previous state
	newdata_interim_DF = spark.createDataFrame(newdata_rdd, 
										schema=gentgtschema(config['target']['cols']), 
										samplingRatio=None, 
										verifySchema=True)		
	prevdata_DyF = glueContext.create_dynamic_frame_from_options(
		connection_type = "s3", 
		connection_options = {"paths": [previous_path]},
		format= "parquet", 
		format_options={}, 
		transformation_ctx = "")
	prevdata_rdd = prevdata_DyF.toDF().rdd.map(lambda x: x.asDict())
	newdata_bykey = newdata_rdd.keyBy(lambda x: x["keyhash"])
	prevdata_bykey = prevdata_rdd.keyBy(lambda x: x["keyhash"])
	joined_bykey = newdata_bykey.fullOuterJoin(prevdata_bykey)
	final_rdd = joined_bykey.flatMap(lambda x: detectchanges(x, this_date))

#
# Output Current Object
#		

final_DF = spark.createDataFrame(final_rdd, 
									schema=gentgtschema(config['target']['cols']), 
									samplingRatio=None, 
									verifySchema=True).coalesce(config['target']['num_partitions'])	
									
current_path = "s3://" + config['target']["destination_bucket"] + '/' + config['target']["destination_path"] + 'bus_eff_date=' + this_date

final_DF.write.parquet(current_path, 
						mode=config['target']['mode'], 
						partitionBy=None, 
						compression=config['target']['compression'])
						
print "Finished processing!"