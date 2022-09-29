def load_data_segment_traces(Asset_list,Raw_data_path,RN,NR):
  
  '''This function takes Asset_list as argument which is its input, it will load the raw wonderware data for these Assets ,construct the traces and mark the three phases of the traces.
  Input :: Name of Assets in a list format, Path where the wonderware data currently stored, Trace directions NR and RN
  Output :: Pyspark data frame with segmented traces'''
  
  #### Loading parquet data from mo_list)))
  ## VALUE column contains the current readings in Amperes
  df = df.withColumnRenamed("Asset", "Name")\
       .withColumnRenamed("Value", "VALUE")
  
  
  ## Renaming the columns
  df = df.withColumn('TraceDir', fn.substring('Attribute', -2,2))
  
  
  # Getting TraceDirection from Attribute, here NR means 'Normal to reverse' and RN means 'Reverse to Normal'
  columns_to_drop =['Attribute', 'Quality','Day','Month','Year']
  df = df.drop(*columns_to_drop)
  
  
  ## Dropping Extra columns
  TimeFormat = "dd/MM/yyyy' 'HH:mm:ss.SSS"
  df = df.withColumn('DateTime2', fn.unix_timestamp('DateTime', TimeFormat) + fn.substring('DateTime', -3,3).cast('float')/1000 )
  
  
  ## Converting the datetime stamp to UNIX (EPOCH) timestamp
  df = df.withColumn('DateTime3', fn.from_unixtime('DateTime2').cast(DateType()))
  
  
  ## Extracting only dates from Datetime stamp
  df = df.withColumn("DateTime4",fn.to_timestamp(fn.col("DateTime2")))
  df = df.withColumn("VALUE", df["VALUE"].cast(DoubleType()))
  ## Casting the VALUE column to double type
  
  #############################################################################################################################################    
  ##### Trace segmentation from raw data for each Asset and marking TraceIds
  
  TimeFormat = "yyyy/MM/dd' 'HH:mm:ss.SSS"
  # Calculating the difference of timestamp with previous timestamp
  df = df.withColumn("difftime",(df.DateTime2 - lag(df.DateTime2,1)\
                            .over(Window.partitionBy("Name", "TraceDir")\
                                  .orderBy("DateTime2")))).na.fill(0)

  # Calculating the difference of current(in Amperes) value with previous current Value
  df = df.withColumn("diffval",(df.VALUE - lag(df.VALUE,1)\
                            .over(Window.partitionBy("Name", "TraceDir")\
                                  .orderBy("DateTime2")))).na.fill(0)

  # TraceNum increases every time two consecutive readings is 0 or timegap between two readings exceeds 5 seconds 
  df = df.withColumn("isgap", ((df.difftime > 5)|(df.difftime < -5)) | ((df.VALUE == 0)&(df.diffval == 0)))

  df = df.withColumn("TraceNum", fn.sum(fn.col("isgap").cast("long")).over(Window.partitionBy("Name")\
                                  .orderBy("DateTime2"))) 

  win = Window.partitionBy("Name", "TraceDir", "TraceNum").orderBy("DateTime2")
  df = df.withColumn("PosIndx", rank().over(win))

  win = Window.partitionBy("Name", "TraceDir", "TraceNum")\
              .orderBy("DateTime2")\
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

  ## PosIndx gives the total swing time or number of datapoints in a trace
  df = df.withColumn("MaxIndx", fn.max("PosIndx").over(win))


  ## This is segmentation logic here 1st 25% of the datapoints are unlocking phase, next 50% of the datapoints are transition phase and last 25% of the datapoints are locking phase. These are represented by 0,1 and 2 respectively

  df = df.withColumn("Phase",\
  (when(col("PosIndx") <= col('MaxIndx')/4, 0)\
  .when(col("PosIndx") <= 3*col('MaxIndx')/4, 1)\
  .otherwise(2)))
  
  return df
