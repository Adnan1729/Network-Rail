
#Extracting the features

def create_features(Asset_list,Raw_data_path,RN,NR):
  
  '''This function takes the extracted traces and creates following seven features for the given assets
  Feature 1 ::'SwingTime'
  feature 2:: 'midcurrent_deviation'
  feature 3:: 'First_phase_min_current_deviation'
  feature 4 :: 'MidMax_Current_dev_from_7amp'
  feature 5 :: "Swingtime_diff"
  feature 6 :: 'Season'
  feature 7 :: 'TraceDir_New'
  
  Input :: Pyspark dataframe Pyspark data frame with segmented traces which was created in the previous notebook
  Output :: Pandas dataframe containing all seven features for all the traces.
  '''
    
  sdf = load_data_segment_traces(Asset_list,Raw_data_path,RN,NR) 
  # calling load_data_segment_traces function to load the extracted traces
  
  sdf = sdf.sort('Name','DateTime2')
  w = (Window.partitionBy(['Name','TraceNum']).orderBy(fn.col("DateTime2")).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

  sdf = sdf.withColumn("full_data_list", fn.collect_list("VALUE").over(w))
  # This is to collect all the values of a trace in one row.

  # This function is to  calculate the minimum current value in unlocking phase of the trace
  def min_val(x):
    try:
      min_x = np.min(x[10:41]).tolist()
    except: 
      min_x = np.nan
    return min_x 

  get_min_curr_udf = fn.udf(min_val, returnType=DoubleType())
  ## Creating udf function to run this in pyspark
  
  sdf = sdf.withColumn("ph1_Min_Val", get_min_curr_udf("full_data_list"))
  # "ph1_Min_Val" : This value will be used while calculating feature 3 i.e 'First_phase_min_current_deviation'



  win1 = Window.partitionBy("Name", "TraceDir",'phase')\
              .orderBy("DateTime2")\
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  
  # This is the formula for to calculate Average current Asset and phase wise
  sdf = sdf.withColumn("Avg_current_Asset&phase_wise",fn.avg(fn.col('VALUE')).over(win1))
  
  # This is the formula for to calculate median current Asset and phase wise
  magic_percentile1 = fn.expr('percentile_approx(VALUE, 0.5)')
  sdf = sdf.withColumn("Median_current_Asset&phase_wise",magic_percentile1.over(win1))
  

  # Creating Features 
  # Feature 1 ::'SwingTime': This calculates the total time that has been taken for the machine for one swing 
  TimeFormat = "dd/MM/yyyy' 'HH:mm:ss.SSS"
  feature_df = sdf.groupBy("Name","TraceDir", "TraceNum").agg(fn.min("DateTime2").cast(TimestampType()).alias("TimeStamp")
                                                            ,fn.min("DateTime2").alias("Trace_Start_Time")
                                                            ,fn.min("DateTime3").alias("DATE")
                                                            ,fn.max("DateTime2").alias("Trace_End_Time")  
                                                            ,fn.max("PosIndx").alias("SwingTime")  
                                                            ,fn.max(fn.when(fn.col("phase") == 1, fn.col("Avg_current_Asset&phase_wise")).otherwise(fn.lit(0))).alias("mid_Avg_current_of_Asset")
  # "mid_Avg_current_of_Asset" :: This value will be used while calculating feature 2 i.e 'midcurrent_deviation'                                       
                                                            ,fn.max(fn.when(fn.col("phase") == 0, fn.col("Median_current_Asset&phase_wise")).otherwise(fn.lit(0))).alias("First_phase_Median_current_of_Asset")                                
  # "First_phase_Median_current_of_Asset" :: This value will be used while calculating feature 3 i.e 'First_phase_min_current_deviation'.  
                                                            ,fn.max(fn.when(fn.col("phase") == 1, fn.col("VALUE")).otherwise(fn.lit(0))).alias("MidMax_Current")
  # "MidMax_Current" :: This value will be used while calculating the feature 2--'midcurrent_deviation' and feature 4 -'MidMax_Current_dev_from_7amp'                                                        
                                                            ,fn.first(fn.col("ph1_Min_Val")).alias("Ph1_MinVal")
                                                            ,fn.avg(fn.when(fn.col("phase") == 0, fn.col("VALUE")).otherwise(fn.lit(0))).alias("First_Phase_Avg_Current"))
  
  feature_df = feature_df.withColumn('midcurrent_deviation',(fn.col('MidMax_Current'))-(fn.col('mid_Avg_current_of_Asset'))) 
  # feature 2:: 'midcurrent_deviation' : It calculates the difference of Maximum current in mid phase of an asset from average current in mid                           phase of that asset

  feature_df = feature_df.withColumn('First_phase_min_current_deviation',fn.col('First_phase_Median_current_of_Asset')-fn.col('Ph1_MinVal')) 
  # feature 3:: 'First_phase_min_current_deviation' : It calculates the difference of minimum current in first phase of trace with that of                              median current in first phase of that asset

  feature_df = feature_df.withColumn('MidMax_Current_dev_from_7amp', 7 - fn.col('MidMax_Current'))
  # feature 4 :: 'MidMax_Current_dev_from_7amp' : It calculates the difference of maximum current in mid phase of the trace from 7 Amperes.

  feature_df = feature_df.withColumn("Swingtime_diff",(feature_df.SwingTime - fn.lag(feature_df.SwingTime,1)\
                          .over(Window.partitionBy("Name", "TraceDir")\
                                .orderBy("TraceNum")))).na.fill(0) 
  # feature 5 :: "Swingtime_diff" : It calculates the difference in swingtime of present trace with that of previous trace.
  
  feature_pdf = feature_df.toPandas()
  feature_pdf.rename(columns= {"DateTime3":"DATE"},inplace = True)
  feature_pdf['DATE'] = pd.to_datetime(feature_pdf['DATE'])
  feature_pdf["Month"] = feature_pdf["DATE"].dt.month
  feature_pdf["Season"] = feature_pdf["Month"]
  feature_pdf["Season"] = feature_pdf["Season"].replace([11, 12, 1, 2], "Low_Temp")
  feature_pdf["Season"] = feature_pdf["Season"].replace([3, 4, 5, 6], "High_Temp")
  feature_pdf["Season"] = feature_pdf["Season"].replace([7, 8, 9, 10], "Medium_Temp")
  feature_pdf["Season"] = feature_pdf["Season"].replace(["Low_Temp", "High_Temp", "Medium_Temp"], [0,2,1])
  # feature 6 :: 'Season' : It divides the months into three buckets of low temperature, medium temp and high temp and represented by 0,1 and 2                                     respectively

  feature_pdf["TraceDir_New"] = feature_pdf["TraceDir"]
  feature_pdf['TraceDir_New'] = feature_pdf['TraceDir'].replace(['NR','RN'],[0,1])
  
  # feature 7 :: 'TraceDir_New' : It represents the direction in which the asset is swinging NR - Normal to Reverse represented by 0 and RN-                              Reverse to Normal represented by 1.
  
  return feature_pdf
     
