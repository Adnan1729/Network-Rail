
Loading model and Prediction.

def predictions_at_trace_level(Asset_list,Raw_data_path,RN,NR,features_to_test,model_path):
  
  '''This function loads the trained random forest model which is in .sav format and then gives predictions as 0 or 1 for every trace, 0 being               anomalous and 1 being Normal
  
  Input :: Pandas dataframe containing all seven features for all the traces and trained random forest model
  Output :: Pandas dataframe containing the predictions of random forest against all the traces either 0 or 1'''
  
  Feature_pdf = create_features(Asset_list,Raw_data_path,RN,NR)
  ### calling create_feature function from the previous notebook
  
  Test_data = Feature_pdf.copy()
  Test_data = Test_data[features_to_test]
  ### Selecting the features on which the predictions will be made.
  
  loaded_model=pickle.load(open(model_path,'rb'))
  ### Loading the trained random forest model.
  
  predictions = loaded_model.predict(Test_data)
  #### Predicting on the features using the trained model
  
  Feature_pdf['forest_Predictions'] = predictions.tolist()
  ### Storing the predictions from random forest model
  
  Feature_pdf['TraceNum'] = Feature_pdf['TraceNum'].astype('float')
  Feature_pdf['TraceDir_New'] = Feature_pdf['TraceDir_New'].astype('float')
  Feature_pdf['forest_Predictions'] = Feature_pdf['forest_Predictions'].astype('float')
  ### Converting datatype to float
  
  model_result = Feature_pdf[['Name', 'DATE', 'TraceDir', 'TraceNum', 'TimeStamp', 'Trace_Start_Time','forest_Predictions']]
  ### Selecting the relevant columns only
  
  model_result = model_result.sort_values(['Name','Trace_Start_Time'])
  ### Sorting the data on Name and Trace Start Time
  
  return model_result
     
