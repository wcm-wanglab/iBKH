#### This code is used to generate the patient x code matrix
#### take inputs: 1. person_df 2. condition/measurement/observation_df 3. disease name
#### output will be saved in ../patient_code/

##################
import pandas as pd
import sys
import pickle
#import json

###input: person_df. A dataframe
###       df (for a certain disease). A disctionary contains dataframes
###       disease name. A string
person_df=pd.read_csv(sys.argv[1])
type_dfs_f=pickle.load(open(sys.argv[2],'rb'))#{'condition':condition_df,...}
disease_name=sys.argv[3]#eg. cancer

##load observation window time for each patient
observe_file_path='../AD_observe_time.pickle'
ob_f=open(observe_file_path,'rb')
ob_time=pickle.load(ob_f)

##get all patient id
all_patient_ids=person_df['person_id'].unique()
#all_patient_code_df=pd.DataFrame(0,index=all_patient_ids)

##get type names, i.e. condition/observation/measurement
type_names=type_dfs_f.keys()
num_type=len(type_names)

#get type_dfs and sourve values and create an empty dataframe
unique_source_values=[]
type_dfs={}
for type_name in type_names:
    type_dfs[type_name]=pd.read_csv(type_dfs_f[type_name])
    unique_source_values.extend(type_dfs[type_name][type_name+'_source_value'].unique().tolist())
unique_source_values=list(set(unique_source_values))
patient_code_df=pd.DataFrame(0,columns=unique_source_values,index=all_patient_ids)

for type_name in type_names:
    #get condition/measurement/observation_df
    type_df=type_dfs[type_name]
    
    #get source value column name
    source_value=type_name+'_source_value'
    
    #get datetime column name
    if type_name=='condition':
        datetime_name=type_name+'_start_datetime'
    else:
        datetime_name=type_name+'_datetime'

    #generate patient by condition_start_value binary matrix
    #all_codes=type_df[source_value].unique()
    for i in all_patient_ids:
        sub=type_df[type_df['person_id']==i]
        if sub.shape[0] > 0:#has that person id
            for j in range(sub.shape[0]):
                subb=sub.iloc[j,:]
                time=subb[datetime_name]
                if type(time)!=type(ob_time[i]):
                    time=pd.to_datetime(time)
                if time > ob_time[i]:
                    code=subb[source_value]
                    patient_code_df.loc[i][code]+=1
                else:#outside observation time
                    pass
        else:
            pass

patient_code_df.to_csv('../patient_code/patient_code_'+str(disease_name)+'.csv')



