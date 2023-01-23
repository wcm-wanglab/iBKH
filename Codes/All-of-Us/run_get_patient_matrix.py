####run get_patitent_code_matrix.py
#### about 1 minute
import os
import glob
import pickle
df_path='../EHR_data/'

all_disease_names_f=open('../disease_names.csv','r')
all_disease_names_l=all_disease_names_f.readlines()
all_disease_names=[i.rstrip() for i in all_disease_names_l]

for name in all_disease_names:
	if os.path.exists('../patient_code/patient_code_'+name+'.csv'):
		pass
	else:
		files=glob.glob(df_path+name+'*')
		person_df_f=[f for f in files if "person" in f][0]
		
		type_dfs={}
		condition_df_f=[f for f in files if "condition" in f]
		observation_df_f=[f for f in files if "observation" in f]
		measurement_df_f=[f for f in files if "measurement" in f]
		if len(condition_df_f)>0:
			type_dfs['condition']=condition_df_f[0]
		if len(observation_df_f)>0:
			type_dfs['observation']=observation_df_f[0]
		if len(measurement_df_f)>0:
			type_dfs['measurement']=measurement_df_f[0]

		dict_f=name+'_type_dfs.pickle'
		pickle.dump(type_dfs,open(dict_f,'wb'))
		os.system('python get_patient_code_matrix.py %s %s %s'%(person_df_f,dict_f,name))
		os.system('rm '+dict_f)
		print('finish '+name)


