import pandas as pd


all_disease_names_f=open('../disease_names.csv','r')
all_disease_names_l=all_disease_names_f.readlines()
all_disease_names=[i.rstrip() for i in all_disease_names_l]


patient_info=pd.read_csv('../patient_demographic.csv')

number_of_cohort=patient_info.shape[0]
number_of_cohort_female=sum(patient_info['gender']=='Female')
number_of_cohort_male=sum(patient_info['gender']=='Male')

cohort_female_id=patient_info['person_id'][patient_info['gender']=='Female']
cohort_male_id=patient_info['person_id'][patient_info['gender']=='Male']



table_cohort=pd.DataFrame(0,columns=['number_of_cohort','count','count_binary','proportion'],index=all_disease_names)
table_cohort_female=pd.DataFrame(0,columns=['number_of_cohort','count','count_binary','proportion'],index=all_disease_names)
table_cohort_male=pd.DataFrame(0,columns=['number_of_cohort','count','count_binary','proportion'],index=all_disease_names)



for d in all_disease_names:
    disease_f=pd.read_csv('../patient_code/patient_code_'+d+'.csv').set_index('Unnamed: 0')
    #table_cohort
    count=(disease_f.sum(axis=1)>0).astype('int').sum(axis=0)
    count_bi=int(count>0)
    prop=count/number_of_cohort
    row=[number_of_cohort,count,count_bi,prop]
    table_cohort.loc[d]=row
    
    #table_cohort_female
    disease_f_female=disease_f.loc[cohort_female_id]
    count=(disease_f_female.sum(axis=1)>0).astype('int').sum(axis=0)
    count_bi=int(count>0)
    prop=count/number_of_cohort_female
    row=[number_of_cohort_female,count,count_bi,prop]
    table_cohort_female.loc[d]=row
    
    #table_cohort_male
    disease_f_male=disease_f.loc[cohort_male_id]
    count=(disease_f_male.sum(axis=1)>0).astype('int').sum(axis=0)
    count_bi=int(count>0)
    prop=count/number_of_cohort_male
    row=[number_of_cohort_male,count,count_bi,prop]
    table_cohort_male.loc[d]=row
    




table_cohort.to_csv('../cohort_table/table_cohort_all.csv')
table_cohort_female.to_csv('../cohort_table/table_cohort_female.csv')
table_cohort_male.to_csv('../cohort_table/table_cohort_male.csv')






