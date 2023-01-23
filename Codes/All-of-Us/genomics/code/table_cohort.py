import pandas as pd



all_genes_f=pd.read_csv('../gwas_select_p1.csv')### filtered GWAS reference
all_genes=all_genes_f['MAPPED_GENE'].unique()


sample_gene_df=pd.read_csv('../sample_gene_all.csv')

all_samples=sample_gene_df.columns[1:]
number_of_cohort=len(all_samples)
num_cohort_all=[number_of_cohort]*len(sample_gene_df['gene'])
count=sample_gene_df.iloc[:,1:].sum(axis=1)
count_bi=(count>0).astype('int')
prop=count/number_of_cohort

##### generate a table for the whole cohort
table_cohort_all=pd.DataFrame(columns=['name','num_cohort','count','count_binary','proportion'])
table_cohort_all['name']=sample_gene_df['gene']
table_cohort_all['num_cohort']=num_cohort_all
table_cohort_all['count']=count
table_cohort_all['count_binary']=count_bi
table_cohort_all['proportion']=prop

table_cohort_all=table_cohort_all.sort_values('proportion',ascending=False)
table_cohort_all.to_csv('../cohort_table_all.csv',index=False)


##### generate two tables for female patients and male patients sepreatly
patient_info=pd.read_csv('../patient_demographic.csv')# can use the script under diagnosis_code/code/get_observation_time.py to generate the patient_demographic.csv
female_ids=[]
male_ids=[]
female_df=pd.DataFrame()
male_df=pd.DataFrame()
female_df['gene']=sample_gene_df['gene']
male_df['gene']=sample_gene_df['gene']
for i in all_samples:
    gender=patient_info[patient_info['person_id']==int(i)]['gender']
    if (gender == 'Female').bool():
        female_ids.append(i)
        female_df[i]=sample_gene_df[i]
    elif (gender == 'Male').bool():
        male_ids.append(i)
        male_df[i]=sample_gene_df[i]
    else:
        pass
        

#### female
number_of_cohort_female=len(female_ids)
num_cohort_female=[number_of_cohort_female]*len(female_df['gene'])
count=female_df.iloc[:,1:].sum(axis=1)
count_bi=(count>0).astype('int')
prop=count/number_of_cohort_female

table_cohort_female=pd.DataFrame(columns=['name','num_cohort','count','count_binary','proportion'])
table_cohort_female['name']=female_df['gene']
table_cohort_female['num_cohort']=num_cohort_female
table_cohort_female['count']=count
table_cohort_female['count_binary']=count_bi
table_cohort_female['proportion']=prop

table_cohort_female=table_cohort_female.sort_values('proportion',ascending=False)
table_cohort_female.to_csv('../cohort_table_female.csv',index=False)


#### male
number_of_cohort_male=len(male_ids)
num_cohort_male=[number_of_cohort_male]*len(male_df['gene'])
count=male_df.iloc[:,1:].sum(axis=1)
count_bi=(count>0).astype('int')
prop=count/number_of_cohort_male

table_cohort_male=pd.DataFrame(columns=['name','num_cohort','count','count_binary','proportion'])
table_cohort_male['name']=male_df['gene']
table_cohort_male['num_cohort']=num_cohort_male
table_cohort_male['count']=count
table_cohort_male['count_binary']=count_bi
table_cohort_male['proportion']=prop

table_cohort_male=table_cohort_male.sort_values('proportion',ascending=False)
table_cohort_male.to_csv('../cohort_table_male.csv',index=False)

