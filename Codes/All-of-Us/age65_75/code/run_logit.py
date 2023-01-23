import pandas as pd
from datetime import datetime, date
import statsmodels.api as sm

### get sample info, same for all diseases
patient_info=pd.read_csv('../EHR_data/amnesia_person_df.csv')##randoms select one person df since all person df are the same
patient_info_sub0=patient_info[['person_id','race','sex_at_birth','ethnicity']]
age=[]
for i in patient_info_sub0['person_id']:   
    #age
    born=patient_info[patient_info['person_id']==i]['date_of_birth'].tolist()[0].split(' ')[0]
    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    a=today.year - born.year - ((today.month,
                                              today.day) < (born.month,
                                                            born.day))

    age.append(a)

patient_info_sub0['age']=age
patient_info_sub0=patient_info_sub0.rename(columns={'sex_at_birth':'sex'})

### apoe ids
apoe_info=pd.read_csv('../apoe_ids.csv')
apoe_ids=apoe_info['Unnamed: 0'].to_list()

### save results as dataframe
save_table=pd.DataFrame(columns=['apoe','p_apoe','sex','p_sex','race','p_race','age','p_age','ethnicity','p_ethnicity'])

##loop for each disease
all_disease_names_f=open('../disease_names.csv','r')
all_disease_names_l=all_disease_names_f.readlines()
all_disease_names=[i.rstrip() for i in all_disease_names_l]

for disease_name in all_disease_names:
    #disease_name='AlzheimersDisease'

    disease_f=pd.read_csv('../patient_code/patient_code_'+disease_name+'.csv').set_index('Unnamed: 0')
    patient_code_bi=(disease_f.sum(axis=1)>0).astype('int')

    # get apoe, disease variable
    is_apoe=[]
    is_disease=[]

    patient_info_sub=patient_info_sub0
    for i in patient_info_sub['person_id']:
        is_disease.append(patient_code_bi[i])
        #apoe
        if i in apoe_ids:
            is_apoe.append(1)
        else:
            is_apoe.append(0)

    patient_info_sub['apoe']=is_apoe
    patient_info_sub['disease']=is_disease

    ##race 1: white 0:others; ethnicity: 1: hisxxx 0: others; sex: 1: female 0: male
    replace_race = {'I prefer not to answer' : 0,\
                    'None of these' : 0, \
                    'More than one population' : 0,\
                    'PMI: Skip' : 0,\
                    'None Indicated' : 0,\
                    'Asian' : 0,\
                    'Black or African American' : 0,\
                    'Middle Eastern or North African' : 0,\
                    'Native Hawaiian or Other Pacific Islander' : 0,\
                    'White' : 1}

    replace_ethnicity = {'PMI: Prefer Not To Answer' : 0, \
                         'What Race Ethnicity: Race Ethnicity None Of These' : 0,\
                         'PMI: Skip' : 0,\
                         'Not Hispanic or Latino' : 0,\
                         'Hispanic or Latino':1}

    replace_sex = {'Male' : 0, \
                   'Female':1}

    patient_info_sub = patient_info_sub.replace({"race": replace_race,'ethnicity':replace_ethnicity,'sex':replace_sex})



    ### train model
    Xtrain = patient_info_sub[['race','sex','ethnicity','apoe','age']]
    ytrain = patient_info_sub[['disease']]
      
    # building the model and fitting the data
    log_reg = sm.Logit(ytrain, Xtrain).fit()

    ## save results
    out='../logit_model/'+disease_name+'_model_summary.csv'
    with open(out, 'w') as out_f:
        out_f.write(log_reg.summary().as_csv())
        
    ## append results to df
    result=[log_reg.params['apoe'],log_reg.pvalues['apoe'],\
     log_reg.params['sex'],log_reg.pvalues['sex'],\
     log_reg.params['race'],log_reg.pvalues['race'],\
     log_reg.params['age'],log_reg.pvalues['age'],\
     log_reg.params['ethnicity'],log_reg.pvalues['ethnicity'],\
    ]
    save_table.loc[disease_name]=result

save_table.to_csv('../logit_result.csv')
