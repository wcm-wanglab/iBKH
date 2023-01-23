"""
_*_ coding: utf-8 _*_
@ Author: Yang Liu
@ File: find_UMLS.py
@ Time: 11/18/22 2:04 PM
"""

import requests
import pickle
import pandas as pd
from tqdm import tqdm
import os

folder = '/Users/yuhou/Documents/Knowledge_Graph/iBKH/'
apiKey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'

di_vob = pd.read_csv('/Users/yuhou/PycharmProjects/iBKH_UI/dashboard/static/data/Result/entity/disease_vocab.csv')
sy_vob = pd.read_csv('/Users/yuhou/PycharmProjects/iBKH_UI/dashboard/static/data/Result/entity/symptom_vocab.csv')
se_vob = pd.read_csv('/Users/yuhou/PycharmProjects/iBKH_UI/dashboard/static/data/Result/entity/side_effect_vocab.csv')

di_vobUMLS_list = di_vob.dropna(subset=['umls_cui'])['umls_cui'].tolist()
sy_vobUMLS_list = sy_vob.dropna(subset=['umls_cui'])['umls_cui'].tolist()
se_vobUMLS_list = se_vob.dropna(subset=['umls_cui'])['umls_cui'].tolist()


def access_UMLS_by_name(name):
    res_url = 'https://uts-ws.nlm.nih.gov/rest/search/current?string=' + name + '&searchType=normalizedString&apiKey=' + apiKey
    res_resp = requests.get(res_url)
    try:
        # res_content = res_resp.json()['result']['results'][0]
        # umls_cui = res_content['ui']
        umls_cui = res_resp.json()['result']['results'][0]['ui']
        for res_content in res_resp.json()['result']['results']:
            umls_name = res_content['name']
            if umls_name.lower() == name.lower():
                umls_cui = res_content['ui']
    except:
        umls_cui = ''
    return umls_cui


def get_UMLS_name(umls_cui):
    url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '?&apiKey=' + apiKey
    resp = requests.get(url)
    name = ''
    if 'error' not in resp.json():
        if ('name' in resp.json()) and (resp.json()['name'] == 'NotFoundError'):
            name = 'error'
        else:
            content = resp.json()['result']
            name = content['name']

    return name


def match_concept2UMLS():
    # concept_list = [
    #     "amnesia", "Anorexia", "Anxiety", "aphrenia", "apnoea", "Atrial fibrillation", "Cancer",
    #     "Circadian Clock Disruption", "Cognitive decline", "Dementia with Lewy bodies", "Depression", "Dysphagia",
    #     "Elevated blood pressure", "falls", "Frontotemporal dementia", "Hallucinations", "Hearing loss",
    #     "Heart failure", "Hyperglycaemia", "hypertension", "hyperthyroidism", "hypothyroidism", "ischaemia",
    #     "Kidney disease", "memory loss", "Obesity", "Parkinson's disease", "parkinsonism", "Seizures", "Sleep disorder",
    #     "Vascular disease", "fractures", "Atherosclerosis", "Osteoporosis", "Diabetes", "Glaucoma"
    # ]
    disease_list = pd.read_csv('cohort_female_disease.csv')['Unnamed: 0'].tolist()
    res = {}
    for concept_name in tqdm(disease_list):
        umls_cui = access_UMLS_by_name(concept_name)
        umls_name = get_UMLS_name(umls_cui)
        res[concept_name] = [umls_cui, umls_name]

    with open('concept_with_UMLS.obj', 'wb') as f:
        pickle.dump(res, f)
    f.close()


def match_concept2iBKH_nodes():
    with open('concept_with_UMLS.obj', 'rb') as f:
        concept_UMLS = pickle.load(f)
    f.close()

    res = {}
    for concept in concept_UMLS:
        umls_cui, umls_name = concept_UMLS[concept]
        if umls_cui in di_vobUMLS_list:
            idx = di_vob.loc[di_vob['umls_cui'] == umls_cui].index[0]
            primary_id = di_vob.loc[di_vob['umls_cui'] == umls_cui].loc[idx, 'primary']
            name = di_vob.loc[di_vob['umls_cui'] == umls_cui].loc[idx, 'name']
            res[concept] = [primary_id, name, 'Disease', umls_cui]
        elif umls_cui in sy_vobUMLS_list:
            idx = sy_vob.loc[sy_vob['umls_cui'] == umls_cui].index[0]
            primary_id = sy_vob.loc[sy_vob['umls_cui'] == umls_cui].loc[idx, 'primary']
            name = sy_vob.loc[sy_vob['umls_cui'] == umls_cui].loc[idx, 'name']
            res[concept] = [primary_id, name, 'Symptom', umls_cui]
        elif umls_cui in se_vobUMLS_list:
            idx = se_vob.loc[se_vob['umls_cui'] == umls_cui].index[0]
            primary_id = se_vob.loc[se_vob['umls_cui'] == umls_cui].loc[idx, 'primary']
            name = se_vob.loc[se_vob['umls_cui'] == umls_cui].loc[idx, 'name']
            res[concept] = [primary_id, name, 'Side_Effect', umls_cui]
    print(res)
    with open('concept_iBKH.obj', 'wb') as f:
        pickle.dump(res, f)
    f.close()


def test_path(weight_type, pval_filter, topk):
    if weight_type == "LR":
        if pval_filter:
            root_path = os.path.dirname('result/LR/pavl_filter/')
        else:
            root_path = os.path.dirname('result/LR/')
    else:
        root_path = os.path.dirname('result/Norm/')
    if topk is None:
        res_path = root_path
    else:
        res_path = root_path + '/top_' + str(topk) + '/'
    print(res_path)


def main():
    # match_concept2UMLS()
    # match_concept2iBKH_nodes()
    # umls_cui = access_UMLS_by_name("falls")
    # print(umls_cui)
    test_path('LR', True, None)


if __name__ == '__main__':
    main()