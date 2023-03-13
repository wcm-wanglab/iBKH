"""
_*_ coding: utf-8 _*_
@ Author: Yu Hou
@File: refine_disease.py
@Time: 4/16/21 9:32 AM
"""

import pandas as pd
import numpy as np
import requests
from lxml.html import fromstring
import string

# folder = ''
folder = '/Users/yuhou/Documents/Knowledge_Graph/knowledge_bases_integration/v2_res_Apr2021_refine/disease/'
term_type_list = ['AC', 'BD', 'BN', 'BPCK', 'BR', 'CC', 'CDC', 'CDO', 'CD', 'CMN', 'CN', 'CPR', 'CP', 'CR', 'CSY', 'CV',
                  'CX', 'DC10', 'DC9', 'DE', 'DFG', 'DF', 'DI', 'DP', 'FI', 'FN', 'GLP', 'GN', 'GO', 'GPCK', 'HTJKN1',
                  'HTJKN', 'HTN', 'HT', 'ID', 'IN', 'IVC', 'IV', 'LA', 'LC', 'LG', 'LN', 'LPDN', 'LPN', 'LVDN', 'MD',
                  'MH', 'MIN', 'MS', 'MTH_CN', 'MTH_FN', 'MTH_LN', 'MTH_OAP', 'MTH_OPN', 'MTH_OP', 'MTH_PTGB',
                  'MTH_PTN', 'MTH_PT', 'MTH_RXN_BD', 'MTH_RXN_CDC', 'MTH_RXN_CD', 'MTH_RXN_DP', 'MTH_SI', 'MTH_SMQ',
                  'MV', 'NM', 'OC', 'OPN', 'OP', 'OR', 'OSN', 'PCE', 'PC', 'PEP', 'PHENO', 'PIN', 'PN', 'POS', 'PR',
                  'PSC', 'PSN', 'PTAV', 'PTCS', 'PTGB', 'PTJKN1', 'PTJKN', 'PTN', 'PT', 'PX', 'RPT', 'RXN_IN', 'RXN_PT',
                  'SBDC', 'SBDF', 'SBDG', 'SBD', 'SCDC', 'SCDF', 'SCDG', 'SCD', 'SCN', 'SD', 'SI', 'SMQ', 'SP', 'ST',
                  'SU', 'TA', 'TG', 'TQ', 'UCN', 'USN', 'VPT', 'VS', 'XD']
                  # 'AS', 'AUN', 'CCN', 'CCS', 'CDD', 'CHN', 'CSN', 'CU', 'DN', 'EQ', 'FBD', 'FSY', 'IS', 'LV', 'MTH_IS',
                  # 'MTH_LO', 'MTH_OAS', 'MTH_SYGB', 'MTH_SY', 'N1', 'NPT', 'NP', 'NS', 'NX', 'ONP', 'PM', 'RSY', 'SS',
                  # 'SX', 'SYGB', 'SYN', 'SY', 'TMSY', 'UAUN', 'UE', 'USY', 'VSY', 'XQ']


def get_UMLS_tgt(apikey):
    uri = "https://utslogin.nlm.nih.gov"
    auth_endpoint = "/cas/v1/api-key"
    params = {'apikey': apikey}
    h = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": "python"}
    r = requests.post(uri + auth_endpoint, data=params, headers=h)
    response = fromstring(r.text)
    tgt = response.xpath('//form/@action')[0]
    return tgt


def get_UMLS_ts(tgt):
    service = "http://umlsks.nlm.nih.gov"
    params = {'service': service}
    h = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": "python"}
    r = requests.post(tgt, data=params, headers=h)
    st = r.text
    return st


def access_UMLS_CUI(tgt, id_type, entity_id):
    st = get_UMLS_ts(tgt)
    umls_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/source/' + id_type + '/' + entity_id + \
               '/atoms?ttys=MH,NM,PT&ticket=' + st
    resp = requests.get(umls_url)
    umls_cui = ''
    if 'error' not in resp.json():
        content = resp.json()['result'][0]
        umls_cui = content['concept'].replace('https://uts-ws.nlm.nih.gov/rest/content/2020AB/CUI/', '')
    # print(umls_cui)
    return umls_cui


def UMLS2MeSH(tgt, umls_cui):
    st = get_UMLS_ts(tgt)
    mesh_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '/atoms?sabs=MSH&ttys=MH,NM,PT&ticket=' + st
    mesh_resp = requests.get(mesh_url)
    mesh_id = ''
    if 'error' not in mesh_resp.json():
        mesh_content = mesh_resp.json()['result']
        mesh_id = mesh_content[0]['code'].replace(
            'https://uts-ws.nlm.nih.gov/rest/content/2020AB/source/MSH/', '')
    return mesh_id


def get_UMLS_name(tgt, umls_cui):
    st = get_UMLS_ts(tgt)
    url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '?ticket=' + st
    resp = requests.get(url)
    name = ''
    if 'error' not in resp.json():
        content = resp.json()['result']
        name = content['name']

    return name


def access_UMLS_CUI_name(tgt, name):
    name = name.lower()
    name = name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
    name_set = set(filter(None, name.split(' ')))
    if 'and' in name_set:
        name_set.remove('and')
    st = get_UMLS_ts(tgt)
    db_url = 'https://uts-ws.nlm.nih.gov/rest/search/current?string=' + name + '&ticket=' + st
    db_resp = requests.get(db_url)
    db_content_list = db_resp.json()['result']['results']
    res_umls = ''
    exact_match = False
    for db_content in db_content_list:
        umls_cui = db_content['ui']
        umls_name = db_content['name'].lower()
        umls_name = umls_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
        umls_name_set = set(filter(None, umls_name.split(' ')))
        if umls_name_set == name_set:
            res_umls = umls_cui
            exact_match = True
    if res_umls == '':
        res_umls = db_content_list[0]['ui']
    res_umls = res_umls if res_umls != 'NONE' else ''
    # print(res_umls, res_umls_name, exact_match)
    if not exact_match:
        st = get_UMLS_ts(tgt)
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + res_umls + '/atoms?ticket=' + st
        resp = requests.get(url)
        if 'error' not in resp.json():
            pageCount = int(resp.json()['pageCount'])
            for page in range(1, pageCount + 1):
                st = get_UMLS_ts(tgt)
                page_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + res_umls + '/atoms?pageNumber=' + str(
                    page) + '&ticket=' + st
                page_resp = requests.get(page_url)
                content = page_resp.json()['result']
                for res in content:
                    if res['termType'] in term_type_list:
                        disease_name = res['name'].lower().replace('to ', '').translate(
                            str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                        disease_name_set = set(filter(None, disease_name.split(' ')))
                        if 'and' in disease_name_set:
                            disease_name_set.remove('and')
                        exact_match = name_set == disease_name_set
                        if exact_match:
                            break
                if exact_match:
                    break
    # print(res_umls, res_umls_name, exact_match)
    return res_umls if exact_match else ''


def enrich_DO():
    do_df = pd.read_csv(folder + 'do.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(do_df)):
        umls_cui = do_df.loc[i, 'umls_cui']
        mesh_id = do_df.loc[i, 'mesh_id']
        if pd.isnull(umls_cui):
            icd_10 = do_df.loc[i, 'icd_10']
            icd_10 = str(icd_10) if not pd.isnull(icd_10) else ''
            icd_9 = do_df.loc[i, 'icd_9']
            icd_9 = str(icd_9) if not pd.isnull(icd_9) else ''
            snomedct_id = do_df.loc[i, 'snomedct_id']
            snomedct_id = str(snomedct_id) if not pd.isnull(snomedct_id) else ''
            name = do_df.loc[i, 'disease_name']
            umls_cui = access_UMLS_CUI(tgt, 'ICD10CM', icd_10)
            if umls_cui == '':
                umls_cui = access_UMLS_CUI(tgt, 'ICD9CM', icd_9)
            if umls_cui == '':
                umls_cui = access_UMLS_CUI(tgt, 'SNOMEDCT_US', snomedct_id)
            if umls_cui == '':
                umls_cui = access_UMLS_CUI_name(tgt, name)
            do_df.loc[i, 'umls_cui'] = umls_cui
        if not pd.isnull(umls_cui) and pd.isnull(mesh_id):
            mesh_id = UMLS2MeSH(tgt, umls_cui)
            do_df.loc[i, 'mesh_id'] = mesh_id
        print(i + 1, '/', len(do_df), 'Completed...')
    # print(do_df[['doid', 'umls_cui', 'mesh_id']])
    do_df.to_csv(folder + 'do_enriched.csv', index=False)


def refine_DO():
    do_df = pd.read_csv(folder + 'do_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    mesh_disease = pd.read_csv(folder + 'mesh_disease.csv')
    mesh_disease['mesh_id'] = mesh_disease['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_disease.set_index('mesh_id')['mesh_term'].to_dict()

    for i in range(len(do_df)):
        mesh_id = do_df.loc[i, 'mesh_id']
        umls_cui = do_df.loc[i, 'umls_cui']
        if not pd.isnull(mesh_id):
            temp_df = do_df[do_df['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id] if mesh_id in mesh_name_dict else ''
                temp_mesh_term = mesh_term.lower()
                temp_mesh_term = temp_mesh_term.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                mesh_term_set = set(filter(None, temp_mesh_term.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != mesh_term_set:
                        do_df.loc[do_df['disease_name'] == name, 'mesh_id'] = np.nan
                temp_2 = do_df[do_df['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    do_df.loc[do_df['disease_name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = do_df[do_df['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    do_df.loc[do_df['doid'] == temp_primary, 'mesh_id'] = np.nan

        if not pd.isnull(umls_cui):
            temp_df = do_df[do_df['umls_cui'] == umls_cui]
            if len(temp_df) > 1:
                umls_name = get_UMLS_name(tgt, umls_cui)
                temp_umls_name = umls_name.lower()
                temp_umls_name = temp_umls_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                umls_name_set = set(filter(None, temp_umls_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != umls_name_set:
                        do_df.loc[do_df['disease_name'] == name, 'umls_cui'] = np.nan
                temp_2 = do_df[do_df['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    do_df.loc[do_df['disease_name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = do_df[do_df['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    do_df.loc[do_df['doid'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(do_df), 'Completed...')
    do_df.to_csv(folder + 'do_enriched_refined.csv', index=False)


def enrich_KEGG():
    kegg_df = pd.read_csv(folder + 'kegg_disease.csv')
    kegg_df = kegg_df.dropna(subset=['name'])
    kegg_df = kegg_df.reset_index(drop=True)
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    umls_list = []
    for i in range(len(kegg_df)):
        names = kegg_df.loc[i, 'name']
        mesh_id = kegg_df.loc[i, 'mesh_id']
        icd_10 = kegg_df.loc[i, 'icd_10']
        icd_10 = icd_10.split(' ')[0] if not pd.isnull(icd_10) else ''
        name = names.split('; ')[0]
        name = name[:name.find(' (')]
        umls_cui = ''
        if not pd.isnull(mesh_id):
            umls_cui = access_UMLS_CUI(tgt, 'MSH', mesh_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI(tgt, 'ICD10CM', icd_10)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI_name(tgt, name)
        umls_list.append(umls_cui)
        if umls_cui != '' and pd.isnull(mesh_id):
            mesh_id = UMLS2MeSH(tgt, umls_cui)
            kegg_df.loc[i, 'mesh_id'] = mesh_id
        print(i + 1, '/', len(kegg_df), 'Completed...')
    kegg_df['umls_cui'] = umls_list
    print(kegg_df[['kegg_id', 'mesh_id', 'umls_cui']])
    kegg_df.to_csv(folder + 'kegg_disease_enriched.csv', index=False)


def refine_KEGG():
    kegg_df = pd.read_csv(folder + 'kegg_disease_enriched.csv')
    mesh_disease = pd.read_csv(folder + 'mesh_disease.csv')
    mesh_disease['mesh_id'] = mesh_disease['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_disease.set_index('mesh_id')['mesh_term'].to_dict()

    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(kegg_df)):
        mesh_id = kegg_df.loc[i, 'mesh_id']
        umls_cui = kegg_df.loc[i, 'umls_cui']

        if not pd.isnull(mesh_id):
            temp_df = kegg_df[kegg_df['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id] if mesh_id in mesh_name_dict else ''
                temp_mesh_term = mesh_term.lower()
                temp_mesh_term = temp_mesh_term.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                mesh_term_set = set(filter(None, temp_mesh_term.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != mesh_term_set:
                        kegg_df.loc[kegg_df['name'] == name, 'mesh_id'] = np.nan
                temp_2 = kegg_df[kegg_df['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    kegg_df.loc[kegg_df['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = kegg_df[kegg_df['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    kegg_df.loc[kegg_df['kegg_id'] == temp_primary, 'mesh_id'] = np.nan

        if not pd.isnull(umls_cui):
            temp_df = kegg_df[kegg_df['umls_cui'] == umls_cui]
            if len(temp_df) > 1:
                umls_name = get_UMLS_name(tgt, umls_cui)
                temp_umls_name = umls_name.lower()
                temp_umls_name = temp_umls_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                umls_name_set = set(filter(None, temp_umls_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != umls_name_set:
                        kegg_df.loc[kegg_df['name'] == name, 'umls_cui'] = np.nan
                temp_2 = kegg_df[kegg_df['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    kegg_df.loc[kegg_df['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = kegg_df[kegg_df['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    kegg_df.loc[kegg_df['kegg_id'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(kegg_df), 'Completed...')
    kegg_df.to_csv(folder + 'kegg_disease_enriched_refined.csv', index=False)


def enrich_PharmGKB():
    pharmgkb_df = pd.read_csv(folder + 'pharmgkb_disease_res.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(pharmgkb_df)):
        mesh_id = pharmgkb_df.loc[i, 'mesh_id']
        umls_cui = pharmgkb_df.loc[i, 'umls_cui']

        if pd.isnull(umls_cui):
            snomedct_id = pharmgkb_df.loc[i, 'snomedct_id']
            snomedct_id = str(snomedct_id) if not pd.isnull(snomedct_id) else ''
            name = pharmgkb_df.loc[i, 'name']
            umls_cui = access_UMLS_CUI(tgt, 'SNOMEDCT_US', snomedct_id)
            if umls_cui == '':
                umls_cui = access_UMLS_CUI_name(tgt, name)
            pharmgkb_df.loc[i, 'umls_cui'] = umls_cui
        if not pd.isnull(umls_cui) and pd.isnull(mesh_id):
            mesh_id = UMLS2MeSH(tgt, umls_cui)
            pharmgkb_df.loc[i, 'mesh_id'] = mesh_id
        print(i + 1, '/', len(pharmgkb_df), 'Completed...')

    pharmgkb_df.to_csv(folder + 'pharmgkb_disease_enriched.csv', index=False)


def refine_PharmGKB():
    pharmgkb_df = pd.read_csv(folder + 'pharmgkb_disease_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    mesh_disease = pd.read_csv(folder + 'mesh_disease.csv')
    mesh_disease['mesh_id'] = mesh_disease['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_disease.set_index('mesh_id')['mesh_term'].to_dict()

    for i in range(len(pharmgkb_df)):
        mesh_id = pharmgkb_df.loc[i, 'mesh_id']
        umls_cui = pharmgkb_df.loc[i, 'umls_cui']
        if not pd.isnull(mesh_id):
            temp_df = pharmgkb_df[pharmgkb_df['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id] if mesh_id in mesh_name_dict else ''
                temp_mesh_term = mesh_term.lower()
                temp_mesh_term = temp_mesh_term.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                mesh_term_set = set(filter(None, temp_mesh_term.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != mesh_term_set:
                        pharmgkb_df.loc[pharmgkb_df['name'] == name, 'mesh_id'] = np.nan
                temp_2 = pharmgkb_df[pharmgkb_df['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    pharmgkb_df.loc[pharmgkb_df['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = pharmgkb_df[pharmgkb_df['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    pharmgkb_df.loc[pharmgkb_df['pharmgkb_id'] == temp_primary, 'mesh_id'] = np.nan
        if not pd.isnull(umls_cui):
            temp_df = pharmgkb_df[pharmgkb_df['umls_cui'] == umls_cui]
            if len(temp_df) > 1:
                umls_name = get_UMLS_name(tgt, umls_cui)
                temp_umls_name = umls_name.lower()
                temp_umls_name = temp_umls_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                umls_name_set = set(filter(None, temp_umls_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != umls_name_set:
                        pharmgkb_df.loc[pharmgkb_df['name'] == name, 'umls_cui'] = np.nan
                temp_2 = pharmgkb_df[pharmgkb_df['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    pharmgkb_df.loc[pharmgkb_df['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = pharmgkb_df[pharmgkb_df['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    pharmgkb_df.loc[pharmgkb_df['pharmgkb_id'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(pharmgkb_df), 'Completed...')
    pharmgkb_df.to_csv(folder + 'pharmgkb_disease_enriched_refined.csv', index=False)


def refine_CTD_disease():
    CTD_disease = pd.read_csv(folder + 'CTD_disease_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(CTD_disease)):
        disease_id = CTD_disease.loc[i, 'disease_id']
        mesh_id = CTD_disease.loc[i, 'mesh_id']
        umls_cui = CTD_disease.loc[i, 'umls_cui']
        if 'OMIM' in disease_id:
            if not pd.isnull(mesh_id):
                temp_df = CTD_disease[CTD_disease['mesh_id'] == mesh_id]
                if len(temp_df) > 1:
                    for j in range(len(temp_df)):
                        temp_primary = temp_df.iloc[j, 0]
                        if 'OMIM' in temp_primary:
                            CTD_disease.loc[CTD_disease['disease_id'] == temp_primary, 'mesh_id'] = np.nan
            if not pd.isnull(umls_cui):
                temp_df = CTD_disease[CTD_disease['umls_cui'] == umls_cui]
                if len(temp_df) > 1:
                    for j in range(len(temp_df)):
                        temp_primary = temp_df.iloc[j, 0]
                        if 'OMIM' in temp_primary:
                            CTD_disease.loc[CTD_disease['disease_id'] == temp_primary, 'umls_cui'] = np.nan

    CTD_disease.to_csv(folder + 'CTD_disease_enriched_refined.csv', index=False)


def enrich_DRKG_DDi():
    drkg_DDi = pd.read_csv('/Users/yuhou/Documents/Knowledge_Graph/knowledge_bases_integration/stage_2/drkg_DDi.csv')
    drkg_DDi_disease = drkg_DDi.drop_duplicates(subset='entity_2', keep='first')
    drkg_DDi_disease['entity_2'] = drkg_DDi_disease['entity_2'].str.replace('Disease::', '')
    drkg_DDi_disease = drkg_DDi_disease.reset_index(drop=True)[['entity_2']]
    print(drkg_DDi_disease)


def main():
    # enrich_DO()
    # enrich_KEGG()
    # enrich_PharmGKB()
    enrich_DRKG_DDi()

    # refine_DO()
    # refine_KEGG()
    # refine_PharmGKB()
    # refine_CTD_disease()

    # apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    # tgt = get_UMLS_tgt(apikey)
    # umls_cui = access_UMLS_CUI(tgt, 'ICD10CM', 'C83.8')
    # # name = 'Water'
    # # umls_cui = access_UMLS_CUI_name(tgt, name)
    # print(umls_cui)
    # umls_name = get_UMLS_name(tgt, 'C0265318')
    # print(umls_name)


if __name__ == '__main__':
    main()