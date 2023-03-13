import pandas as pd
import numpy as np
import requests
from lxml.html import fromstring
import string

folder = ''
term_type_list = ['AC', 'BD', 'BN', 'BPCK', 'BR', 'CC', 'CDC', 'CDO', 'CD', 'CMN', 'CN', 'CPR', 'CP', 'CR', 'CSY', 'CV',
                  'CX', 'DC10', 'DC9', 'DE', 'DFG', 'DF', 'DI', 'DP', 'FI', 'FN', 'GLP', 'GN', 'GO', 'GPCK', 'HTJKN1',
                  'HTJKN', 'HTN', 'HT', 'ID', 'IN', 'IVC', 'IV', 'LA', 'LC', 'LG', 'LN', 'LPDN', 'LPN', 'LVDN', 'MD',
                  'MH', 'MIN', 'MS', 'MTH_CN', 'MTH_FN', 'MTH_LN', 'MTH_OAP', 'MTH_OPN', 'MTH_OP', 'MTH_PTGB',
                  'MTH_PTN', 'MTH_PT', 'MTH_RXN_BD', 'MTH_RXN_CDC', 'MTH_RXN_CD', 'MTH_RXN_DP', 'MTH_SI', 'MTH_SMQ',
                  'MV', 'NM', 'OC', 'OPN', 'OP', 'OR', 'OSN', 'PCE', 'PC', 'PEP', 'PHENO', 'PIN', 'PN', 'POS', 'PR',
                  'PSC', 'PSN', 'PTAV', 'PTCS', 'PTGB', 'PTJKN1', 'PTJKN', 'PTN', 'PT', 'PX', 'RPT', 'RXN_IN', 'RXN_PT',
                  'SBDC', 'SBDF', 'SBDG', 'SBD', 'SCDC', 'SCDF', 'SCDG', 'SCD', 'SCN', 'SD', 'SI', 'SMQ', 'SP', 'ST',
                  'SU', 'TA', 'TG', 'TQ', 'UCN', 'USN', 'VPT', 'VS', 'XD']

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
               '/atoms?ttys=IN&ticket=' + st
    resp = requests.get(umls_url)
    umls_cui = ''
    if 'error' not in resp.json():
        content = resp.json()['result'][0]
        umls_cui = content['concept'].replace('https://uts-ws.nlm.nih.gov/rest/content/2020AB/CUI/', '')
    # print(umls_cui)
    return umls_cui


def UMLS2MeSH(tgt, umls_cui):
    st = get_UMLS_ts(tgt)
    mesh_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '/atoms?sabs=MSH&ttys=MH,NM&ticket=' + st
    mesh_resp = requests.get(mesh_url)
    mesh_id = ''
    if 'error' not in mesh_resp.json():
        mesh_content = mesh_resp.json()['result']
        mesh_id = mesh_content[0]['code'].replace(
            'https://uts-ws.nlm.nih.gov/rest/content/2020AB/source/MSH/', '')
    return mesh_id


def UMLS2DrugBank(tgt, umls_cui):
    st = get_UMLS_ts(tgt)
    db_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '/atoms?sabs=DRUGBANK&ttys=IN&ticket=' + st
    db_resp = requests.get(db_url)
    db_id = ''
    if 'error' not in db_resp.json():
        db_content = db_resp.json()['result']
        db_id = db_content[0]['code'].replace(
            'https://uts-ws.nlm.nih.gov/rest/content/2020AB/source/DRUGBANK/', '')
    return db_id


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


def get_UMLS_name(tgt, umls_cui):
    st = get_UMLS_ts(tgt)
    url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '?ticket=' + st
    resp = requests.get(url)
    content = resp.json()['result']
    name = content['name']

    return name


def enrich_DrugBank():
    drugbank_df = pd.read_csv(folder + 'drugbank_res.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    umls_list = []
    mesh_list = []
    for i in range(len(drugbank_df)):
        drugbank_id = drugbank_df.loc[i, 'drugbank_id']
        rxcui_id = drugbank_df.loc[i, 'rxcui_id']
        rxcui_id = str(int(rxcui_id)) if not pd.isnull(rxcui_id) else ''
        name = drugbank_df.loc[i, 'name']
        umls_cui = access_UMLS_CUI(tgt, 'DRUGBANK', drugbank_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI(tgt, 'RXNORM', rxcui_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI_name(tgt, name)
        umls_list.append(umls_cui)
        mesh_id = UMLS2MeSH(tgt, umls_cui)
        mesh_list.append(mesh_id)
        print(i + 1, '/', len(drugbank_df), 'Completed...')
    drugbank_df['umls_cui'] = umls_list
    drugbank_df['mesh_id'] = mesh_list
    drugbank_df.to_csv(folder + 'drugbank_enriched.csv', index=False)


def refine_DrugBank():
    drugbank_df = pd.read_csv(folder + 'drugbank_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    mesh_drug = pd.read_csv(folder + 'mesh_drug.csv')
    mesh_drug['mesh_id'] = mesh_drug['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_drug.set_index('mesh_id')['mesh_term'].to_dict()

    pharmgkb_drug = pd.read_csv(folder + 'pharmgkb_drug_enriched.csv')
    pharmgkb_name_dict = pharmgkb_drug.set_index('pharmgkb_id')['name'].to_dict()

    for i in range(len(drugbank_df)):
        mesh_id = drugbank_df.loc[i, 'mesh_id']
        umls_cui = drugbank_df.loc[i, 'umls_cui']
        pharmgkb_id = drugbank_df.loc[i, 'pharmgkb_id']
        if not pd.isnull(pharmgkb_id):
            temp_df = drugbank_df[drugbank_df['pharmgkb_id'] == pharmgkb_id]
            if len(temp_df) > 1:
                pharmgkb_name = pharmgkb_name_dict[pharmgkb_id]
                temp_pharmgkb_name = pharmgkb_name.lower()
                temp_pharmgkb_name = temp_pharmgkb_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                pharmgkb_name_set = set(filter(None, temp_pharmgkb_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != pharmgkb_name_set:
                        drugbank_df.loc[drugbank_df['name'] == name, 'pharmgkb_id'] = np.nan
                temp_2 = drugbank_df[drugbank_df['pharmgkb_id'] == pharmgkb_id]
                if len(temp_2) == 0:
                    drugbank_df.loc[drugbank_df['name'] == temp_df.iloc[0, 1], 'pharmgkb_id'] = pharmgkb_id
            temp_df_2 = drugbank_df[drugbank_df['pharmgkb_id'] == pharmgkb_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    drugbank_df.loc[drugbank_df['drugbank_id'] == temp_primary, 'pharmgkb_id'] = np.nan
        if not pd.isnull(mesh_id):
            temp_df = drugbank_df[drugbank_df['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id]
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
                        drugbank_df.loc[drugbank_df['name'] == name, 'mesh_id'] = np.nan
                temp_2 = drugbank_df[drugbank_df['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    drugbank_df.loc[drugbank_df['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = drugbank_df[drugbank_df['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    drugbank_df.loc[drugbank_df['drugbank_id'] == temp_primary, 'mesh_id'] = np.nan
        if not pd.isnull(umls_cui):
            temp_df = drugbank_df[drugbank_df['umls_cui'] == umls_cui]
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
                        drugbank_df.loc[drugbank_df['name'] == name, 'umls_cui'] = np.nan
                temp_2 = drugbank_df[drugbank_df['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    drugbank_df.loc[drugbank_df['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = drugbank_df[drugbank_df['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    drugbank_df.loc[drugbank_df['drugbank_id'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(drugbank_df), 'Completed...')
    drugbank_df.to_csv(folder + 'drugbank_enriched_refined.csv', index=False)


def enrich_KEGG():
    kegg_df = pd.read_csv(folder + 'kegg_drug.csv')
    kegg_df = kegg_df.dropna(subset=['name'])
    kegg_df = kegg_df.reset_index(drop=True)
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    umls_list = []
    mesh_list = []
    for i in range(len(kegg_df)):
        names = kegg_df.loc[i, 'name']
        drugbank_id = kegg_df.loc[i, 'drugbank_id']
        name = names.split('; ')[0]
        name = name[:name.find(' (')]
        umls_cui = ''
        if not pd.isnull(drugbank_id):
            umls_cui = access_UMLS_CUI(tgt, 'DRUGBANK', drugbank_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI_name(tgt, name)
        umls_list.append(umls_cui)
        mesh_id = UMLS2MeSH(tgt, umls_cui)
        mesh_list.append(mesh_id)
        print(i + 1, '/', len(kegg_df), 'Completed...')
    kegg_df['umls_cui'] = umls_list
    kegg_df['mesh_id'] = mesh_list
    kegg_df.to_csv(folder + 'kegg_drug_enriched.csv', index=False)


def refine_KEGG():
    kegg_df = pd.read_csv(folder + 'kegg_drug_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    mesh_drug = pd.read_csv(folder + 'mesh_drug.csv')
    mesh_drug['mesh_id'] = mesh_drug['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_drug.set_index('mesh_id')['mesh_term'].to_dict()

    drugbank = pd.read_csv(folder + 'drugbank_enriched_refined.csv')
    drugbank_name_dict = drugbank.set_index('drugbank_id')['name'].to_dict()

    for i in range(len(kegg_df)):
        drugbank_id = kegg_df.loc[i, 'drugbank_id']
        mesh_id = kegg_df.loc[i, 'mesh_id']
        umls_cui = kegg_df.loc[i, 'umls_cui']

        if not pd.isnull(drugbank_id):
            temp_df = kegg_df[kegg_df['drugbank_id'] == drugbank_id]
            if len(temp_df) > 1:
                drugbank_name = drugbank_name_dict[drugbank_id] if drugbank_id in drugbank_name_dict else ''
                temp_drugbank_name = drugbank_name.lower()
                temp_drugbank_name = temp_drugbank_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                drugbank_name_set = set(filter(None, temp_drugbank_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != drugbank_name_set:
                        kegg_df.loc[kegg_df['name'] == name, 'drugbank_id'] = np.nan
                temp_2 = kegg_df[kegg_df['drugbank_id'] == drugbank_id]
                if len(temp_2) == 0:
                    kegg_df.loc[kegg_df['name'] == temp_df.iloc[0, 1], 'drugbank_id'] = drugbank_id
            temp_df_2 = kegg_df[kegg_df['drugbank_id'] == drugbank_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    kegg_df.loc[kegg_df['kegg_id'] == temp_primary, 'drugbank_id'] = np.nan

        if not pd.isnull(mesh_id):
            temp_df = kegg_df[kegg_df['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id]
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
    kegg_df.to_csv(folder + 'kegg_drug_enriched_refined.csv', index=False)


def enrich_pharmgkb():
    pharmgkb_df = pd.read_csv(folder + 'pharmgkb_drug_res.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    umls_list = []
    mesh_list = []
    for i in range(len(pharmgkb_df)):
        drugbank_id = pharmgkb_df.loc[i, 'drugbank_id']
        drugbank_id = drugbank_id if not pd.isnull(drugbank_id) else ''
        rxcui_id = pharmgkb_df.loc[i, 'rxcui_id']
        if not pd.isnull(rxcui_id):
            if isinstance(rxcui_id, str):
                rxcui_id = rxcui_id
            else:
                rxcui_id = str(int(rxcui_id))
        else:
            rxcui_id = ''
        atc_id = pharmgkb_df.loc[i, 'atc_id']
        atc_id = atc_id if not pd.isnull(atc_id) else ''
        name = pharmgkb_df.loc[i, 'name']
        umls_cui = access_UMLS_CUI(tgt, 'DRUGBANK', drugbank_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI(tgt, 'RXNORM', rxcui_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI(tgt, 'ATC', atc_id)
        if umls_cui == '':
            umls_cui = access_UMLS_CUI_name(tgt, name)
        umls_list.append(umls_cui)
        mesh_id = UMLS2MeSH(tgt, umls_cui)
        mesh_list.append(mesh_id)
        print(i + 1, '/', len(pharmgkb_df), 'Completed...')
    pharmgkb_df['umls_cui'] = umls_list
    pharmgkb_df['mesh_id'] = mesh_list
    print(pharmgkb_df)
    pharmgkb_df.to_csv(folder + 'pharmgkb_drug_enriched.csv', index=False)


def refine_PharmGKB():
    pharmgkb_df = pd.read_csv(folder + 'pharmgkb_drug_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    mesh_drug = pd.read_csv(folder + 'mesh_drug.csv')
    mesh_drug['mesh_id'] = mesh_drug['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_drug.set_index('mesh_id')['mesh_term'].to_dict()

    for i in range(len(pharmgkb_df)):
        mesh_id = pharmgkb_df.loc[i, 'mesh_id']
        umls_cui = pharmgkb_df.loc[i, 'umls_cui']
        if not pd.isnull(mesh_id):
            temp_df = pharmgkb_df[pharmgkb_df['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id]
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
    pharmgkb_df.to_csv(folder + 'pharmgkb_drug_enriched_refined.csv', index=False)


def refine_CTD_drug():
    CTD_drug = pd.read_csv(folder + 'CTD_drugs_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(CTD_drug)):
        umls_cui = CTD_drug.loc[i, 'umls_cui']

        if not pd.isnull(umls_cui):
            temp_df = CTD_drug[CTD_drug['umls_cui'] == umls_cui]
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
                        CTD_drug.loc[CTD_drug['name'] == name, 'umls_cui'] = np.nan
                temp_2 = CTD_drug[CTD_drug['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    CTD_drug.loc[CTD_drug['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = CTD_drug[CTD_drug['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    CTD_drug.loc[CTD_drug['mesh_id'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(CTD_drug), 'Completed...')
    CTD_drug.to_csv(folder + 'CTD_drug_enriched_refined.csv', index=False)


def refine_SIDER_drug():
    SIDER_drug = pd.read_csv(folder + 'drug_SIDER_enriched.csv')
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    mesh_drug = pd.read_csv(folder + 'mesh_drug.csv')
    mesh_drug['mesh_id'] = mesh_drug['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_drug.set_index('mesh_id')['mesh_term'].to_dict()

    drugbank = pd.read_csv(folder + 'drugbank_enriched_refined.csv')
    drugbank_name_dict = drugbank.set_index('drugbank_id')['name'].to_dict()

    for i in range(len(SIDER_drug)):
        drugbank_id = SIDER_drug.loc[i, 'drugbank_id']
        mesh_id = SIDER_drug.loc[i, 'mesh_id']
        umls_cui = SIDER_drug.loc[i, 'umls_cui']

        if not pd.isnull(drugbank_id):
            temp_df = SIDER_drug[SIDER_drug['drugbank_id'] == drugbank_id]
            if len(temp_df) > 1:
                drugbank_name = drugbank_name_dict[drugbank_id] if drugbank_id in drugbank_name_dict else ''
                temp_drugbank_name = drugbank_name.lower()
                temp_drugbank_name = temp_drugbank_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                drugbank_name_set = set(filter(None, temp_drugbank_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != drugbank_name_set:
                        SIDER_drug.loc[SIDER_drug['name'] == name, 'drugbank_id'] = np.nan
                temp_2 = SIDER_drug[SIDER_drug['drugbank_id'] == drugbank_id]
                if len(temp_2) == 0:
                    SIDER_drug.loc[SIDER_drug['name'] == temp_df.iloc[0, 1], 'drugbank_id'] = drugbank_id
            temp_df_2 = SIDER_drug[SIDER_drug['drugbank_id'] == drugbank_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    SIDER_drug.loc[SIDER_drug['CID'] == temp_primary, 'drugbank_id'] = np.nan

        if not pd.isnull(mesh_id):
            temp_df = SIDER_drug[SIDER_drug['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                mesh_term = mesh_name_dict[mesh_id]
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
                        SIDER_drug.loc[SIDER_drug['name'] == name, 'mesh_id'] = np.nan
                temp_2 = SIDER_drug[SIDER_drug['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    SIDER_drug.loc[SIDER_drug['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = SIDER_drug[SIDER_drug['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    SIDER_drug.loc[SIDER_drug['CID'] == temp_primary, 'mesh_id'] = np.nan

        if not pd.isnull(umls_cui):
            temp_df = SIDER_drug[SIDER_drug['umls_cui'] == umls_cui]
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
                        SIDER_drug.loc[SIDER_drug['name'] == name, 'umls_cui'] = np.nan
                temp_2 = SIDER_drug[SIDER_drug['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    SIDER_drug.loc[SIDER_drug['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = SIDER_drug[SIDER_drug['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    SIDER_drug.loc[SIDER_drug['CID'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(SIDER_drug), 'Completed...')
    SIDER_drug.to_csv(folder + 'drug_SIDER_enriched_refined.csv', index=False)


def refine_res():
    drug_vocab = pd.read_csv(folder + 'drug_vocab_refined_2.csv')
    mesh_drug = pd.read_csv(folder + 'mesh_drug.csv')
    mesh_drug['mesh_id'] = mesh_drug['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_drug.set_index('mesh_id')['mesh_term'].to_dict()

    for i in range(len(drug_vocab)):
        mesh_id = drug_vocab.loc[i, 'mesh_id']
        if not pd.isnull(mesh_id):
            temp_df = drug_vocab[drug_vocab['mesh_id'] == mesh_id]
            if len(temp_df) > 1:
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    mesh_term = mesh_name_dict[mesh_id]
                    temp_mesh_term = mesh_term.lower()
                    temp_mesh_term = temp_mesh_term.translate(
                        str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    mesh_term_set = set(filter(None, temp_mesh_term.split(' ')))
                    if name_set != mesh_term_set:
                        drug_vocab.loc[drug_vocab['name'] == name, 'mesh_id'] = np.nan
                temp_2 = drug_vocab[drug_vocab['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    drug_vocab.loc[drug_vocab['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = drug_vocab[drug_vocab['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    drug_vocab.loc[drug_vocab['primary'] == temp_primary, 'mesh_id'] = np.nan
        print(i + 1, '/', len(drug_vocab), 'Completed...')
    drug_vocab.to_csv(folder + 'drug_vocab_refined_3.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('drug_vocab_refined_3.csv: Remove the duplicated MeSH ID in drug_vocab_refined_2.\n')
    f.close()


def mesh2umls(tgt, mesh_id):
    st = get_UMLS_ts(tgt)
    mesh_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/source/MSH/' + mesh_id + '/atoms?ttys=MH,NM&ticket=' + st
    mesh_resp = requests.get(mesh_url)
    umls_cui = ''
    if 'error' not in mesh_resp.json():
        mesh_content = mesh_resp.json()['result'][0]
        umls_cui = mesh_content['concept'].replace('https://uts-ws.nlm.nih.gov/rest/content/2020AB/CUI/', '')

    return umls_cui


def enrich_SIDER_drug():
    drug_SIDER = pd.read_table(folder + 'drug_names.tsv', header=None)
    drug_SIDER = drug_SIDER.rename(columns={0: 'CID', 1: 'name'})
    print(drug_SIDER)
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    res = pd.DataFrame(columns=['CID', 'name', 'drugbank_id', 'umls_cui', 'mesh_id'])
    idx = 0
    for i in range(len(drug_SIDER)):
        cid = drug_SIDER.loc[i, 'CID']
        name = drug_SIDER.loc[i, 'name']
        umls_cui = access_UMLS_CUI_name(tgt, name)
        drugbank_id = UMLS2DrugBank(tgt, umls_cui)
        mesh_id = UMLS2MeSH(tgt, umls_cui)
        res.loc[idx] = [cid, name, drugbank_id, umls_cui, mesh_id]
        idx += 1
        print(i + 1, '/', len(drug_SIDER), 'Completed...')
    res.to_csv(folder + 'drug_SIDER_enriched.csv', index=False)


def main():
    # enrich_DrugBank()
    # enrich_KEGG()
    # enrich_pharmgkb()
    # enrich_SIDER_drug()

    # refine_DrugBank()
    # refine_KEGG()
    # refine_PharmGKB()
    # refine_CTD_drug()
    # refine_SIDER_drug()
    # refine_res()

    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)
    # mesh_id = UMLS2MeSH(tgt, 'C0021735')
    # print(mesh_id)
    # get_UMLS_name(tgt, 'C0935989')
    # umls_cui = access_UMLS_CUI(tgt, 'DRUGBANK', 'DB01536')
    # name = 'baclofen'
    # umls_cui = access_UMLS_CUI_name(tgt, name)
    umls_cui = mesh2umls(tgt, 'C063419')
    # db_id = UMLS2DrugBank(tgt, 'C0023413')
    print(umls_cui)

    # umls_ttys = pd.read_excel(folder + 'umls_ttys.xlsx')
    # umls_ttys_synonym = umls_ttys[umls_ttys['TTY (tty_class) Description'] == 'synonym']
    # print(list(umls_ttys_synonym['TTY (tty_class)']))


if __name__ == '__main__':
    main()
