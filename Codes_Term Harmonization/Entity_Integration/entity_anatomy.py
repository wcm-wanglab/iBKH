import pandas as pd
import numpy as np
import string
import requests
from lxml.html import fromstring

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


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


def get_UMLS_name(tgt, umls_cui):
    st = get_UMLS_ts(tgt)
    url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + umls_cui + '?ticket=' + st
    resp = requests.get(url)
    name = ''
    if 'error' not in resp.json():
        content = resp.json()['result']
        name = content['name']

    return name


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


def refine_res_2():
    anatomy_res = pd.read_csv('anatomy_res_2.csv')
    anatomy_res = anatomy_res[['primary', 'name', 'uberon_id', 'bto_id', 'mesh_id', 'umls_cui']]

    anatomy_res['mesh_id'] = anatomy_res['mesh_id'].str.replace('MESH:', '')
    anatomy_res['umls_cui'] = anatomy_res['umls_cui'].str.replace('UMLS:', '')

    mesh_anatomy = pd.read_csv('anatomy_mesh.csv')
    mesh_anatomy['mesh_id'] = mesh_anatomy['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_anatomy.set_index('mesh_id')['mesh_term'].to_dict()

    bto = pd.read_csv('bto.csv')
    bto_name_dict = bto.set_index('bto_id')['name'].to_dict()

    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(anatomy_res)):
        mesh_id = anatomy_res.loc[i, 'mesh_id']
        umls_cui = anatomy_res.loc[i, 'umls_cui']
        bto_id = anatomy_res.loc[i, 'bto_id']

        if not pd.isnull(bto_id):
            temp_df = anatomy_res[anatomy_res['bto_id'] == bto_id]
            if len(temp_df) > 1:
                bto_name = bto_name_dict[bto_id]
                temp_bto_name = bto_name.lower()
                temp_bto_name = temp_bto_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                bto_name_set = set(filter(None, temp_bto_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != bto_name_set:
                        anatomy_res.loc[anatomy_res['name'] == name, 'bto_id'] = np.nan
                temp_2 = anatomy_res[anatomy_res['bto_id'] == bto_id]
                if len(temp_2) == 0:
                    anatomy_res.loc[anatomy_res['name'] == temp_df.iloc[0, 1], 'bto_id'] = bto_id
            temp_df_2 = anatomy_res[anatomy_res['bto_id'] == bto_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    anatomy_res.loc[anatomy_res['primary'] == temp_primary, 'bto_id'] = np.nan

        if not pd.isnull(mesh_id):
            temp_df = anatomy_res[anatomy_res['mesh_id'] == mesh_id]
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
                        anatomy_res.loc[anatomy_res['name'] == name, 'mesh_id'] = np.nan
                temp_2 = anatomy_res[anatomy_res['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    anatomy_res.loc[anatomy_res['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = anatomy_res[anatomy_res['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    anatomy_res.loc[anatomy_res['primary'] == temp_primary, 'mesh_id'] = np.nan

        if not pd.isnull(umls_cui):
            temp_df = anatomy_res[anatomy_res['umls_cui'] == umls_cui]
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
                        anatomy_res.loc[anatomy_res['name'] == name, 'umls_cui'] = np.nan
                temp_2 = anatomy_res[anatomy_res['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    anatomy_res.loc[anatomy_res['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = anatomy_res[anatomy_res['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    anatomy_res.loc[anatomy_res['primary'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(anatomy_res), 'Completed...')
    anatomy_res.to_csv('anatomy_res_2_refined.csv', index=False)


def enriche_CL():
    cl_df = pd.read_csv('cl.csv')

    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(cl_df)):
        fma_id = cl_df.loc[i, 'fma']
        mesh_id = cl_df.loc[i, 'mesh_id']
        umls_cui = cl_df.loc[i, 'umls_cui']
        name = cl_df.loc[i, 'name']

        if pd.isnull(umls_cui):
            if not pd.isnull(fma_id):
                temp_umls = access_UMLS_CUI(tgt, 'FMA', fma_id)
            else:
                temp_umls = access_UMLS_CUI_name(tgt, name)
            cl_df.loc[i, 'umls_cui'] = temp_umls

        if not pd.isnull(umls_cui) and pd.isnull(mesh_id):
            temp_mesh = UMLS2MeSH(tgt, umls_cui)
            cl_df.loc[i, 'mesh_id'] = temp_mesh
        print(i + 1, '/', len(cl_df), 'Completed...')
    cl_df.to_csv('cl_enriched.csv', index=False)


def refine_CL():
    cl_df = pd.read_csv('cl_enriched.csv')
    cl_df = cl_df[['cl_id', 'name', 'mesh_id', 'umls_cui', 'fma', 'bto_id']]

    mesh_anatomy = pd.read_csv('anatomy_mesh.csv')
    mesh_anatomy['mesh_id'] = mesh_anatomy['mesh_id'].str.replace('MESH:', '')
    mesh_name_dict = mesh_anatomy.set_index('mesh_id')['mesh_term'].to_dict()

    bto = pd.read_csv('bto.csv')
    bto_name_dict = bto.set_index('bto_id')['name'].to_dict()

    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(cl_df)):
        mesh_id = cl_df.loc[i, 'mesh_id']
        umls_cui = cl_df.loc[i, 'umls_cui']
        bto_id = cl_df.loc[i, 'bto_id']

        if not pd.isnull(bto_id):
            temp_df = cl_df[cl_df['bto_id'] == bto_id]
            if len(temp_df) > 1:
                bto_name = bto_name_dict[bto_id]
                temp_bto_name = bto_name.lower()
                temp_bto_name = temp_bto_name.translate(
                    str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                bto_name_set = set(filter(None, temp_bto_name.split(' ')))
                for j in range(len(temp_df)):
                    name = temp_df.iloc[j, 1]
                    temp_name = name.lower()
                    temp_name = temp_name.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
                    name_set = set(filter(None, temp_name.split(' ')))
                    if name_set != bto_name_set:
                        cl_df.loc[cl_df['name'] == name, 'bto_id'] = np.nan
                temp_2 = cl_df[cl_df['bto_id'] == bto_id]
                if len(temp_2) == 0:
                    cl_df.loc[cl_df['name'] == temp_df.iloc[0, 1], 'bto_id'] = bto_id
            temp_df_2 = cl_df[cl_df['bto_id'] == bto_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    cl_df.loc[cl_df['cl_id'] == temp_primary, 'bto_id'] = np.nan

        if not pd.isnull(mesh_id):
            temp_df = cl_df[cl_df['mesh_id'] == mesh_id]
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
                        cl_df.loc[cl_df['name'] == name, 'mesh_id'] = np.nan
                temp_2 = cl_df[cl_df['mesh_id'] == mesh_id]
                if len(temp_2) == 0:
                    cl_df.loc[cl_df['name'] == temp_df.iloc[0, 1], 'mesh_id'] = mesh_id
            temp_df_2 = cl_df[cl_df['mesh_id'] == mesh_id]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    cl_df.loc[cl_df['cl_id'] == temp_primary, 'mesh_id'] = np.nan

        if not pd.isnull(umls_cui):
            temp_df = cl_df[cl_df['umls_cui'] == umls_cui]
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
                        cl_df.loc[cl_df['name'] == name, 'umls_cui'] = np.nan
                temp_2 = cl_df[cl_df['umls_cui'] == umls_cui]
                if len(temp_2) == 0:
                    cl_df.loc[cl_df['name'] == temp_df.iloc[0, 1], 'umls_cui'] = umls_cui
            temp_df_2 = cl_df[cl_df['umls_cui'] == umls_cui]
            if len(temp_df_2) > 1:
                for j in range(1, len(temp_df_2)):
                    temp_primary = temp_df_2.iloc[j, 0]
                    cl_df.loc[cl_df['cl_id'] == temp_primary, 'umls_cui'] = np.nan
        print(i + 1, '/', len(cl_df), 'Completed...')
    cl_df.to_csv('cl_refined.csv', index=False)


def integrate_CL():
    anatomy_res = pd.read_csv('anatomy_res_2_refined.csv')
    anatomy_res['cl_id'] = [''] * len(anatomy_res)
    idx = len(anatomy_res)

    anatomy_res['mesh_id'] = anatomy_res['mesh_id'].str.replace('MESH:', '')
    anatomy_res['umls_cui'] = anatomy_res['umls_cui'].str.replace('UMLS:', '')

    bto_list_res = list(anatomy_res.dropna(subset=['bto_id'])['bto_id'])
    mesh_list_res = list(anatomy_res.dropna(subset=['mesh_id'])['mesh_id'])
    umls_list_res = list(anatomy_res.dropna(subset=['umls_cui'])['umls_cui'])

    cl_res = pd.read_csv('cl_refined.csv')
    for i in range(len(cl_res)):
        cl_id = cl_res.loc[i, 'cl_id']
        cl_name = cl_res.loc[i, 'name']
        mesh_id = cl_res.loc[i, 'mesh_id']
        umlc_cui = cl_res.loc[i, 'umls_cui']
        bto_id = cl_res.loc[i, 'bto_id']

        if bto_id in bto_list_res:
            anatomy_res.loc[anatomy_res['bto_id'] == bto_id, 'cl_id'] = cl_id
        elif mesh_id in mesh_list_res:
            anatomy_res.loc[anatomy_res['mesh_id'] == mesh_id, 'cl_id'] = cl_id
        elif umlc_cui in umls_list_res:
            anatomy_res.loc[anatomy_res['umls_cui'] == umlc_cui, 'cl_id'] = cl_id
        else:
            anatomy_res.loc[idx] = [cl_id, cl_name, '', '', mesh_id, umlc_cui, cl_id]
            idx += 1
        print(i + 1, '/', len(cl_res), 'Completed...')
    anatomy_res.to_csv('anatomy_res_3.csv', index=False)


def main():
    refine_res_2()
    # enriche_CL()
    # refine_CL()
    integrate_CL()

    an_vocab = pd.read_csv('anatomy_res_3.csv')
    print(len(an_vocab), len(an_vocab.drop_duplicates(subset='primary', keep='first')))
    mesh_vocab = an_vocab.dropna(subset=['mesh_id'])
    print(len(mesh_vocab), len(mesh_vocab.drop_duplicates(subset='mesh_id', keep='first')))
    bto_vocab = an_vocab.dropna(subset=['bto_id'])
    print(len(bto_vocab), len(bto_vocab.drop_duplicates(subset='bto_id', keep='first')))
    cl_vocab = an_vocab.dropna(subset=['cl_id'])
    print(len(cl_vocab), len(cl_vocab.drop_duplicates(subset='cl_id', keep='first')))
    umls_vocab = an_vocab.dropna(subset=['umls_cui'])
    print(len(umls_vocab), len(umls_vocab.drop_duplicates(subset='umls_cui', keep='first')))

    # apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    # tgt = get_UMLS_tgt(apikey)
    # # umls_cui = access_UMLS_CUI(tgt, 'FMA', '68646')
    # umls_cui = access_UMLS_CUI_name(tgt, 'cell')
    # print(umls_cui)


if __name__ == '__main__':
    main()
