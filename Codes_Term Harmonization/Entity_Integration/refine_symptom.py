"""
_*_ coding: utf-8 _*_
@ Author: Yu Hou
@File: refine_symptom.py
@Time: 4/26/21 8:49 PM
"""

import pandas as pd
import requests
from lxml.html import fromstring

folder = '/Users/yuhou/Documents/Knowledge_Graph/knowledge_bases_integration/v2_res_Apr2021_refine/symptom/'
iDISK_folder = '/Users/yuhou/Documents/Knowledge_Graph/knowledge_bases_integration/stage_3/idisk/'


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


def mesh2umls(tgt, mesh_id):
    st = get_UMLS_ts(tgt)
    mesh_url = 'https://uts-ws.nlm.nih.gov/rest/content/current/source/MSH/' + mesh_id + '/atoms?ttys=MH,NM&ticket=' + st
    mesh_resp = requests.get(mesh_url)
    umls_cui = ''
    if 'error' not in mesh_resp.json():
        mesh_content = mesh_resp.json()['result'][0]
        umls_cui = mesh_content['concept'].replace('https://uts-ws.nlm.nih.gov/rest/content/2020AB/CUI/', '')

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


def enrich_Hetionet():
    hetionet_df = pd.read_table('/Users/yuhou/Documents/Knowledge_Graph/hetionet/hetionet-v1.0-nodes.tsv')
    hetionet_symptom = hetionet_df[hetionet_df['kind'] == 'Symptom']
    hetionet_symptom = hetionet_symptom.reset_index(drop=True)
    print(hetionet_symptom)
    res = pd.DataFrame(columns=['primary', 'name', 'mesh_id', 'umls_cui'])
    idx = 0
    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)
    for i in range(len(hetionet_symptom)):
        mesh_id = hetionet_symptom.loc[i, 'id'].replace('Symptom::', '')
        name = hetionet_symptom.loc[i, 'name']
        umls_cui = mesh2umls(tgt, mesh_id)
        res.loc[idx] = ['MESH:' + mesh_id, name, mesh_id, umls_cui]
        idx += 1
        print(i + 1, '/', len(hetionet_symptom), 'Completed...')
    res.to_csv(folder + 'symptom_vocab_refined.csv', index=False)


def integrate_iDISK():
    iDISK_SS = pd.read_csv(iDISK_folder + 'entity/SS_enriched.csv')

    symptom_vocab = pd.read_csv(folder + 'symptom_vocab_refined.csv')
    symptom_vocab['iDISK_id'] = [''] * len(symptom_vocab)
    idx = len(symptom_vocab)
    mesh_vocab_list = list(symptom_vocab.dropna(subset=['mesh_id'])['mesh_id'])
    umls_vocab_list = list(symptom_vocab.dropna(subset=['umls_cui'])['umls_cui'])

    apikey = '9a095f1e-f79f-4958-bfdd-2bcba5f134d6'
    tgt = get_UMLS_tgt(apikey)

    for i in range(len(iDISK_SS)):
        cui = iDISK_SS.loc[i, 'CUI']
        name = iDISK_SS.loc[i, 'name']
        umls_cui = iDISK_SS.loc[i, 'UMLS']
        mesh_id = UMLS2MeSH(tgt, umls_cui)

        if mesh_id in mesh_vocab_list:
            symptom_vocab.loc[symptom_vocab['mesh_id'] == mesh_id, 'iDISK_id'] = cui
        elif umls_cui in umls_vocab_list:
            symptom_vocab.loc[symptom_vocab['umls_cui'] == umls_cui, 'iDISK_id'] = cui
        else:
            if mesh_id != '':
                symptom_vocab.loc[idx] = ['MESH:' + mesh_id, name, mesh_id, umls_cui, cui]
                idx += 1
            else:
                symptom_vocab.loc[idx] = ['UMLS:' + umls_cui, name, mesh_id, umls_cui, cui]
                idx += 1
        print(i + 1, '/', len(iDISK_SS), 'Completed...')
    symptom_vocab.to_csv(folder + 'symptom_vocab_refined_2.csv', index=False)


def main():
    # enrich_Hetionet()
    integrate_iDISK()

    # symptom_vocab = pd.read_csv(folder + 'symptom_vocab_refined.csv')
    # print(len(symptom_vocab), len(symptom_vocab.drop_duplicates(subset='primary', keep='first')))
    # mesh_vocab = symptom_vocab.dropna(subset=['mesh_id'])
    # print(len(mesh_vocab), len(mesh_vocab.drop_duplicates(subset='mesh_id', keep='first')))
    # umls_vocab = symptom_vocab.dropna(subset=['umls_cui'])
    # print(len(umls_vocab), len(umls_vocab.drop_duplicates(subset='umls_cui', keep='first')))


if __name__ == '__main__':
    main()
