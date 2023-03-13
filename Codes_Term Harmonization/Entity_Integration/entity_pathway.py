import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import urllib.error

folder = ''


def extract_Reactome_vocab():
    pathway_vocab = pd.read_table(folder + 'ReactomePathways.txt', header=None)
    pathway_vocab = pathway_vocab.rename(columns={0: 'Reactome_ID', 1: 'Name', 2: 'Species'})
    pathway_vocab['primary'] = ['REACT:'] + pathway_vocab['Reactome_ID'].astype(str)
    pathway_res = pathway_vocab[['primary', 'Reactome_ID', 'Name', 'Species']]
    pathway_res = pathway_res[pathway_res['Species'] == 'Homo sapiens']
    pathway_res = pathway_res[['primary', 'Reactome_ID', 'Name']]
    print(pathway_res)
    pathway_res.to_csv(folder + 'res/pathway_res.csv', index=False)


def add_CTD_pathway():
    ctd_pw = pd.read_csv('/Users/yuhou/Documents/Knowledge_Graph/CTD/vocabulary/CTD_pathways.csv', header=27)
    ctd_pw = ctd_pw.dropna(subset=['PathwayID'])
    ctd_pw = ctd_pw.reset_index(drop=True)
    pathway_res = pd.read_csv(folder + 'res/pathway_res.csv')
    idx = len(pathway_res)
    pathway_res['KEGG_ID'] = [''] * idx
    react_list = list(pathway_res['Reactome_ID'])

    for i in range(len(ctd_pw)):
        pathway_id = ctd_pw.loc[i, 'PathwayID']
        pathway_name = ctd_pw.loc[i, '# PathwayName']
        if 'REACT' in pathway_id:
            pathway_id = pathway_id.replace('REACT:', '')
            if pathway_id not in react_list:
                pathway_res.loc[idx] = ['REACT:' + pathway_id, pathway_id, pathway_name, '']
                idx += 1
        elif 'KEGG' in pathway_id:
            pathway_id = pathway_id.replace('KEGG:hsa', '')
            if 'M' in pathway_id:
                pathway_res.loc[idx] = ['KEGG:' + pathway_id.replace('_', ''), '', pathway_name, 'hsa' + pathway_id]
                idx += 1
            else:
                pathway_res.loc[idx] = ['KEGG:map' + pathway_id, '', pathway_name, 'hsa' + pathway_id]
                idx += 1
        print(i + 1, '/', len(ctd_pw), 'Completed...')
    pathway_res.to_csv(folder + 'res/pathway_res_2.csv', index=False)


def process_reactome_go():
    reactome_vocab = pd.read_csv(folder + 'res/pathway_res.csv')
    go_id_list = []
    for i in range(len(reactome_vocab)):
        reactome_id = reactome_vocab.loc[i, 'Reactome_ID']
        url = 'https://reactome.org/content/detail/' + reactome_id
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        go_id = ''
        try:
            rep = urlopen(req)
            webpage = rep.read()
            soup = BeautifulSoup(webpage, 'html.parser')
            go_link = soup.findAll("a", {"title": "go to GO"})
            if len(go_link) > 0:
                go_id = go_link[0].get('href').replace('https://www.ebi.ac.uk/QuickGO/term/', '')
        except urllib.error.HTTPError as e:
            print(reactome_id, 'HTTPError: {}'.format(e.code))
        go_id_list.append(go_id)
        print(i + 1, '/', len(reactome_vocab), 'Completed...')
    reactome_vocab['go_id'] = go_id_list
    reactome_vocab.to_csv(folder + 'res/reactome_pathway.csv', index=False)


def integrate_reactome_kegg():
    pathway_res = pd.read_csv(folder + 'stage_4/entity/pathway/res/pathway_res.csv')
    pathway_res['kegg_id'] = [''] * len(pathway_res)
    idx = len(pathway_res)
    kegg_pathway = pd.read_csv(folder + 'KEGG/kegg_pathway.csv')
    print(kegg_pathway)
    reactome_golist = list(pathway_res.dropna(subset=['go_id'])['go_id'])

    for i in range(len(kegg_pathway)):
        kegg_id = kegg_pathway.loc[i, 'kegg_id']
        pathway_name = kegg_pathway.loc[i, 'name']
        go_id = kegg_pathway.loc[i, 'go_id']
        if go_id in reactome_golist:
            pathway_res.loc[pathway_res['go_id'] == go_id, 'kegg_id'] = kegg_id
        else:
            pathway_res.loc[idx] = ['KEGG:' + kegg_id, '', pathway_name, go_id, kegg_id]
            idx += 1
        print(i + 1, '/', len(kegg_pathway), 'Completed...')
    print(pathway_res)
    pathway_res.to_csv(folder + 'stage_4/entity/pathway/res/pathway_res_2.csv', index=False)
    with open(folder + 'stage_4/entity/pathway/res/integrate_note.txt', 'w') as f:
        f.write('pathway_res_2.csv: Reactome; KEGG')
    f.close()


def main():
    # extract_Reactome_vocab()
    # add_CTD_pathway()
    # process_reactome_go()

    integrate_reactome_kegg()


if __name__ == '__main__':
    main()
