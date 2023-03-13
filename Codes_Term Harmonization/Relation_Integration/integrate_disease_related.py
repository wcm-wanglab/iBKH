import pandas as pd


folder = ''


def integrate_DiPwy():
    Di_Pwy_kegg = pd.read_csv(folder + 'kegg_disease_pathway.csv')
    print(Di_Pwy_kegg)

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    kegg_disease_vocab = disease_vocab.dropna(subset=['kegg_id'])
    kegg_disease_primary_dict = kegg_disease_vocab.set_index('kegg_id')['primary'].to_dict()

    pathway_vocab = pd.read_csv(folder + 'pathway_vocab.csv')
    pathway_primary_dict = pathway_vocab.dropna(subset=['kegg_id']).set_index('kegg_id')['primary'].to_dict()

    Di_Pwy_res = Di_Pwy_kegg.replace({'kegg_id': kegg_disease_primary_dict, 'pathway_id': pathway_primary_dict})
    Di_Pwy_res = Di_Pwy_res.rename(columns={'kegg_id': 'Disease', 'pathway_id': 'Pathway'})
    Di_Pwy_res = Di_Pwy_res[['Disease', 'Pathway']]
    Di_Pwy_res['Association'] = [1] * len(Di_Pwy_res)
    Di_Pwy_res['Source'] = ['KEGG'] * len(Di_Pwy_res)
    print(Di_Pwy_res)
    Di_Pwy_res.to_csv(folder + 'Di_Pw_res.csv', index=False)


def integrate_DiSy():
    hetionet_DiSy = pd.read_csv(folder + 'hetionet_DiS.csv')
    hetionet_DiSy = hetionet_DiSy.rename(columns={'source': 'Disease', 'target': 'Symptom'})
    hetionet_DiSy = hetionet_DiSy[['Disease', 'Symptom']]
    print(hetionet_DiSy)
    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    do_vocab = disease_vocab.dropna(subset=['do_id'])
    do_primary_dict = do_vocab.set_index('do_id')['primary'].to_dict()

    symptom_vocab = pd.read_csv(folder + 'symptom_vocab.csv')
    symptom_primary_dict = symptom_vocab.set_index('mesh_id')['primary'].to_dict()

    hetionet_DiSy = hetionet_DiSy.replace({'Disease': do_primary_dict, 'Symptom': symptom_primary_dict})
    DiSy_res = hetionet_DiSy
    DiSy_res['Present'] = [1] * len(DiSy_res)
    DiSy_res['Source'] = ['Hetionet'] * len(DiSy_res)
    DiSy_res.to_csv(folder + 'Di_S_res.csv', index=False)


def integrate_DiDSI():
    sdsi_dis = pd.read_table(folder + 'MRREL.RRF', delimiter='|')
    sdsi_dis = sdsi_dis[sdsi_dis['REL'] == 'is_effective_for']
    sdsi_dis = sdsi_dis.reset_index(drop=True)

    sdsi_vocab = pd.read_csv(folder + 'sdsi_vocab.csv')
    sdsi_primary_dict = sdsi_vocab.set_index('iDISK_id')['primary'].to_dict()
    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    iDISK_vocab = disease_vocab.dropna(subset=['iDISK_id'])
    iDISK_primary_dict = iDISK_vocab.set_index('iDISK_id')['primary'].to_dict()

    sdsi_dis = sdsi_dis.rename(columns={'CUI1': 'DSI', 'CUI2': 'Disease'})
    sdsi_dis = sdsi_dis[['DSI', 'Disease']]
    sdsi_dis = sdsi_dis.replace({'DSI': sdsi_primary_dict, 'Disease': iDISK_primary_dict})

    DSIDi_res = sdsi_dis
    DSIDi_res['is_effective_for'] = [1] * len(DSIDi_res)
    DSIDi_res['Source'] = ['iDISK'] * len(DSIDi_res)
    DSIDi_res.to_csv(folder + 'SDSI_Di_res.csv', index=False)


def integrate_DSISy():
    sdsi_ss = pd.read_table(folder + 'MRREL.RRF', delimiter='|')
    sdsi_ss = sdsi_ss[sdsi_ss['REL'] == 'has_adverse_reaction']
    sdsi_ss = sdsi_ss.reset_index(drop=True)

    sdsi_vocab = pd.read_csv(folder + 'sdsi_vocab.csv')
    sdsi_primary_dict = sdsi_vocab.set_index('iDISK_id')['primary'].to_dict()

    symptom_vocab = pd.read_csv(folder + 'symptom_vocab.csv')
    symptom_primary_dict = symptom_vocab.dropna(subset=['iDISK_id']).set_index('iDISK_id')['primary'].to_dict()

    sdsi_ss = sdsi_ss.rename(columns={'CUI1': 'DSI', 'CUI2': 'Symptom'})
    sdsi_ss = sdsi_ss[['DSI', 'Symptom']]
    sdsi_ss = sdsi_ss.replace({'DSI': sdsi_primary_dict, 'Symptom': symptom_primary_dict})

    DSIDy_res = sdsi_ss
    DSIDy_res['has_adverse_reaction'] = [1] * len(DSIDy_res)
    DSIDy_res['Source'] = ['iDISK'] * len(DSIDy_res)
    DSIDy_res.to_csv(folder + 'SDSI_S_res.csv', index=False)


def main():
    # integrate_DiPwy()
    # integrate_DiSy()
    # integrate_DiDSI()
    integrate_DSISy()


if __name__ == '__main__':
    main()
