import pandas as pd


folder = ''


def integrate_DPwy():
    D_Pwy_kegg = pd.read_csv(folder + '/kegg_drug_pathway.csv')
    print(D_Pwy_kegg)
    pathway_vocab = pd.read_csv(folder + '/pathway_vocab.csv')
    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')

    pathway_primary_dict = pathway_vocab.dropna(subset=['kegg_id']).set_index('kegg_id')['primary'].to_dict()
    kegg_drug_vocab = drug_vocab.dropna(subset=['kegg_id'])
    kegg_drug_primary_dict = kegg_drug_vocab.set_index('kegg_id')['primary'].to_dict()

    D_Pwy_res = D_Pwy_kegg.replace({'kegg_id': kegg_drug_primary_dict, 'pathway_id': pathway_primary_dict})
    D_Pwy_res = D_Pwy_res.rename(columns={'kegg_id': 'Drug', 'pathway_id': 'Pathway'})
    D_Pwy_res = D_Pwy_res[['Drug', 'Pathway']]
    D_Pwy_res['Association'] = [1] * len(D_Pwy_res)
    D_Pwy_res['Source'] = ['KEGG'] * len(D_Pwy_res)
    print(D_Pwy_res)
    D_Pwy_res.to_csv(folder + '/D_Pw_res.csv', index=False)


def integrate_DSE():
    sider_df = pd.read_table(folder + '/meddra_all_se.tsv', header=None)
    sider_df = sider_df[sider_df[3] == 'PT']
    sider_df = sider_df[[0, 4, 5]]
    sider_df = sider_df.rename(columns={0: 'CID', 4: 'umls_cui', 5: 'name'})
    sider_df = sider_df.reset_index(drop=True)

    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    side_effect_vocab = pd.read_csv(folder + '/side_effect_vocab.csv')

    cid_primary = drug_vocab.dropna(subset=['CID']).set_index('CID')['primary'].to_dict()
    se_primary = side_effect_vocab.set_index(['umls_cui'])['primary'].to_dict()

    D_SE_res = sider_df[['CID', 'umls_cui']]
    D_SE_res = D_SE_res.replace({'CID': cid_primary})
    D_SE_res = D_SE_res.replace({'umls_cui': se_primary})
    D_SE_res['Cause'] = [1] * len(D_SE_res)
    D_SE_res['Source'] = ['SIDER'] * len(D_SE_res)
    D_SE_res = D_SE_res.rename(columns={'CID': 'Drug', 'umls_cui': 'Side_Effect'})

    print(D_SE_res)
    D_SE_res.to_csv(folder + '/D_SE_res.csv', index=False)


def integrate_DSDSI():
    sdsi_spd = pd.read_table(folder + '/MRREL.RRF', delimiter='|')
    sdsi_spd = sdsi_spd[sdsi_spd['REL'] == 'interacts_with']
    sdsi_spd = sdsi_spd.reset_index(drop=True)

    sdsi_vocab = pd.read_csv(folder + '/sdsi_vocab.csv')
    sdsi_primary_dict = sdsi_vocab.set_index('iDISK_id')['primary'].to_dict()
    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    drug_idisk_primary_dict = drug_vocab.dropna(subset=['iDISK_id']).set_index('iDISK_id')['primary'].to_dict()

    sdsi_spd_res = pd.DataFrame(columns=['SDSI', 'Drug', 'interacts_with', 'Source'])
    for i in range(len(sdsi_spd)):
        sdsi = sdsi_spd.loc[i, 'CUI1']
        drug = sdsi_spd.loc[i, 'CUI2']
        sdsi_primary = sdsi_primary_dict[sdsi]
        drug_primary = drug_idisk_primary_dict[drug]
        sdsi_spd_res.loc[i] = [sdsi_primary, drug_primary, 1, 'iDISK']
        print(i + 1, '/', len(sdsi_spd), 'Completed (SDSI_SPD)...')

    sdsi_spd_res.to_csv(folder + '/SDSI_D_res.csv', index=False)


def main():
    # integrate_DPwy()
    # integrate_DSE()
    integrate_DSDSI()


if __name__ == '__main__':
    main()
