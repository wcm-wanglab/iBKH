import pandas as pd
import numpy as np

folder = ''
CTD_folder = '../CTD/'

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)


def integrate_DrugBank_KEGG():
    DG_res = pd.read_csv(folder + '/DGres_DrugBank_regulate.csv')
    DG_res_col = list(DG_res.columns)
    DG_res_col = [col_name.replace('_DrugBank', '') for col_name in DG_res_col]
    DG_res.columns = DG_res_col
    rel_list = list(DG_res_col)[2:]
    DG_res['Associate_KEGG'] = [0] * len(DG_res)
    DG_res['Source'] = ['DrugBank'] * len(DG_res)
    # print(DG_res)
    kegg_res = pd.read_csv(folder + '/kegg_drug_gene.csv')

    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    kegg_vocab = drug_vocab.dropna(subset=['kegg_id'])
    kegg_primary_dict = kegg_vocab.set_index('kegg_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    kegg_res = kegg_res.replace({'drug': kegg_primary_dict, 'gene': ncbi_primary_dict})
    kegg_res = kegg_res.rename(columns={'drug': 'Drug', 'gene': 'Gene'})
    kegg_res = kegg_res[['Drug', 'Gene']]
    for col in rel_list:
        kegg_res[col] = [0] * len(kegg_res)
    kegg_res['Associate_KEGG'] = [1] * len(kegg_res)
    kegg_res['Source'] = ['KEGG'] * len(kegg_res)
    # print(kegg_res)
    DG_res = pd.concat((DG_res, kegg_res))
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Associate_KEGG'] = 1
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Source'] = 'DrugBank;KEGG'
    DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
    DG_res.to_csv(folder + '/DG_res.csv', index=False)
    with open(folder + '/integration_notes.txt', 'w') as f:
        f.write('DG_res: DrugBank (Target, Transporter, Enzyme, Carrier, Downregulates and Upregulates) and KEGG (Associate).\n')
    f.close()


def extract_PharmGKB_DG():
    pharmgkb_rel = pd.read_table(folder + 'pharmgkb_rel.tsv')
    pharmgkb_rel = pharmgkb_rel[pharmgkb_rel['Association'] == 'associated']
    pharmgkb_rel = pharmgkb_rel.reset_index(drop=True)
    res = pd.DataFrame(columns=['Drug', 'Gene'])
    idx = 0
    for i in range(len(pharmgkb_rel)):
        p1_id = pharmgkb_rel.loc[i, 'Entity1_id']
        p1_type = pharmgkb_rel.loc[i, 'Entity1_type']
        p2_id = pharmgkb_rel.loc[i, 'Entity2_id']
        p2_type = pharmgkb_rel.loc[i, 'Entity2_type']
        if p1_type == 'Chemical' and p2_type == 'Gene':
            drug = p1_id
            gene = p2_id
        elif p2_type == 'Chemical' and p1_type == 'Gene':
            drug = p2_id
            gene = p1_id
        else:
            continue
        res.loc[idx] = [drug, gene]
        idx += 1
    res.to_csv(folder + '/pharmgkb_drug_gene.csv', index=False)


def integrate_PharmGKB():
    DG_res = pd.read_csv(folder + '/DG_res.csv')
    DG_res_col = list(DG_res.columns)[2:]
    DG_res['Associate_PharmGKB'] = [0] * len(DG_res)
    # print(DG_res)
    pharmgkb_res = pd.read_csv(folder + '/pharmgkb_drug_gene.csv')

    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    pharmgkb_drug_vocab = drug_vocab.dropna(subset=['pharmgkb_id'])
    pharmgkb_drug_primary_dict = pharmgkb_drug_vocab.set_index('pharmgkb_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    pharmgkb_gene_vocab = gene_vocab.dropna(subset=['pharmgkb_id'])
    pharmgkb_gene_primary_dict = pharmgkb_gene_vocab.set_index('pharmgkb_id')['primary'].to_dict()

    pharmgkb_res = pharmgkb_res.replace({'Drug': pharmgkb_drug_primary_dict, 'Gene': pharmgkb_gene_primary_dict})
    for col in DG_res_col[:-1]:
        pharmgkb_res[col] = [0] * len(pharmgkb_res)
    pharmgkb_res['Source'] = ['PharmGKB'] * len(pharmgkb_res)
    pharmgkb_res['Associate_PharmGKB'] = [1] * len(pharmgkb_res)
    # print(pharmgkb_res)
    DG_res = pd.concat((DG_res, pharmgkb_res))
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Associate_PharmGKB'] = 1
    DG_res['Source'] = np.where(DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), DG_res['Source'].astype(str) + ';PharmGKB', DG_res['Source'].astype(str) + '')
    DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
    DG_res_col = list(DG_res.columns)
    DG_res_col_new = DG_res_col[:-2] + DG_res_col[-1:] + DG_res_col[-2:-1]
    DG_res = DG_res[DG_res_col_new]
    DG_res['Source'] = DG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DG_res.to_csv(folder + '/DG_res_2.csv', index=False)
    with open(folder + '/integration_notes.txt', 'a') as f:
        f.write('DG_res_2: DrugBank, KEGG and PharmGKB (Associate).\n')
    f.close()


def integrate_Hetionet():
    DG_res = pd.read_csv(folder + '/DG_res_2.csv')
    DG_res_col = list(DG_res.columns)[2:]
    DG_res['Binds_Hetionet'] = [0] * len(DG_res)

    hetionet_DG = pd.read_csv(folder + '/hetionet_DG.csv')
    hetionet_DG = hetionet_DG.rename(columns={'source': 'Drug', 'target': 'Gene'})
    hetionet_DG['Drug'] = hetionet_DG['Drug'].str.replace('Compound::', '')
    hetionet_DG['Gene'] = hetionet_DG['Gene'].str.replace('Gene::', '')

    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    db_vocab = drug_vocab.dropna(subset=['drugbank_id'])
    db_primary_dict = db_vocab.set_index('drugbank_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    # integrate binds
    hetionet_binds = hetionet_DG[hetionet_DG['metaedge'] == 'CbG']
    hetionet_binds = hetionet_binds.replace({'Drug': db_primary_dict, 'Gene': ncbi_primary_dict})
    hetionet_binds = hetionet_binds[['Drug', 'Gene']]
    for col in DG_res_col[:-1]:
        hetionet_binds[col] = [0] * len(hetionet_binds)
    hetionet_binds['Source'] = ['Hetinoet'] * len(hetionet_binds)
    hetionet_binds['Binds_Hetionet'] = [1] * len(hetionet_binds)
    DG_res = pd.concat((DG_res, hetionet_binds))
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Binds_Hetionet'] = 1
    DG_res['Source'] = np.where(DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), DG_res['Source'].astype(str) + ';Hetinoet', DG_res['Source'].astype(str) + '')
    DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
    DG_res_col = list(DG_res.columns)
    DG_res_col_new = DG_res_col[:-2] + DG_res_col[-1:] + DG_res_col[-2:-1]
    DG_res = DG_res[DG_res_col_new]
    DG_res['Source'] = DG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    print(DG_res)

    # integrate Downregulates
    DG_res_col = list(DG_res.columns)[2:]
    DG_res['Downregulates_Hetionet'] = [0] * len(DG_res)
    hetionet_downregulates = hetionet_DG[hetionet_DG['metaedge'] == 'CdG']
    hetionet_downregulates = hetionet_downregulates.replace({'Drug': db_primary_dict, 'Gene': ncbi_primary_dict})
    hetionet_downregulates = hetionet_downregulates[['Drug', 'Gene']]
    for col in DG_res_col[:-1]:
        hetionet_downregulates[col] = [0] * len(hetionet_downregulates)
    hetionet_downregulates['Source'] = ['Hetinoet'] * len(hetionet_downregulates)
    hetionet_downregulates['Downregulates_Hetionet'] = [1] * len(hetionet_downregulates)
    DG_res = pd.concat((DG_res, hetionet_downregulates))
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Downregulates_Hetionet'] = 1
    DG_res['Source'] = np.where(DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), DG_res['Source'].astype(str) + ';Hetinoet', DG_res['Source'].astype(str) + '')
    DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
    DG_res_col = list(DG_res.columns)
    DG_res_col_new = DG_res_col[:-2] + DG_res_col[-1:] + DG_res_col[-2:-1]
    DG_res = DG_res[DG_res_col_new]
    DG_res['Source'] = DG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    print(DG_res)

    # integrate Upregulates
    DG_res_col = list(DG_res.columns)[2:]
    DG_res['Upregulates_Hetionet'] = [0] * len(DG_res)
    hetionet_upregulates = hetionet_DG[hetionet_DG['metaedge'] == 'CuG']
    hetionet_upregulates = hetionet_upregulates.replace({'Drug': db_primary_dict, 'Gene': ncbi_primary_dict})
    hetionet_upregulates = hetionet_upregulates[['Drug', 'Gene']]
    for col in DG_res_col[:-1]:
        hetionet_upregulates[col] = [0] * len(hetionet_upregulates)
    hetionet_upregulates['Source'] = ['Hetinoet'] * len(hetionet_upregulates)
    hetionet_upregulates['Upregulates_Hetionet'] = [1] * len(hetionet_upregulates)
    DG_res = pd.concat((DG_res, hetionet_upregulates))
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Upregulates_Hetionet'] = 1
    DG_res['Source'] = np.where(DG_res.duplicated(subset=['Drug', 'Gene'], keep=False),
                                DG_res['Source'].astype(str) + ';Hetinoet', DG_res['Source'].astype(str) + '')
    DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
    DG_res_col = list(DG_res.columns)
    DG_res_col_new = DG_res_col[:-2] + DG_res_col[-1:] + DG_res_col[-2:-1]
    DG_res = DG_res[DG_res_col_new]
    DG_res['Source'] = DG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    print(DG_res)
    DG_res.to_csv(folder + '/DG_res_3.csv', index=False)
    with open(folder + '/integration_notes.txt', 'a') as f:
        f.write('DG_res_3: DrugBank, KEGG, PharmGKB and Hetionet (Binds, Downregulates and Upregulates).\n')
    f.close()


def intergrate_CTD_DG():
    DG_res = pd.read_csv(folder + '/DG_res_3.csv')
    DG_res_col = list(DG_res.columns)[2:]
    DG_res['Interaction_CTD'] = [0] * len(DG_res)

    chem_gene = pd.read_csv(CTD_folder + 'CTD_chem_gene_ixns.csv', header=27)
    chem_gene = chem_gene[['ChemicalID', 'GeneID']].dropna()
    chem_gene = chem_gene.drop_duplicates(subset=['ChemicalID', 'GeneID'])
    chem_gene = chem_gene.reset_index(drop=True)
    chem_gene = chem_gene.rename(columns={'ChemicalID': 'Drug', 'GeneID': 'Gene'})

    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    mesh_vocab = drug_vocab.dropna(subset=['mesh_id'])
    mesh_primary_dict = mesh_vocab.set_index('mesh_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    # chem_gene = chem_gene.replace({'Drug': mesh_primary_dict, 'Gene': ncbi_primary_dict})

    for i in range(len(chem_gene)):
        drug_id = chem_gene.loc[i, 'Drug']
        gene_id = chem_gene.loc[i, 'Gene']

        chem_gene.loc[i, 'Drug'] = mesh_primary_dict[drug_id]
        chem_gene.loc[i, 'Gene'] = ncbi_primary_dict[gene_id]
        print(i + 1, '/', len(chem_gene), 'Completed...')
    print(chem_gene)

    for col in DG_res_col[:-1]:
        chem_gene[col] = [0] * len(chem_gene)
    chem_gene['Source'] = ['CTD'] * len(chem_gene)
    chem_gene['Interaction_CTD'] = [1] * len(chem_gene)
    DG_res = pd.concat((DG_res, chem_gene))
    DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), 'Interaction_CTD'] = 1
    DG_res['Source'] = np.where(DG_res.duplicated(subset=['Drug', 'Gene'], keep=False),
                                DG_res['Source'].astype(str) + ';CTD', DG_res['Source'].astype(str) + '')
    DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
    DG_res_col = list(DG_res.columns)
    DG_res_col_new = DG_res_col[:-2] + DG_res_col[-1:] + DG_res_col[-2:-1]
    DG_res = DG_res[DG_res_col_new]
    DG_res['Source'] = DG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DG_res.to_csv(folder + '/DG_res_4.csv', index=False)
    with open(folder + '/integration_notes.txt', 'a') as f:
        f.write('DG_res_4: DrugBank, KEGG, PharmGKB, Hetionet and CTD (Interaction).\n')
    f.close()


def integrate_DRKG_DG():
    DG_res = pd.read_csv(folder + '/DG_res_4.csv')
    DG_res_col = list(DG_res.columns)[2:]

    drkg_DG = pd.read_csv('drug/drkg_DG.csv')
    drkg_DG = drkg_DG.rename(columns={'entity_1': 'Drug', 'entity_2': 'Gene'})
    drkg_DG['Drug'] = drkg_DG['Drug'].str.replace('Compound::', '')
    drkg_DG['Gene'] = drkg_DG['Gene'].str.replace('Gene::', '')
    dg_relation_list = list(drkg_DG.drop_duplicates(subset='relation', keep='first')['relation'])
    # dg_source_list = list(drkg_DG.drop_duplicates(subset='source', keep='first')['source'])
    # print(dg_relation_list)
    # print(dg_source_list)
    # print(drkg_DG.drop_duplicates(subset='relation', keep='first'))

    drug_vocab = pd.read_csv(folder + '/drug_vocab.csv')
    db_vocab = drug_vocab.dropna(subset=['drugbank_id'])
    db_primary_dict = db_vocab.set_index('drugbank_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    for drkg_rel in dg_relation_list:
        print(drkg_rel)
        DG_res[drkg_rel] = [0] * len(DG_res)
        drkg_DG_temp = drkg_DG[drkg_DG['relation'] == drkg_rel]
        drkg_DG_temp = drkg_DG_temp.replace({'Drug': db_primary_dict, 'Gene': ncbi_primary_dict})
        drkg_DG_temp = drkg_DG_temp[['Drug', 'Gene']]
        for col in DG_res_col[:-1]:
            drkg_DG_temp[col] = [0] * len(drkg_DG_temp)
        drkg_DG_temp['Source'] = ['DRKG'] * len(drkg_DG_temp)
        drkg_DG_temp[drkg_rel] = [1] * len(drkg_DG_temp)
        DG_res = pd.concat((DG_res, drkg_DG_temp))
        DG_res.loc[DG_res.duplicated(subset=['Drug', 'Gene'], keep=False), drkg_rel] = 1
        DG_res['Source'] = np.where(DG_res.duplicated(subset=['Drug', 'Gene'], keep=False),
                                    DG_res['Source'].astype(str) + ';DRKG', DG_res['Source'].astype(str) + '')
        DG_res = DG_res.drop_duplicates(subset=['Drug', 'Gene'], keep='first')
        DG_res_col = list(DG_res.columns)
        DG_res_col_new = DG_res_col[:-2] + DG_res_col[-1:] + DG_res_col[-2:-1]
        DG_res = DG_res[DG_res_col_new]
        DG_res_col = DG_res_col_new[2:]
        DG_res['Source'] = DG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))

    DG_res = DG_res.rename(columns={'association': 'Associate_DRKG', 'direct interation': 'Interaction_DRKG'})
    DG_res.to_csv(folder + '/DG_res_5.csv', index=False)
    with open(folder + '/integration_notes.txt', 'a') as f:
        f.write('DG_res_5: DrugBank, KEGG, PharmGKB, Hetionet, CTD and DRKG (Semantic Relations, Interaction and Associate).\n')
    f.close()


def main():
    # integrate_DrugBank_KEGG()
    # # extract_PharmGKB_DG()
    # integrate_PharmGKB()
    integrate_Hetionet()
    intergrate_CTD_DG()
    integrate_DRKG_DG()


if __name__ == '__main__':
    main()
