import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

folder = ''
CTD_folder = '../CTD/'


def integrate_Hetionet():
    hetionet_DiG = pd.read_csv(folder + 'hetionet_DiG.csv')
    hetionet_DiG = hetionet_DiG.rename(columns={'source': 'Disease', 'target': 'Gene'})
    hetionet_DiG['Disease'] = hetionet_DiG['Disease'].str.replace('Disease::', '')
    hetionet_DiG['Gene'] = hetionet_DiG['Gene'].str.replace('Gene::', '')

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    do_vocab = disease_vocab.dropna(subset=['do_id'])
    do_primary_dict = do_vocab.set_index('do_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + 'gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    hetionet_dig_dag = hetionet_DiG[hetionet_DiG['metaedge'] == 'DaG']
    hetionet_dig_dag = hetionet_dig_dag.replace({'Disease': do_primary_dict, 'Gene': ncbi_primary_dict})
    hetionet_dig_dag = hetionet_dig_dag[['Disease', 'Gene']]
    hetionet_dig_dag['Associate_Hetionet'] = [1] * len(hetionet_dig_dag)
    hetionet_dig_dag['Downregulates_Hetionet'] = [0] * len(hetionet_dig_dag)

    hetionet_dig_ddg = hetionet_DiG[hetionet_DiG['metaedge'] == 'DdG']
    hetionet_dig_ddg = hetionet_dig_ddg.replace({'Disease': do_primary_dict, 'Gene': ncbi_primary_dict})
    hetionet_dig_ddg = hetionet_dig_ddg[['Disease', 'Gene']]
    hetionet_dig_ddg['Associate_Hetionet'] = [0] * len(hetionet_dig_ddg)
    hetionet_dig_ddg['Downregulates_Hetionet'] = [1] * len(hetionet_dig_ddg)

    DiG_res = pd.concat((hetionet_dig_dag, hetionet_dig_ddg))
    DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), 'Downregulates_Hetionet'] = 1
    DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')

    DiG_res['Upregulates_Hetionet'] = [0] * len(DiG_res)

    hetionet_dig_dug = hetionet_DiG[hetionet_DiG['metaedge'] == 'DuG']
    hetionet_dig_dug = hetionet_dig_dug.replace({'Disease': do_primary_dict, 'Gene': ncbi_primary_dict})
    hetionet_dig_dug = hetionet_dig_dug[['Disease', 'Gene']]
    hetionet_dig_dug['Associate_Hetionet'] = [0] * len(hetionet_dig_dug)
    hetionet_dig_dug['Downregulates_Hetionet'] = [0] * len(hetionet_dig_dug)
    hetionet_dig_dug['Upregulates_Hetionet'] = [1] * len(hetionet_dig_dug)

    DiG_res = pd.concat((DiG_res, hetionet_dig_dug))
    DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), 'Upregulates_Hetionet'] = 1
    DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')

    DiG_res['Source'] = ['Hetionet'] * len(DiG_res)
    print(DiG_res)
    DiG_res.to_csv(folder + 'DiG_res.csv', index=False)
    with open(folder + 'integration_notes.txt', 'w') as f:
        f.write('DiG_res: Hetionet (Associate, Downregulates and Upregulates).\n')
    f.close()


def integrate_KEGG():
    DiG_res = pd.read_csv(folder + 'DiG_res.csv')
    DiG_res_cols = list(DiG_res.columns)[2:]
    DiG_res['Associate_KEGG'] = [0] * len(DiG_res)

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    kegg_disease_vocab = disease_vocab.dropna(subset=['kegg_id'])
    kegg_disease_primary_dict = kegg_disease_vocab.set_index('kegg_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + 'gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    kegg_df = pd.read_csv(folder + 'kegg_disease_gene.csv')
    kegg_df = kegg_df.rename(columns={'disease': 'Disease', 'gene': 'Gene'})
    kegg_df = kegg_df.replace({'Disease': kegg_disease_primary_dict, 'Gene': ncbi_primary_dict})

    for col in DiG_res_cols[:-1]:
        kegg_df[col] = [0] * len(kegg_df)
    kegg_df['Source'] = ['KEGG'] * len(kegg_df)
    kegg_df['Associate_KEGG'] = [1] * len(kegg_df)
    DiG_res = pd.concat((DiG_res, kegg_df))
    DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), 'Associate_KEGG'] = 1
    DiG_res['Source'] = np.where(DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False),
                                 DiG_res['Source'].astype(str) + ';KEGG', DiG_res['Source'].astype(str) + '')
    DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')
    DiG_res_col = list(DiG_res.columns)
    DiG_res_col_new = DiG_res_col[:-2] + DiG_res_col[-1:] + DiG_res_col[-2:-1]
    DiG_res = DiG_res[DiG_res_col_new]
    print(DiG_res)
    DiG_res.to_csv(folder + 'DiG_res_2.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DiG_res_2: Hetionet and KEGG (Associate).\n')
    f.close()


def extract_PharmGKB_DiG():
    pharmgkb_rel = pd.read_table(folder + 'pharmgkb_rel.tsv')
    pharmgkb_rel = pharmgkb_rel[pharmgkb_rel['Association'] == 'associated']
    pharmgkb_rel = pharmgkb_rel.reset_index(drop=True)
    res = pd.DataFrame(columns=['Disease', 'Gene'])
    idx = 0
    for i in range(len(pharmgkb_rel)):
        p1_id = pharmgkb_rel.loc[i, 'Entity1_id']
        p1_type = pharmgkb_rel.loc[i, 'Entity1_type']
        p2_id = pharmgkb_rel.loc[i, 'Entity2_id']
        p2_type = pharmgkb_rel.loc[i, 'Entity2_type']
        if p1_type == 'Disease' and p2_type == 'Gene':
            disease = p1_id
            gene = p2_id
        elif p2_type == 'Disease' and p1_type == 'Gene':
            disease = p2_id
            gene = p1_id
        else:
            continue
        res.loc[idx] = [disease, gene]
        idx += 1
    res.to_csv(folder + 'pharmgkb_disease_gene.csv', index=False)


def integrate_PharmGKB():
    DiG_res = pd.read_csv(folder + 'DiG_res_2.csv')
    DiG_res_cols = list(DiG_res.columns)[2:]
    DiG_res['Associate_PharmGKB'] = [0] * len(DiG_res)

    pharmgkb_res = pd.read_csv(folder + 'pharmgkb_disease_gene.csv')

    gene_vocab = pd.read_csv(folder + 'gene_vocab_2.csv')
    pharmgkb_gene_vocab = gene_vocab.dropna(subset=['pharmgkb_id'])
    pharmgkb_gene_primary_dict = pharmgkb_gene_vocab.set_index('pharmgkb_id')['primary'].to_dict()

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    pharmgkb_disease_vocab = disease_vocab.dropna(subset=['pharmgkb_id'])
    pharmgkb_disease_primary_dict = pharmgkb_disease_vocab.set_index('pharmgkb_id')['primary'].to_dict()

    pharmgkb_res = pharmgkb_res.replace({'Disease': pharmgkb_disease_primary_dict, 'Gene': pharmgkb_gene_primary_dict})
    for col in DiG_res_cols[:-1]:
        pharmgkb_res[col] = [0] * len(pharmgkb_res)
    pharmgkb_res['Source'] = ['PharmGKB'] * len(pharmgkb_res)
    pharmgkb_res['Associate_PharmGKB'] = [1] * len(pharmgkb_res)
    DiG_res = pd.concat((DiG_res, pharmgkb_res))
    DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), 'Associate_PharmGKB'] = 1
    DiG_res['Source'] = np.where(DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False),
                                 DiG_res['Source'].astype(str) + ';PharmGKB', DiG_res['Source'].astype(str) + '')
    DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')
    DiG_res_col = list(DiG_res.columns)
    DiG_res_col_new = DiG_res_col[:-2] + DiG_res_col[-1:] + DiG_res_col[-2:-1]
    DiG_res = DiG_res[DiG_res_col_new]
    DiG_res['Source'] = DiG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DiG_res.to_csv(folder + 'DiG_res_3.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DiG_res_3: Hetionet, KEGG and PharmGKB (Associate).\n')
    f.close()


def integrate_CTD_DiG_curated():
    disease_gene = pd.read_csv(CTD_folder + 'CTD_genes_diseases.csv', header=27)
    disease_gene = disease_gene.dropna(subset=['GeneID', 'DiseaseID'])
    disease_gene = disease_gene.drop_duplicates(subset=['GeneID', 'DiseaseID'])
    disease_gene = disease_gene.reset_index(drop=True)
    disease_gene = disease_gene.rename(columns={'DiseaseID': 'Disease', 'GeneID': 'Gene'})
    disease_gene_curated = disease_gene[pd.isnull(disease_gene['InferenceScore'])]

    disease_gene_curated = disease_gene_curated[['Disease', 'Gene']]
    disease_gene_curated = disease_gene_curated.reset_index(drop=True)

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    mesh_disease_vocab = disease_vocab.dropna(subset=['mesh_id'])
    mesh_disease_primary_dict = mesh_disease_vocab.set_index('mesh_id')['primary'].to_dict()
    omim_vocab = disease_vocab.dropna(subset=['omim_id'])
    omim_vocab['omim_id'] = omim_vocab['omim_id'].astype(int).astype(str)
    omim_primary_dict = omim_vocab.set_index('omim_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + 'gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    DiG_res = pd.read_csv(folder + 'DiG_res_3.csv')
    DiG_res_col = list(DiG_res.columns)[2:]
    DiG_res['Associate_CTD'] = [0] * len(DiG_res)
    print(disease_gene_curated)
    disease_list = []
    gene_list = []
    for i in range(len(disease_gene_curated)):
        disease_id = disease_gene_curated.loc[i, 'Disease']
        gene_id = disease_gene_curated.loc[i, 'Gene']

        gene_list.append(ncbi_primary_dict[gene_id])
        if 'MESH' in disease_id:
            disease_id = disease_id.replace('MESH:', '')
            disease_list.append(mesh_disease_primary_dict[disease_id])
        else:
            disease_id = disease_id.replace('OMIM:', '')
            disease_list.append(omim_primary_dict[disease_id])
        print(i + 1, '/', len(disease_gene_curated), 'Completed...')
    disease_gene_curated = pd.DataFrame({'Disease': disease_list, 'Gene': gene_list})
    print(disease_gene_curated)

    for col in DiG_res_col[:-1]:
        disease_gene_curated[col] = [0] * len(disease_gene_curated)
    disease_gene_curated['Source'] = ['CTD'] * len(disease_gene_curated)
    disease_gene_curated['Associate_CTD'] = [1] * len(disease_gene_curated)
    DiG_res = pd.concat((DiG_res, disease_gene_curated))
    DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), 'Associate_CTD'] = 1
    DiG_res['Source'] = np.where(DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False),
                                 DiG_res['Source'].astype(str) + ';CTD', DiG_res['Source'].astype(str) + '')
    DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')
    DiG_res_col = list(DiG_res.columns)
    DiG_res_col_new = DiG_res_col[:-2] + DiG_res_col[-1:] + DiG_res_col[-2:-1]
    DiG_res = DiG_res[DiG_res_col_new]
    DiG_res['Source'] = DiG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DiG_res.to_csv(folder + 'DiG_res_4.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DiG_res_4: Hetionet, KEGG, PharmGKB and CTD_curated (Associate).\n')
    f.close()


def integrate_CTD_DiG_inferred():
    DiG_res = pd.read_csv(folder + 'DiG_res_4.csv')
    DiG_res_col = list(DiG_res.columns)[2:]
    DiG_res['Inferred_Relation'] = [0] * len(DiG_res)
    DiG_res['Inference_Score'] = [''] * len(DiG_res)

    disease_gene_inferred = pd.read_csv(folder + 'CTD_disease_gene_inferred.csv')

    for col in DiG_res_col[:-1]:
        disease_gene_inferred[col] = [0] * len(disease_gene_inferred)
    disease_gene_inferred['Source'] = ['CTD'] * len(disease_gene_inferred)
    disease_gene_inferred['Inferred_Relation'] = [1] * len(disease_gene_inferred)
    temp_col = list(disease_gene_inferred.columns)
    disease_gene_inferred_col = temp_col[:2] + temp_col[3:] + temp_col[2:3]
    disease_gene_inferred = disease_gene_inferred[disease_gene_inferred_col]
    print(list(disease_gene_inferred.columns))
    DiG_res = pd.concat((DiG_res, disease_gene_inferred))
    DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), 'Inferred_Relation'] = 1
    DiG_res['Source'] = np.where(DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False),
                                 DiG_res['Source'].astype(str) + ';CTD', DiG_res['Source'].astype(str) + '')
    DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')
    DiG_res_col = list(DiG_res.columns)
    DiG_res_col_new = DiG_res_col[:-3] + DiG_res_col[-2:-1] + DiG_res_col[-3:-2] + DiG_res_col[-1:]
    DiG_res = DiG_res[DiG_res_col_new]
    DiG_res['Source'] = DiG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DiG_res.to_csv(folder + 'DiG_res_5.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DiG_res_5: Hetionet, KEGG, PharmGKB, CTD_curated and CTD (Inferred_Relation).\n')
    f.close()


def integrate_DRKG_DiG():
    DiG_res = pd.read_csv(folder + 'DiG_res_5.csv')
    DiG_res_col = list(DiG_res.columns)[2:]

    drkg_DiG = pd.read_csv('drkg_DiG.csv')
    # drkg_DDi = pd.read_csv('/drkg_DDi.csv')
    drkg_DiG = drkg_DiG.rename(columns={'entity_1': 'Disease', 'entity_2': 'Gene'})
    drkg_DiG['Disease'] = drkg_DiG['Disease'].str.replace('Disease::', '')
    drkg_DiG['Gene'] = drkg_DiG['Gene'].str.replace('Gene::', '')
    dig_relation_list = list(drkg_DiG.drop_duplicates(subset='relation', keep='first')['relation'])
    # dig_source_list = list(drkg_DiG.drop_duplicates(subset='source', keep='first')['source'])
    # print(dig_relation_list)
    # print(dig_source_list)
    # print(drkg_DiG.drop_duplicates(subset='relation', keep='first'))

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    mesh_disease_vocab = disease_vocab.dropna(subset=['mesh_id'])
    mesh_disease_primary_dict = mesh_disease_vocab.set_index('mesh_id')['primary'].to_dict()
    omim_vocab = disease_vocab.dropna(subset=['omim_id'])
    omim_vocab['omim_id'] = omim_vocab['omim_id'].astype(int).astype(str)
    omim_primary_dict = omim_vocab.set_index('omim_id')['primary'].to_dict()

    gene_vocab = pd.read_csv(folder + 'gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    for drkg_rel in dig_relation_list:
        print(drkg_rel)
        DiG_res[drkg_rel] = [0] * len(DiG_res)
        drkg_DiG_temp = drkg_DiG[drkg_DiG['relation'] == drkg_rel]
        drkg_DiG_temp = drkg_DiG_temp[['Disease', 'Gene']]
        drkg_DiG_temp = drkg_DiG_temp.reset_index(drop=True)

        disease_list = []
        gene_list = []
        for i in range(len(drkg_DiG_temp)):
            disease_id = drkg_DiG_temp.loc[i, 'Disease']
            gene_id = drkg_DiG_temp.loc[i, 'Gene']

            if gene_id in ncbi_primary_dict:
                gene_list.append(ncbi_primary_dict[gene_id])
            else:
                continue

            if 'MESH' in disease_id:
                disease_id = disease_id.replace('MESH:', '')
                disease_list.append(mesh_disease_primary_dict[disease_id])
            else:
                disease_id = disease_id.replace('OMIM:', '')
                disease_list.append(omim_primary_dict[disease_id])

            print(i + 1, '/', len(drkg_DiG_temp), 'Completed...')

        drkg_DiG_temp_primary = pd.DataFrame({'Disease': disease_list, 'Gene': gene_list})

        for col in DiG_res_col[:-2]:
            drkg_DiG_temp_primary[col] = [0] * len(drkg_DiG_temp_primary)
        drkg_DiG_temp_primary['Source'] = ['DRKG'] * len(drkg_DiG_temp_primary)
        drkg_DiG_temp_primary['Inference_Score'] = [''] * len(drkg_DiG_temp_primary)
        drkg_DiG_temp_primary[drkg_rel] = [1] * len(drkg_DiG_temp_primary)
        DiG_res = pd.concat((DiG_res, drkg_DiG_temp_primary))
        DiG_res.loc[DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False), drkg_rel] = 1
        DiG_res['Source'] = np.where(DiG_res.duplicated(subset=['Disease', 'Gene'], keep=False),
                                    DiG_res['Source'].astype(str) + ';DRKG', DiG_res['Source'].astype(str) + '')
        DiG_res = DiG_res.drop_duplicates(subset=['Disease', 'Gene'], keep='first')
        DiG_res_col = list(DiG_res.columns)
        DiG_res_col_new = DiG_res_col[:-3] + DiG_res_col[-1:] + DiG_res_col[-3:-1]
        DiG_res = DiG_res[DiG_res_col_new]
        DiG_res_col = DiG_res_col_new[2:]
        DiG_res['Source'] = DiG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))

    DiG_res.to_csv(folder + 'DiG_res_6.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DiG_res_6: Hetionet, KEGG, CTD and DRKG (Semantic Relations).\n')
    f.close()


def main():
    integrate_Hetionet()
    integrate_KEGG()
    # extract_PharmGKB_DiG()
    integrate_PharmGKB()
    integrate_CTD_DiG_curated()
    integrate_CTD_DiG_inferred()
    integrate_DRKG_DiG()

    # DiG = pd.read_csv(res_folder + 'relation/Di_G_res_6.csv')
    # print(len(DiG), len(DiG.drop_duplicates(subset=['Disease', 'Gene'], keep='first')))
    # DiG_raw = pd.read_csv(res_folder + 'relation/Di_G_res.csv')
    # print(len(DiG_raw), len(DiG_raw.drop_duplicates(subset=['Disease', 'Gene'], keep='first')))


if __name__ == '__main__':
    main()
