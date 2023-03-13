import pandas as pd
import numpy as np

folder = ''
CTD_folder = '../CTD/'


pd.set_option('display.max_columns', None)


def integrate_Hetionet_KEGG():
    hetionet_DDi = pd.read_csv(folder + 'hetionet_DDi.csv')
    hetionet_DDi = hetionet_DDi.rename(columns={'source': 'Drug', 'target': 'Disease'})
    hetionet_DDi['Drug'] = hetionet_DDi['Drug'].str.replace('Compound::', '')
    hetionet_DDi['Disease'] = hetionet_DDi['Disease'].str.replace('Disease::', '')

    drug_vocab = pd.read_csv(folder + 'drug_vocab.csv')
    db_vocab = drug_vocab.dropna(subset=['drugbank_id'])
    db_primary_dict = db_vocab.set_index('drugbank_id')['primary'].to_dict()
    kegg_drug_vocab = drug_vocab.dropna(subset=['kegg_id'])
    kegg_drug_primary_dict = kegg_drug_vocab.set_index('kegg_id')['primary'].to_dict()

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    do_vocab = disease_vocab.dropna(subset=['do_id'])
    do_primary_dict = do_vocab.set_index('do_id')['primary'].to_dict()
    kegg_disease_vocab = disease_vocab.dropna(subset=['kegg_id'])
    kegg_disease_primary_dict = kegg_disease_vocab.set_index('kegg_id')['primary'].to_dict()

    hetionet_ddi_ctd = hetionet_DDi[hetionet_DDi['metaedge'] == 'CtD']
    hetionet_ddi_ctd = hetionet_ddi_ctd.replace({'Drug': db_primary_dict, 'Disease': do_primary_dict})
    hetionet_ddi_ctd = hetionet_ddi_ctd[['Drug', 'Disease']]
    hetionet_ddi_ctd['Treats_Hetionet'] = [1] * len(hetionet_ddi_ctd)
    hetionet_ddi_ctd['Palliates_Hetionet'] = [0] * len(hetionet_ddi_ctd)

    hetionet_ddi_cpd = hetionet_DDi[hetionet_DDi['metaedge'] == 'CpD']
    hetionet_ddi_cpd = hetionet_ddi_cpd.replace({'Drug': db_primary_dict, 'Disease': do_primary_dict})
    hetionet_ddi_cpd = hetionet_ddi_cpd[['Drug', 'Disease']]
    hetionet_ddi_cpd['Treats_Hetionet'] = [0] * len(hetionet_ddi_cpd)
    hetionet_ddi_cpd['Palliates_Hetionet'] = [1] * len(hetionet_ddi_cpd)

    DDi_res = pd.concat((hetionet_ddi_ctd, hetionet_ddi_cpd))
    DDi_res.loc[DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False), 'Palliates_Hetionet'] = 1
    DDi_res = DDi_res.drop_duplicates(subset=['Drug', 'Disease'], keep='first')

    DDi_res['Source'] = ['Hetionet'] * len(DDi_res)
    print(DDi_res)
    DDi_res['Effect_KEGG'] = [0] * len(DDi_res)
    kegg_df = pd.read_csv(folder + 'kegg_drug_disease.csv')
    kegg_df = kegg_df.rename(columns={'drug': 'Drug', 'disease': 'Disease'})
    kegg_df = kegg_df.replace({'Drug': kegg_drug_primary_dict, 'Disease': kegg_disease_primary_dict})
    kegg_df['Treats_Hetionet'] = [0] * len(kegg_df)
    kegg_df['Palliates_Hetionet'] = [0] * len(kegg_df)
    kegg_df['Source'] = ['KEGG'] * len(kegg_df)
    kegg_df['Effect_KEGG'] = [1] * len(kegg_df)

    DDi_res = pd.concat((DDi_res, kegg_df))
    DDi_res.loc[DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False), 'Effect_KEGG'] = 1
    DDi_res['Source'] = np.where(DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False),
                                DDi_res['Source'].astype(str) + ';KEGG', DDi_res['Source'].astype(str) + '')
    DDi_res = DDi_res.drop_duplicates(subset=['Drug', 'Disease'], keep='first')
    DDi_res_col = list(DDi_res.columns)
    DDi_res_col_new = DDi_res_col[:-2] + DDi_res_col[-1:] + DDi_res_col[-2:-1]
    DDi_res = DDi_res[DDi_res_col_new]
    DDi_res['Source'] = DDi_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    print(DDi_res)
    DDi_res.to_csv(folder + 'DDi_res.csv', index=False)
    with open(folder + 'integration_notes.txt', 'w') as f:
        f.write('DDi_res: Hetionet (Treats and Palliates) and KEGG (Effect).\n')
    f.close()


def extract_PharmGKB_DDi():
    pharmgkb_rel = pd.read_table(folder + 'pharmgkb_rel.tsv')
    pharmgkb_rel = pharmgkb_rel[pharmgkb_rel['Association'] == 'associated']
    pharmgkb_rel = pharmgkb_rel.reset_index(drop=True)
    res = pd.DataFrame(columns=['Drug', 'Disease'])
    idx = 0
    for i in range(len(pharmgkb_rel)):
        p1_id = pharmgkb_rel.loc[i, 'Entity1_id']
        p1_type = pharmgkb_rel.loc[i, 'Entity1_type']
        p2_id = pharmgkb_rel.loc[i, 'Entity2_id']
        p2_type = pharmgkb_rel.loc[i, 'Entity2_type']
        if p1_type == 'Chemical' and p2_type == 'Disease':
            drug = p1_id
            disease = p2_id
        elif p2_type == 'Chemical' and p1_type == 'Disease':
            drug = p2_id
            disease = p1_id
        else:
            continue
        res.loc[idx] = [drug, disease]
        idx += 1
    res.to_csv(folder + 'pharmgkb_drug_disease.csv', index=False)


def integrate_CTD_DDi_curated():
    chem_disease = pd.read_csv(CTD_folder + 'CTD_chemicals_diseases.csv', header=27)
    chem_disease = chem_disease.dropna(subset=['ChemicalID', 'DiseaseID'])
    chem_disease = chem_disease.drop_duplicates(subset=['ChemicalID', 'DiseaseID'])
    chem_disease = chem_disease.reset_index(drop=True)
    chem_disease = chem_disease.rename(columns={'ChemicalID': 'Drug', 'DiseaseID': 'Disease'})
    chem_disease_curated = chem_disease[pd.isnull(chem_disease['InferenceScore'])]

    chem_disease_curated = chem_disease_curated[['Drug', 'Disease']]
    chem_disease_curated = chem_disease_curated.reset_index(drop=True)

    drug_vocab = pd.read_csv(folder + 'drug_vocab.csv')
    mesh_drug_vocab = drug_vocab.dropna(subset=['mesh_id'])
    mesh_durg_primary_dict = mesh_drug_vocab.set_index('mesh_id')['primary'].to_dict()

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    mesh_disease_vocab = disease_vocab.dropna(subset=['mesh_id'])
    mesh_disease_primary_dict = mesh_disease_vocab.set_index('mesh_id')['primary'].to_dict()
    omim_vocab = disease_vocab.dropna(subset=['omim_id'])
    omim_vocab['omim_id'] = omim_vocab['omim_id'].astype(int).astype(str)
    omim_primary_dict = omim_vocab.set_index('omim_id')['primary'].to_dict()

    DDi_res = pd.read_csv(folder + 'DDi_res.csv')
    DDi_res_col = list(DDi_res.columns)[2:]
    DDi_res['Associate_CTD'] = [0] * len(DDi_res)

    for i in range(len(chem_disease_curated)):
        drug_id = chem_disease_curated.loc[i, 'Drug']
        disease_id = chem_disease_curated.loc[i, 'Disease']

        chem_disease_curated.loc[i, 'Drug'] = mesh_durg_primary_dict[drug_id]
        if 'MESH' in disease_id:
            disease_id = disease_id.replace('MESH:', '')
            chem_disease_curated.loc[i, 'Disease'] = mesh_disease_primary_dict[disease_id]
        else:
            disease_id = disease_id.replace('OMIM:', '')
            chem_disease_curated.loc[i, 'Disease'] = omim_primary_dict[disease_id]
        print(i + 1, '/', len(chem_disease_curated), 'Completed...')
    print(chem_disease_curated)

    for col in DDi_res_col[:-1]:
        chem_disease_curated[col] = [0] * len(chem_disease_curated)
    chem_disease_curated['Source'] = ['CTD'] * len(chem_disease_curated)
    chem_disease_curated['Associate_CTD'] = [1] * len(chem_disease_curated)
    DDi_res = pd.concat((DDi_res, chem_disease_curated))
    DDi_res.loc[DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False), 'Associate_CTD'] = 1
    DDi_res['Source'] = np.where(DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False),
                                DDi_res['Source'].astype(str) + ';CTD', DDi_res['Source'].astype(str) + '')
    DDi_res = DDi_res.drop_duplicates(subset=['Drug', 'Disease'], keep='first')
    DDi_res_col = list(DDi_res.columns)
    DDi_res_col_new = DDi_res_col[:-2] + DDi_res_col[-1:] + DDi_res_col[-2:-1]
    DDi_res = DDi_res[DDi_res_col_new]
    DDi_res['Source'] = DDi_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DDi_res.to_csv(folder + 'DDi_res_2.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DDi_res_2: Hetionet, KEGG and CTD (Associate).\n')
    f.close()


def integrate_CTD_DDi_inferred():
    DDi_res = pd.read_csv(folder + 'DDi_res_2.csv')
    DDi_res_col = list(DDi_res.columns)[2:]
    DDi_res['Inferred_Relation'] = [0] * len(DDi_res)
    DDi_res['Inference_Score'] = [''] * len(DDi_res)

    chem_disease_inferred = pd.read_csv(folder + 'CTD_chem_disease_inferred.csv')

    for col in DDi_res_col[:-1]:
        chem_disease_inferred[col] = [0] * len(chem_disease_inferred)
    chem_disease_inferred['Source'] = ['CTD'] * len(chem_disease_inferred)
    chem_disease_inferred['Inferred_Relation'] = [1] * len(chem_disease_inferred)
    temp_col = list(chem_disease_inferred.columns)
    chem_disease_inferred_col = temp_col[:2] + temp_col[3:] + temp_col[2:3]
    chem_disease_inferred = chem_disease_inferred[chem_disease_inferred_col]
    print(list(chem_disease_inferred.columns))
    DDi_res = pd.concat((DDi_res, chem_disease_inferred))
    DDi_res.loc[DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False), 'Inferred_Relation'] = 1
    DDi_res['Source'] = np.where(DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False),
                                 DDi_res['Source'].astype(str) + ';CTD', DDi_res['Source'].astype(str) + '')
    DDi_res = DDi_res.drop_duplicates(subset=['Drug', 'Disease'], keep='first')
    DDi_res_col = list(DDi_res.columns)
    DDi_res_col_new = DDi_res_col[:-3] + DDi_res_col[-2:-1] + DDi_res_col[-3:-2] + DDi_res_col[-1:]
    DDi_res = DDi_res[DDi_res_col_new]
    DDi_res['Source'] = DDi_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    DDi_res.to_csv(folder + 'DDi_res_3.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DDi_res_3: Hetionet, KEGG and CTD (Inferred_Relation).\n')
    f.close()


def integrate_DRKG_DDi():
    DDi_res = pd.read_csv(folder + 'DDi_res_3.csv')
    DDi_res_col = list(DDi_res.columns)[2:]

    drkg_DDi = pd.read_csv('drkg_DDi.csv')
    # drkg_DDi = pd.read_csv('drkg_DDi.csv')
    drkg_DDi = drkg_DDi.rename(columns={'entity_1': 'Drug', 'entity_2': 'Disease'})
    drkg_DDi['Drug'] = drkg_DDi['Drug'].str.replace('Compound::', '')
    drkg_DDi['Disease'] = drkg_DDi['Disease'].str.replace('Disease::', '')
    ddi_relation_list = list(drkg_DDi.drop_duplicates(subset='relation', keep='first')['relation'])
    # ddi_source_list = list(drkg_DDi.drop_duplicates(subset='source', keep='first')['source'])
    # print(ddi_relation_list)
    # print(ddi_source_list)
    # print(drkg_DDi.drop_duplicates(subset='relation', keep='first'))

    drug_vocab = pd.read_csv(folder + 'drug_vocab.csv')
    db_vocab = drug_vocab.dropna(subset=['drugbank_id'])
    db_primary_dict = db_vocab.set_index('drugbank_id')['primary'].to_dict()
    mesh_drug_vocab = drug_vocab.dropna(subset=['mesh_id'])
    mesh_durg_primary_dict = mesh_drug_vocab.set_index('mesh_id')['primary'].to_dict()

    disease_vocab = pd.read_csv(folder + 'disease_vocab.csv')
    mesh_disease_vocab = disease_vocab.dropna(subset=['mesh_id'])
    mesh_disease_primary_dict = mesh_disease_vocab.set_index('mesh_id')['primary'].to_dict()
    omim_vocab = disease_vocab.dropna(subset=['omim_id'])
    omim_vocab['omim_id'] = omim_vocab['omim_id'].astype(int).astype(str)
    omim_primary_dict = omim_vocab.set_index('omim_id')['primary'].to_dict()

    for drkg_rel in ddi_relation_list:
        print(drkg_rel)
        DDi_res[drkg_rel] = [0] * len(DDi_res)
        drkg_DDi_temp = drkg_DDi[drkg_DDi['relation'] == drkg_rel]
        drkg_DDi_temp = drkg_DDi_temp[['Drug', 'Disease']]
        drkg_DDi_temp = drkg_DDi_temp.reset_index(drop=True)
        drkg_DDi_temp_primary = pd.DataFrame(columns=['Drug', 'Disease'])
        idx = 0
        for i in range(len(drkg_DDi_temp)):
            drug_id = drkg_DDi_temp.loc[i, 'Drug']
            disease_id = drkg_DDi_temp.loc[i, 'Disease']

            if 'DB' in drug_id:
                drug_id_primary = db_primary_dict[drug_id]
            elif 'MESH' in drug_id:
                drug_id = drug_id.replace('MESH:', '')
                if drug_id in mesh_durg_primary_dict:
                    drug_id_primary = mesh_durg_primary_dict[drug_id]
                else:
                    continue
            else:
                continue

            if 'MESH' in disease_id:
                disease_id = disease_id.replace('MESH:', '')
                disease_id_primary = mesh_disease_primary_dict[disease_id]
            else:
                disease_id = disease_id.replace('OMIM:', '')
                disease_id_primary = omim_primary_dict[disease_id]

            drkg_DDi_temp_primary.loc[idx] = [drug_id_primary, disease_id_primary]
            idx += 1
        for col in DDi_res_col[:-2]:
            drkg_DDi_temp_primary[col] = [0] * len(drkg_DDi_temp_primary)
        drkg_DDi_temp_primary['Source'] = ['DRKG'] * len(drkg_DDi_temp_primary)
        drkg_DDi_temp_primary['Inference_Score'] = [''] * len(drkg_DDi_temp_primary)
        drkg_DDi_temp_primary[drkg_rel] = [1] * len(drkg_DDi_temp_primary)
        DDi_res = pd.concat((DDi_res, drkg_DDi_temp_primary))
        DDi_res.loc[DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False), drkg_rel] = 1
        DDi_res['Source'] = np.where(DDi_res.duplicated(subset=['Drug', 'Disease'], keep=False),
                                    DDi_res['Source'].astype(str) + ';DRKG', DDi_res['Source'].astype(str) + '')
        DDi_res = DDi_res.drop_duplicates(subset=['Drug', 'Disease'], keep='first')
        DDi_res_col = list(DDi_res.columns)
        DDi_res_col_new = DDi_res_col[:-3] + DDi_res_col[-1:] + DDi_res_col[-3:-1]
        DDi_res = DDi_res[DDi_res_col_new]
        DDi_res_col = DDi_res_col_new[2:]
        DDi_res['Source'] = DDi_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))

    DDi_res = DDi_res.rename(columns={'Compound treats the disease': 'Treats_DRKG'})
    DDi_res.to_csv(folder + 'DDi_res_4.csv', index=False)
    with open(folder + 'integration_notes.txt', 'a') as f:
        f.write('DDi_res_4: Hetionet, KEGG, CTD and DRKG (Treats and Semantic Relations).\n')
    f.close()


def main():
    # integrate_Hetionet_KEGG()
    # extract_PharmGKB_DDi()
    integrate_CTD_DDi_curated()
    integrate_CTD_DDi_inferred()
    integrate_DRKG_DDi()


if __name__ == '__main__':
    main()
