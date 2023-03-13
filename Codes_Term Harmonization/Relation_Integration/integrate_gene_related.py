import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

folder = ''


def integrate_Hetionet_GG():
    hetionet_GG = pd.read_csv(folder + '/hetionet_GG.csv')
    hetionet_GG = hetionet_GG.rename(columns={'source': 'Gene_1', 'target': 'Gene_2'})
    hetionet_GG['Gene_1'] = hetionet_GG['Gene_1'].str.replace('Gene::', '')
    hetionet_GG['Gene_2'] = hetionet_GG['Gene_2'].str.replace('Gene::', '')

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    hetionet_GcG = hetionet_GG[hetionet_GG['metaedge'] == 'GcG']
    print(hetionet_GcG)
    hetionet_GcG = hetionet_GcG.replace({'Gene_1': ncbi_primary_dict, 'Gene_2': ncbi_primary_dict})
    hetionet_GcG = hetionet_GcG[['Gene_1', 'Gene_2']]
    print(hetionet_GcG)
    hetionet_GcG['Covaries'] = [1] * len(hetionet_GcG)
    hetionet_GcG['Interacts'] = [0] * len(hetionet_GcG)

    hetionet_GiG = hetionet_GG[hetionet_GG['metaedge'] == 'GiG']
    hetionet_GiG = hetionet_GiG.replace({'Gene_1': ncbi_primary_dict, 'Gene_2': ncbi_primary_dict})
    hetionet_GiG = hetionet_GiG[['Gene_1', 'Gene_2']]
    hetionet_GiG['Covaries'] = [0] * len(hetionet_GiG)
    hetionet_GiG['Interacts'] = [1] * len(hetionet_GiG)

    GG_res = pd.concat((hetionet_GcG, hetionet_GiG))
    GG_res.loc[GG_res.duplicated(subset=['Gene_1', 'Gene_2'], keep=False), 'Interacts'] = 1
    GG_res = GG_res.drop_duplicates(subset=['Gene_1', 'Gene_2'], keep='first')

    GG_res['Regulates'] = [0] * len(GG_res)

    hetionet_GrG = hetionet_GG[hetionet_GG['metaedge'] == 'Gr>G']
    hetionet_GrG = hetionet_GrG.replace({'Gene_1': ncbi_primary_dict, 'Gene_2': ncbi_primary_dict})
    hetionet_GrG = hetionet_GrG[['Gene_1', 'Gene_2']]
    hetionet_GrG['Covaries'] = [0] * len(hetionet_GrG)
    hetionet_GrG['Interacts'] = [0] * len(hetionet_GrG)
    hetionet_GrG['Regulates'] = [1] * len(hetionet_GrG)

    GG_res = pd.concat((GG_res, hetionet_GrG))
    GG_res.loc[GG_res.duplicated(subset=['Gene_1', 'Gene_2'], keep=False), 'Regulates'] = 1
    GG_res = GG_res.drop_duplicates(subset=['Gene_1', 'Gene_2'], keep='first')

    GG_res['Source'] = ['Hetionet'] * len(GG_res)
    print(GG_res)
    GG_res.to_csv(folder + '/GG_res.csv', index=False)
    with open(folder + '/integration_notes.txt', 'w') as f:
        f.write('GG_res: Hetionet (Covaries, Interacts and Regulates).\n')
    f.close()


def extract_PharmGKB_GG():
    pharmgkb_rel = pd.read_table(folder + 'pharmgkb_rel.tsv')
    pharmgkb_rel = pharmgkb_rel[pharmgkb_rel['Association'] == 'associated']
    pharmgkb_rel = pharmgkb_rel.reset_index(drop=True)
    res = pd.DataFrame(columns=['Gene_1', 'Gene_2'])
    idx = 0
    for i in range(len(pharmgkb_rel)):
        p1_id = pharmgkb_rel.loc[i, 'Entity1_id']
        p1_type = pharmgkb_rel.loc[i, 'Entity1_type']
        p2_id = pharmgkb_rel.loc[i, 'Entity2_id']
        p2_type = pharmgkb_rel.loc[i, 'Entity2_type']
        if p1_type == 'Gene' and p2_type == 'Gene':
            gene_1 = p1_id
            gene_2 = p2_id
        else:
            continue
        res.loc[idx] = [gene_1, gene_2]
        idx += 1
    res.to_csv(folder + '/pharmgkb_gene_gene.csv', index=False)


def integrate_PharmGKB_GG():
    GG_res = pd.read_csv(folder + '/GG_res.csv')
    GG_res_cols = list(GG_res.columns)[2:]
    GG_res['Associate'] = [0] * len(GG_res)

    pharmgkb_res = pd.read_csv(folder + '/pharmgkb_gene_gene.csv')

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    pharmgkb_gene_vocab = gene_vocab.dropna(subset=['pharmgkb_id'])
    pharmgkb_gene_primary_dict = pharmgkb_gene_vocab.set_index('pharmgkb_id')['primary'].to_dict()

    pharmgkb_res = pharmgkb_res.replace({'Gene_1': pharmgkb_gene_primary_dict, 'Gene_2': pharmgkb_gene_primary_dict})
    for col in GG_res_cols[:-1]:
        pharmgkb_res[col] = [0] * len(pharmgkb_res)
    pharmgkb_res['Source'] = ['PharmGKB'] * len(pharmgkb_res)
    pharmgkb_res['Associate'] = [1] * len(pharmgkb_res)
    GG_res = pd.concat((GG_res, pharmgkb_res))
    GG_res.loc[GG_res.duplicated(subset=['Gene_1', 'Gene_2'], keep=False), 'Associate'] = 1
    GG_res['Source'] = np.where(GG_res.duplicated(subset=['Gene_1', 'Gene_2'], keep=False),
                                 GG_res['Source'].astype(str) + ';PharmGKB', GG_res['Source'].astype(str) + '')
    GG_res = GG_res.drop_duplicates(subset=['Gene_1', 'Gene_2'], keep='first')
    GG_res_col = list(GG_res.columns)
    GG_res_col_new = GG_res_col[:-2] + GG_res_col[-1:] + GG_res_col[-2:-1]
    GG_res = GG_res[GG_res_col_new]
    GG_res['Source'] = GG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    GG_res.to_csv(folder + '/GG_res_2.csv', index=False)
    with open(folder + '/integration_notes.txt', 'a') as f:
        f.write('GG_res_2: Hetionet and PharmGKB (Associate).\n')
    f.close()


def integrate_DRKG_GG():
    drkg_GG = pd.read_csv(folder + '/drkg_GG.csv')
    drkg_GG = drkg_GG[(drkg_GG['source'] == 'GNBR') | (drkg_GG['source'] == 'IntAct')]
    drkg_GG = drkg_GG[['entity_1', 'relation', 'entity_2']]
    drkg_GG = drkg_GG[~((drkg_GG['entity_1'] == 'Gene::') | (drkg_GG['entity_2'] == 'Gene::'))]
    drkg_GG = drkg_GG.drop_duplicates(subset=['entity_1', 'entity_2'])
    drkg_GG = drkg_GG.reset_index(drop=True)
    drkg_GG = drkg_GG.rename(columns={'entity_1': 'Gene_1', 'entity_2': 'Gene_2'})
    drkg_GG['Gene_1'] = drkg_GG['Gene_1'].str.replace('Gene::', '')
    drkg_GG['Gene_2'] = drkg_GG['Gene_2'].str.replace('Gene::', '')
    gg_relation_list = list(drkg_GG.drop_duplicates(subset='relation', keep='first')['relation'])
    # gg_source_list = list(drkg_GG.drop_duplicates(subset='source', keep='first')['source'])
    # print(gg_relation_list)
    # print(gg_source_list)
    # print(drkg_GG.drop_duplicates(subset='relation', keep='first'))

    gene_vocab = pd.read_csv(folder + '/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    GG_res = pd.read_csv(folder + '/GG_res_2.csv')
    GG_res_cols = list(GG_res.columns)[2:]

    for drkg_rel in gg_relation_list:
        print(drkg_rel)
        GG_res[drkg_rel] = [0] * len(GG_res)
        drkg_GG_temp = drkg_GG[drkg_GG['relation'] == drkg_rel]
        drkg_GG_temp = drkg_GG_temp.replace({'Gene_1': ncbi_primary_dict, 'Gene_2': ncbi_primary_dict})
        drkg_GG_temp = drkg_GG_temp[['Gene_1', 'Gene_2']]
        for col in GG_res_cols[:-1]:
            drkg_GG_temp[col] = [0] * len(drkg_GG_temp)
        drkg_GG_temp['Source'] = ['DRKG'] * len(drkg_GG_temp)
        drkg_GG_temp[drkg_rel] = [1] * len(drkg_GG_temp)
        GG_res = pd.concat((GG_res, drkg_GG_temp))
        GG_res.loc[GG_res.duplicated(subset=['Gene_1', 'Gene_2'], keep=False), drkg_rel] = 1
        GG_res['Source'] = np.where(GG_res.duplicated(subset=['Gene_1', 'Gene_2'], keep=False),
                                    GG_res['Source'].astype(str) + ';DRKG', GG_res['Source'].astype(str) + '')
        GG_res = GG_res.drop_duplicates(subset=['Gene_1', 'Gene_2'], keep='first')
        GG_res_col = list(GG_res.columns)
        GG_res_col_new = GG_res_col[:-2] + GG_res_col[-1:] + GG_res_col[-2:-1]
        GG_res = GG_res[GG_res_col_new]
        GG_res_cols = GG_res_col_new[2:]
        GG_res['Source'] = GG_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    GG_res.to_csv(folder + '/GG_res_3.csv', index=False)
    with open(folder + '/integration_notes.txt', 'a') as f:
        f.write('GG_res_3: Hetionet, PharmGKB and DRKG.\n')
    f.close()


def integrate_GA_Bgee_present():
    Bgee_present = pd.read_csv('/processed_Bgee_present.csv')

    ncbi_df = pd.read_table('/gene2ensembl')
    ensembl_ncbi_dict = ncbi_df.set_index('Ensembl_gene_identifier')['GeneID'].to_dict()

    gene_vocab = pd.read_csv('/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    anatomy_vocab = pd.read_csv('/anatomy_res_3.csv')
    uberon_vocab = anatomy_vocab.dropna(subset=['uberon_id'])
    uberon_primary_dict = uberon_vocab.set_index('uberon_id')['primary'].to_dict()

    gene_list = []
    anatomy_list = []
    for i in range(len(Bgee_present)):
        gene_id = Bgee_present.loc[i, 'Gene ID']
        anatomy_id = Bgee_present.loc[i, 'Anatomical entity ID']

        if gene_id in ensembl_ncbi_dict:
            ncbi_id = ensembl_ncbi_dict[gene_id]
            gene_primary = ncbi_primary_dict[ncbi_id]
        else:
            continue

        anatomy_primary = uberon_primary_dict[anatomy_id]
        gene_list.append(gene_primary)
        anatomy_list.append(anatomy_primary)
        print(i + 1, '/', len(Bgee_present), 'Completed (Bgee present)...')
    GA_res = pd.DataFrame({'Gene': gene_list, 'Anatomy': anatomy_list, 'Present': [1] * len(gene_list), 'Source': ['Reactome'] * len(gene_list)})
    GA_res.to_csv('/GA_res.csv', index=False)


def integrate_GA_Bgee_absent():
    GA_res = pd.read_csv('/GA_res.csv')
    GA_res = GA_res.rename(columns={'Present': 'Express'})
    GA_res['Absent'] = [0] * len(GA_res)
    print(list(GA_res.columns))
    Bgee_absent = pd.read_csv('/processed_Bgee_absent.csv')

    ncbi_df = pd.read_table('/gene2ensembl')
    ensembl_ncbi_dict = ncbi_df.set_index('Ensembl_gene_identifier')['GeneID'].to_dict()

    gene_vocab = pd.read_csv('/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    anatomy_vocab = pd.read_csv('res/entity/anatomy_res_3.csv')
    uberon_vocab = anatomy_vocab.dropna(subset=['uberon_id'])
    uberon_primary_dict = uberon_vocab.set_index('uberon_id')['primary'].to_dict()

    gene_list = []
    anatomy_list = []
    for i in range(len(Bgee_absent)):
        gene_id = Bgee_absent.loc[i, 'Gene ID']
        anatomy_id = Bgee_absent.loc[i, 'Anatomical entity ID']

        if gene_id in ensembl_ncbi_dict:
            ncbi_id = ensembl_ncbi_dict[gene_id]
            gene_primary = ncbi_primary_dict[ncbi_id]
        else:
            continue

        anatomy_primary = uberon_primary_dict[anatomy_id]
        gene_list.append(gene_primary)
        anatomy_list.append(anatomy_primary)
        print(i + 1, '/', len(Bgee_absent), 'Completed (Bgee absent)...')
    Bgee_absent = pd.DataFrame({'Gene': gene_list, 'Anatomy': anatomy_list, 'Express': [0] * len(gene_list),
                                'Source': ['Reactome'] * len(gene_list), 'Absent': [1] * len(gene_list)})
    print(Bgee_absent)
    GA_res = pd.concat((GA_res, Bgee_absent))
    GA_res.loc[GA_res.duplicated(subset=['Gene', 'Anatomy'], keep=False), 'Absent'] = 1
    GA_res = GA_res.drop_duplicates(subset=['Gene', 'Anatomy'], keep='first')
    GA_res = GA_res[['Gene', 'Anatomy', 'Express', 'Absent', 'Source']]
    GA_res.to_csv('/GA_res_2.csv', index=False)


def integrate_GA_TISSUE():
    GA_res = pd.read_csv('/GA_res_2.csv')
    GA_res['Express_TISSUE'] = [0] * len(GA_res)
    print(list(GA_res.columns))
    tissue_df = pd.read_csv('/processed_TISSUE.csv')

    gene_vocab = pd.read_csv('/gene_vocab_2.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    anatomy_vocab = pd.read_csv('/anatomy_res_3.csv')
    bto_vocab = anatomy_vocab.dropna(subset=['bto_id'])
    bto_primary_dict = bto_vocab.set_index('bto_id')['primary'].to_dict()

    gene_list = []
    anatomy_list = []
    for i in range(len(tissue_df)):
        gene_id = tissue_df.loc[i, 'gene_id'].replace('NCBI:', '')
        anatomy_id = tissue_df.loc[i, 'tissue_id']

        gene_primary = ncbi_primary_dict[gene_id] if gene_id in ncbi_primary_dict else gene_id
        gene_list.append(gene_primary)
        anatomy_list.append(bto_primary_dict[anatomy_id])

        print(i + 1, '/', len(tissue_df), 'Completed (TISSUE)...')
    tissue_res = pd.DataFrame({'Gene': gene_list, 'Anatomy': anatomy_list, 'Express': [0] * len(gene_list),
                               'Absent': [0] * len(gene_list), 'Source': ['TISSUE'] * len(gene_list),
                               'Express_TISSUE': [1] * len(gene_list)})
    print(tissue_res)
    GA_res = pd.concat((GA_res, tissue_res))
    GA_res.loc[GA_res.duplicated(subset=['Gene', 'Anatomy'], keep=False), 'Express_TISSUE'] = 1
    GA_res['Source'] = np.where(GA_res.duplicated(subset=['Gene', 'Anatomy'], keep=False),
                                GA_res['Source'].astype(str) + ';TISSUE', GA_res['Source'].astype(str) + '')
    GA_res = GA_res.drop_duplicates(subset=['Gene', 'Anatomy'], keep='first')
    GA_res['Source'] = GA_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    GA_res['Express'] = GA_res['Express'] + GA_res['Express_TISSUE']
    GA_res.loc[GA_res['Express'] != 0, 'Express'] = 1
    GA_res_col = list(GA_res.columns)
    GA_res_col.remove('Express_TISSUE')
    GA_res = GA_res[GA_res_col]
    GA_res.to_csv('/GA_res_3.csv', index=False)


def integrate_GPwy_Reactome():
    gpwy_Reactome = pd.read_table(folder + 'NCBI2Reactome_All_Levels.txt', header=None)
    homo_Reactome = gpwy_Reactome[gpwy_Reactome[5] == 'Homo sapiens']
    homo_Reactome = homo_Reactome[homo_Reactome[0].astype(str).str.isdigit()]
    homo_Reactome[0] = homo_Reactome[0].astype(int).astype(str)
    homo_Reactome = homo_Reactome.drop_duplicates(subset=[0, 1], keep='first')
    homo_Reactome = homo_Reactome.reset_index(drop=True)
    homo_Reactome = homo_Reactome[[0, 1]]
    homo_Reactome = homo_Reactome.rename(columns={0: 'Gene', 1: 'Pathway'})
    print(homo_Reactome)
    gene_vocab = pd.read_csv(folder + 'gene_vocab.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_vocab['ncbi_id'] = ncbi_vocab['ncbi_id'].astype(int).astype(str)
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    pwy_vocab = pd.read_csv(folder + 'pathway_vocab.csv')
    reactome_vocab = pwy_vocab.dropna(subset=['reactome_id'])
    reactome_primary_dict = reactome_vocab.set_index('reactome_id')['primary'].to_dict()

    homo_Reactome = homo_Reactome.replace({'Gene': ncbi_primary_dict, 'Pathway': reactome_primary_dict})
    print(homo_Reactome)
    homo_Reactome['Reaction'] = [1] * len(homo_Reactome)
    homo_Reactome['Source'] = ['Reactome'] * len(homo_Reactome)
    homo_Reactome.to_csv(folder + 'GPwy_res.csv', index=False)


def integrate_GPwy_KEGG():
    GPwy_res = pd.read_csv(folder + 'GPwy_res.csv')
    GPwy_res['Associate'] = [0] * len(GPwy_res)

    kegg_GPwy = pd.read_csv(folder + '/kegg_gene_pathway.csv')
    kegg_GPwy = kegg_GPwy.rename(columns={'pathway_id': 'Pathway', 'ncbi_id': 'Gene'})
    kegg_GPwy = kegg_GPwy[['Gene', 'Pathway']]
    print(kegg_GPwy)
    gene_vocab = pd.read_csv(folder + 'gene_vocab.csv')
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    ncbi_primary_dict = ncbi_vocab.set_index('ncbi_id')['primary'].to_dict()

    pwy_vocab = pd.read_csv(folder + 'pathway_vocab.csv')
    kegg_vocab = pwy_vocab.dropna(subset=['kegg_id'])
    kegg_primary_dict = kegg_vocab.set_index('kegg_id')['primary'].to_dict()

    kegg_GPwy = kegg_GPwy.replace({'Gene': ncbi_primary_dict, 'Pathway': kegg_primary_dict})
    print(kegg_GPwy)
    kegg_GPwy['Reaction'] = [0] * len(kegg_GPwy)
    kegg_GPwy['Source'] = ['KEGG'] * len(kegg_GPwy)
    kegg_GPwy['Associate'] = [1] * len(kegg_GPwy)

    GPwy_res = pd.concat((GPwy_res, kegg_GPwy))
    GPwy_res.loc[GPwy_res.duplicated(subset=['Gene', 'Pathway'], keep=False), 'Associate'] = 1
    GPwy_res['Source'] = np.where(GPwy_res.duplicated(subset=['Gene', 'Pathway'], keep=False),
                                GPwy_res['Source'].astype(str) + ';KEGG', GPwy_res['Source'].astype(str) + '')
    GPwy_res = GPwy_res.drop_duplicates(subset=['Gene', 'Pathway'], keep='first')
    GPwy_res['Source'] = GPwy_res['Source'].apply(lambda x: ';'.join(sorted(set(x.split(';')))))
    GPwy_res_cols = list(GPwy_res.columns)
    GPwy_res_cols_new = GPwy_res_cols[:-2] + GPwy_res_cols[-1:] + GPwy_res_cols[-2:-1]
    GPwy_res = GPwy_res[GPwy_res_cols_new]
    GPwy_res.to_csv(folder + 'GPwy_res_2.csv', index=False)


def modify_res():
    AG_res = pd.read_csv(folder + 'A_G_res.csv')
    AG_res['Source'] = AG_res['Source'].str.replace('Reactome', 'Bgee')
    print(AG_res)
    AG_res.to_csv(folder + 'A_G_res.csv', index=False)


def main():
    # integrate_Hetionet_GG()
    # extract_PharmGKB_GG()
    # integrate_PharmGKB_GG()
    # integrate_DRKG_GG()
    # integrate_GA_Bgee_present()
    # integrate_GA_Bgee_absent()
    # integrate_GA_TISSUE()
    # integrate_GPwy_Reactome()
    # integrate_GPwy_KEGG()

    modify_res()


if __name__ == '__main__':
    main()
