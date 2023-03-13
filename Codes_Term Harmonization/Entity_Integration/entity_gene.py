import pandas as pd


folder = ''
res_folder = ''


def remove_duplicated_ncbi():
    gene_vocab = pd.read_csv(folder + 'entity/gene_vocab.csv')
    gene_vocab = gene_vocab[['primary', 'symbol', 'hgnc_id', 'ncbi_id']]
    # gene_vocab = gene_vocab.drop_duplicates(subset='ncbi_id', keep='first')
    gene_vocab = gene_vocab[(~gene_vocab.duplicated(subset='ncbi_id')) | (gene_vocab['ncbi_id'].isnull())]
    print(len(gene_vocab), len(gene_vocab.drop_duplicates(subset='primary', keep='first')))
    hgnc_vocab = gene_vocab.dropna(subset=['hgnc_id'])
    print(len(hgnc_vocab), len(hgnc_vocab.drop_duplicates(subset='hgnc_id', keep='first')))
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    print(len(ncbi_vocab), len(ncbi_vocab.drop_duplicates(subset='ncbi_id', keep='first')))
    print(gene_vocab)
    gene_vocab.to_csv(res_folder + 'gene_vocab.csv', index=False)


def add_PharmGKB_gene():
    gene_vocab = pd.read_csv(res_folder + 'gene_vocab.csv')
    gene_vocab['pharmgkb_id'] = [''] * len(gene_vocab)
    idx = len(gene_vocab)

    hgnc_vocab = gene_vocab.dropna(subset=['hgnc_id'])
    hgnc_vocab['hgnc_id'] = hgnc_vocab['hgnc_id'].astype(int).astype(str)
    ncbi_vocab = gene_vocab.dropna(subset=['ncbi_id'])
    hgnc_list = list(hgnc_vocab['hgnc_id'])
    ncbi_list = list(ncbi_vocab['ncbi_id'])

    pharmgkb_gene = pd.read_table(res_folder + 'pharmgkb_gene.tsv')
    for i in range(len(pharmgkb_gene)):
        p_id = pharmgkb_gene.loc[i, 'PharmGKB Accession Id']
        hgnc_id = pharmgkb_gene.loc[i, 'HGNC ID']
        ncbi_id = pharmgkb_gene.loc[i, 'NCBI Gene ID']
        symbol = pharmgkb_gene.loc[i, 'Symbol']

        if not pd.isnull(hgnc_id):
            hgnc_id = hgnc_id.replace('HGNC:', '')
            if hgnc_id in hgnc_list:
                gene_vocab.loc[gene_vocab['hgnc_id'] == int(hgnc_id), 'pharmgkb_id'] = p_id
        elif not pd.isnull(ncbi_id):
            if ncbi_id in ncbi_list:
                gene_vocab.loc[gene_vocab['ncbi_id'] == ncbi_id, 'pharmgkb_id'] = p_id
        else:
            gene_vocab.loc[idx] = ['PharmGKB:' + p_id, symbol, '', '', p_id]
            idx += 1
        print(i + 1, '/', len(pharmgkb_gene), 'Completed...')
    print(gene_vocab)
    gene_vocab.to_csv(res_folder + 'gene_vocab_2.csv', index=False)


def add_ensembl():
    gene_vocab = pd.read_csv(folder + 'entity/gene_vocab.csv')

    ensembl_df = pd.read_table(res_folder + 'gene2ensembl_May_3')
    ncbi_ensembl_dict = ensembl_df.set_index('GeneID')['Ensembl_gene_identifier'].to_dict()
    # ncbi_protein_dict = ensembl_df.set_index('GeneID')['Ensembl_protein_identifier'].to_dict()
    # print(gene_vocab)
    # print(ncbi_ensembl_dict[100527964], ncbi_protein_dict[100527964])
    ensembl_list = []
    for i in range(len(gene_vocab)):
        ncbi_id = gene_vocab.loc[i, 'ncbi_id']
        ensembl_id = ncbi_ensembl_dict[ncbi_id] if ncbi_id in ncbi_ensembl_dict else ''
        ensembl_list.append(ensembl_id)
        print(i + 1, '/', len(gene_vocab), 'Completed...')
    gene_vocab['ensembl_id'] = ensembl_list
    print(gene_vocab)
    gene_vocab.to_csv(res_folder + 'gene_vocab_3.csv', index=False)


def main():
    # remove_duplicated_ncbi()
    # add_PharmGKB_gene()

    add_ensembl()


if __name__ == '__main__':
    main()
