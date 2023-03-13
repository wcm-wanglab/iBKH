import pandas as pd

folder = ''


def extract_SIDER():
    sider_df = pd.read_table(folder + 'entity/side_effect/meddra_all_se.tsv', header=None)
    sider_df = sider_df[sider_df[3] == 'PT']
    sider_df = sider_df[[0, 4, 5]]
    sider_df = sider_df.rename(columns={0: 'CID', 4: 'umls_cui', 5: 'name'})
    sider_df = sider_df.reset_index(drop=True)
    print(sider_df)
    res = sider_df[['umls_cui', 'name']]
    res = res.drop_duplicates(subset='umls_cui', keep='first')
    res['primary'] = 'UMLS:' + res['umls_cui'].astype(str)
    res = res[['primary', 'umls_cui', 'name']]
    print(res)
    res.to_csv(folder + 'entity/side_effect/side_effect_vocab.csv', index=False)


def main():
    extract_SIDER()


if __name__ == '__main__':
    main()
