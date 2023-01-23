import pandas as pd

#combine sample x snp
all_df=[]
for i in range(757):
    file_name='../sample_snp/0000000'+str('{:03d}'.format(i))+'.csv'
    try:
        df=pd.read_csv(file_name)
        all_df.append(df)
    except:
        print(i)

result = pd.concat(all_df)

result.rename(columns={result.columns[0]:'SNPS'}, inplace=True)
result.to_csv('sample_snp_all.csv',index=False)

#combine sample x gene
all_gene_df=[]
for i in range(757):
    file_name='../sample_gene/0000000'+str('{:03d}'.format(i))+'.csv'
    try:
        df=pd.read_csv(file_name)
        all_gene_df.append(df)
    except:
        print(i)

result2 = pd.concat(all_gene_df)
result2.groupby(result2['Unnamed: 0']).sum()
result2.rename(columns={result2.columns[0]:'gene'}, inplace=True)
result2.to_csv('sample_gene_all.csv',index=False)