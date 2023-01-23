import pandas as pd
import re
import sys

input_f=sys.argv[1]#gwas reference path
output_f=sys.argv[2]# output path to save filtered gwas

pv=1#cut off p value, default 1
gwas=pd.read_csv(input_f)
gwas_select=gwas.loc[gwas['P-VALUE']<pv,:][['CHR_ID','CHR_POS','SNPS','MAPPED_GENE']]#save SNP information and mapped genes, remove other information

gwas_sub=[]
for i in range(gwas_select.shape[0]):
    chr_id=gwas_select.iloc[i]['CHR_ID']
    pos=gwas_select.iloc[i]['CHR_POS']
    rs_id=gwas_select.iloc[i]['SNPS']
    genes=gwas_select.iloc[i]['MAPPED_GENE']

    if pd.isnull(chr_id) or pd.isnull(pos) or pd.isnull(genes):#remove null positions
        pass
    elif (';' in chr_id) or (';' in pos) or (';' in rs_id):
        ids=chr_id.split(';')
        poss=pos.split(';')
        rss=rs_id.split(';')
        gs=re.split('; |, | - |;|,',genes)
        if 'NA' in gs:
            gs.remove('NA')
        for j in range(len(ids)):
            gwas_sub.append([ids[j],poss[j],rss[j],gs[j]])
    else:#multiple gene for single id
        gs=re.split('; |, | - |;|,',genes)
        if 'NA' in gs:
            gs.remove('NA')
        for j in range(len(gs)):
            gwas_sub.append([chr_id,pos,rs_id,gs[j]])

gwas_sub_df=pd.DataFrame(gwas_sub)
gwas_sub_df.columns =['CHR_ID','CHR_POS','SNPS','MAPPED_GENE']
gwas_sub_df=gwas_sub_df.drop_duplicates()
gwas_sub_df=gwas_sub_df.reset_index(drop=True)
gwas_sub_df=gwas_sub_df.sort_values('MAPPED_GENE')

#gwas_sub_df.to_csv('../gwas_select_p'+str(pv)+'.csv',index=False)
gwas_sub_df.to_csv(output_f,index=False)