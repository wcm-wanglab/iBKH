import os
import io
import pandas as pd
import numpy as np

start=0
end=101

#read gwas
gwas_select=pd.read_csv('../gwas_select_p1.csv')## change the path as needed

for i in range(start,end):
    gz_file='../dataset_vcfs/'+'0000000'+str('{:03d}'.format(i))+'-interval.vcf.gz'
    ungz_file='../dataset_unzip_vcfs/'+'0000000'+str('{:03d}'.format(i))+'-interval.vcf'
    #unzip file
    os.system('gunzip -c %s > %s' % (gz_file,ungz_file))
    #read file
    with open(ungz_file, 'r') as f:
        lines = [l for l in f if not l.startswith('##')]
        
    f.close()
    os.system('rm %s' % ungz_file)
    #convert VCF to dataframe
    df=pd.read_csv(io.StringIO(''.join(lines)),
                   dtype={'#CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str,
                          'QUAL': str, 'FILTER': str, 'INFO': str},
                   sep='\t').rename(columns={'#CHROM': 'CHROM'})
    
    #save output
    sample_ids=list(df.columns[9:])
    
    out_geno_df=pd.DataFrame(columns=sample_ids)    
    out_gene_df=pd.DataFrame(columns=sample_ids)

    
    for ind in range(len(gwas_select)):#loop for each id in the reference
        chr_id=gwas_select.iloc[ind]['CHR_ID']
        chr_pos=gwas_select.iloc[ind]['CHR_POS']
        rs_id=gwas_select.iloc[ind]['SNPS']
        gene=gwas_select.iloc[ind]['MAPPED_GENE']
        
        #location of the id
        df_line=df.loc[(df['CHROM']=='chr'+str(chr_id)) & (df['POS']==int(chr_pos))]
        if (len(df_line) > 0):#if has such position
            gt_all=df_line.iloc[:,9:]
            gt=[i.split(':')[0] for i in gt_all.iloc[0,:]]
            
            #impute if missing
            missing_prop=gt.count('./.')/len(gt)
            if missing_prop < 0.1:
                if missing_prop != 0:#impute by the most frequent one
                    most_common=max(set(gt), key=gt.count)
                    gt = [most_common if item == './.' else item for item in gt]
                    
                gt_num=[]#genotype number. 0:all ref;1: 1 ref;2:no ref;
                is_gene=[]#whether the gene is mutated. 0: no 1: mutated

                for j in gt:
                    if j.split('/').count('.')==2:
                        print('wrong! missing')
                    else:
                        num=2-j.split('/').count('0')
                        gt_num.append(num)
                        is_gene.append(int(num>0))#true-1-mutated

                out_geno_df.loc[rs_id]=gt_num
                out_gene_df.loc[gene]=is_gene
                    
            else:#delete loci if too many missings
                pass
            
    
    if (out_geno_df.shape[0] > 0):
        out_geno_name='../sample_snp/0000000'+str('{:03d}'.format(i))+'.csv'
        out_geno_df.to_csv(out_geno_name)
        
    if (out_gene_df.shape[0] > 0):
        out_gene_name='../sample_gene/0000000'+str('{:03d}'.format(i))+'.csv'
        out_gene_df.to_csv(out_gene_name)

