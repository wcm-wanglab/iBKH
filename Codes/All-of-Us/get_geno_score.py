import os
import io
import pandas as pd
import numpy as np
import sys

f=sys.argv[1]
group=sys.argv[2]
### for loop part

copy file to local
os.system('gsutil -m cp %s ../dataset_vcfs/' % f)

#unzip file
gz_file='../dataset_vcfs/'+f.split('/')[-1]
ungz_file='../dataset_unzip_vcfs/'+group+'_'+f.split('/')[-1].split('.')[0]+'.vcf'

os.system('gunzip -c %s > %s' % (gz_file,ungz_file))
#read file
with open(ungz_file, 'r') as ff:
    lines = [l for l in ff if not l.startswith('##')]

ff.close()
os.system('rm %s' % ungz_file)
os.system('rm %s' % gz_file)

##
#convert to dataframe
df=pd.read_csv(io.StringIO(''.join(lines)),
               dtype={'#CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str,
                      'QUAL': str, 'FILTER': str, 'INFO': str},
               sep='\t').rename(columns={'#CHROM': 'CHROM'})

#save output
sample_ids=list(df.columns[9:])

out_geno_df=pd.DataFrame(index=sample_ids)    
out_score_df=pd.DataFrame(index=sample_ids)

## search for two loci
#rs429358 19 44908684
df_line_snp1=df.loc[(df['CHROM']=='chr19') & (df['POS']==44908684)]
#rs7412 19 44908822
df_line_snp2=df.loc[(df['CHROM']=='chr19') & (df['POS']==44908822)]

####
if (len(df_line_snp1) > 0) or (len(df_line_snp2) > 0):#has such position
        #rs429358
        if (len(df_line_snp1) > 0):
            gt_all=df_line_snp1.iloc[:,9:]
            gt=[i.split(':')[0] for i in gt_all.iloc[0,:]]

            ## save genotype string
            out_geno_df['ref1']=df_line_snp1['REF'].to_string(index=False)
            out_geno_df['alt1']=df_line_snp1['ALT'].to_string(index=False)
            out_geno_df['rs429358']=gt

            #impute if missing
            missing_prop=gt.count('./.')/len(gt)
            if missing_prop < 0.1:
                if missing_prop != 0:#impute by most frequent:
                    most_common=max(set(gt), key=gt.count)
                    gt = [most_common if item == './.' else item for item in gt]

                gt_num=[]#genotype number. 0:all ref;1: 1 ref;2:no ref;

                for j in gt:
                    num=2-j.split('/').count('0')
                    gt_num.append(num)

                out_score_df['rs429358']=gt_num

            else:#delete loci if too many missings
                pass

        #rs7412
        if (len(df_line_snp2) > 0):
            gt_all=df_line_snp2.iloc[:,9:]
            gt=[i.split(':')[0] for i in gt_all.iloc[0,:]]

            ## save genotype string
            out_geno_df['ref2']=df_line_snp2['REF'].to_string(index=False)
            out_geno_df['alt2']=df_line_snp2['ALT'].to_string(index=False)
            out_geno_df['rs7412']=gt

            #impute if missing
            missing_prop=gt.count('./.')/len(gt)
            if missing_prop < 0.1:
                if missing_prop != 0:#impute by most frequent:
                    most_common=max(set(gt), key=gt.count)
                    gt = [most_common if item == './.' else item for item in gt]

                gt_num=[]#genotype number. 0:all ref;1: 1 ref;2:no ref;

                for j in gt:
                    num=2-j.split('/').count('0')
                    gt_num.append(num)

                out_score_df['rs7412']=gt_num

            else:#delete loci if too many missings
                pass
else:#neither loci were in this file
    pass

out_name=group+'_'+f.split('/')[-1].split('.')[0]
if (out_geno_df.shape[1] > 0):
    out_geno_name='../sample_geno/'+out_name+'.csv'
    out_geno_df.to_csv(out_geno_name)

if (out_score_df.shape[1] > 0):
    out_score_name='../sample_score/'+out_name+'.csv'
    out_score_df.to_csv(out_score_name)
