import sys
import os
import subprocess

##### input
datase_dir=sys.argv[1]
group=sys.argv[2]
##bucket path
#DATASET_11433767_VCF_DIR='gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/f27e309a-6692-4069-954c-0f65c133a01c/vcfs'
#group='age65_m'

###
all_files=subprocess.check_output(f"gsutil ls -r {datase_dir}", shell=True).decode('utf-8')
all_files_list=all_files.split('\n')

#all_files_list=all_files_list[:10]
num=70#for iteration

n=0
for f in all_files_list:	
	if f.endswith('.gz'):
		n+=1
		#function
		if n%num!=0:
			os.system('python get_geno_score.py %s % s&' % (f,group))
		else:
			os.system('python get_geno_score.py %s %s ' % (f,group))
	else:
		pass
