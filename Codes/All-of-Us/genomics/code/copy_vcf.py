import os
import subprocess
import numpy as np
import pandas as pd

# get workspace bucket path
my_bucket = os.getenv('WORKSPACE_BUCKET')

# create directories under the analysis environment
os.system('mkdir ../dataset_vcfs')
os.system('mkdir ../dataset_unzip_vcfs')
os.system('mkdir ../sample_snp')
os.system('mkdir ../sample_gene')
# copy VCF files from the workspace bucket to the analysis environment. Please change the path to your path
os.system(f"gsutil cp '{my_bucket}/genomic-extractions/91ea1247-7c51-445a-ab64-7da849dc2de3/vcfs/0000000*vcf.gz' ../dataset_vcfs/")


