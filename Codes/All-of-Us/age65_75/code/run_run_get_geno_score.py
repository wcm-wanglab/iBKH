import pickle
import os

if not os.path.exists('../dataset_vcfs'):
    os.makedirs('../dataset_vcfs')

if not os.path.exists('../dataset_unzip_vcfs'):
    os.makedirs('../dataset_unzip_vcfs')

if not os.path.exists('../sample_score'):
    os.makedirs('../sample_score')

if not os.path.exists('../sample_geno'):
    os.makedirs('../sample_geno')

with open('../dataset_path.pkl', 'rb') as f:
    dataset_path = pickle.load(f)

keys=dataset_path.keys()

group='age75_m'
path=dataset_path[group]
os.system('python run_get_geno_score.py %s %s &' % (path,group))

group='age75_f'
path=dataset_path[group]
os.system('python run_get_geno_score.py %s %s' % (path,group))
