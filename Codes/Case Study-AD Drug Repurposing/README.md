# Alzheimer's Disease Drugs Repurposing via pre-trained embedding

This is an example showing that conduct AD drug repurposing by using iBKH with pre-trained embedding information. We used pre-trained iBKH embedding information from different models as input, then predicted the most likely associated entity with Alzheimer's Disease (AD).

You can find the embedding data from different models and the drug list with the ground truth in the following link. 
https://wcm.box.com/s/jbh90entaed2jotvyab8wjsrprs4i5i8

When you download the data, you will see the following files:
```
./Data
./Data/ComplEx
./Data/ComplEx/entities.tsv
./Data/ComplEx/relations.tsv
./Data/ComplEx/iBKH_ComplEx_entity.npy
./Data/ComplEx/iBKH_ComplEx_relation.npy
./Data/DistMult
./Data/DistMult/entities.tsv
./Data/DistMult/relations.tsv
./Data/DistMult/iBKH_DistMult_entity.npy
./Data/DistMult/iBKH_DistMult_relation.npy
./Data/TransE_l2
./Data/TransE_l2/entities.tsv
./Data/TransE_l2/relations.tsv
./Data/TransE_l2/iBKH_TransE_l2_entity.npy
./Data/TransE_l2/iBKH_TransE_l2_relation.npy
./Data/TransR
./Data/TransR/entities.tsv
./Data/TransR/relations.tsv
./Data/TransR/iBKH_TransR_entity.npy
./Data/TransR/iBKH_TransR_relation.npy
./Data/Drug_list
./Data/Drug_list/drugs_list_approve_phase1234.csv
./Data/Drug_list/drugs_list_approve_phase234.csv
./Data/Drug_list/drugs_list_approve_phase34.csv
./Data/Drug_list/drugs_list_approve_phase4.csv
./Data/Drug_list/drugs_list_approve.csv
```

When you run the code, the code Case_Study-AD-Drug-Repurposing.ipynb and the Data folder should in this structure.
