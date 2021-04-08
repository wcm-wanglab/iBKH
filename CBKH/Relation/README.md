## Download CBKH relations
To access the relations in the CBKH, you can directly download the CBKH relations by the following link.
```
link...
```

When you unzip the file, you will get the following .csv files.
```
./relation/A_G_res.csv
./relation/D_D_res.csv
./relation/D_Di_res.csv
./relation/D_G_res.csv
./relation/Di_Di_res.csv
./relation/Di_G_res.csv
./relation/Di_S_res.csv
./relation/DSP_SDSI_res.csv
./relation/G_G_res.csv
./relation/SDSI_Ares.csv
./relation/SDSI_D_res.csv
./relation/SDSI_Di_res.csv
./relation/SDSI_S.csv
./relation/SDSI_TC_res.csv
```

## CBKH relations
Each row in the CBKH relationship describes the relationship between a pair of entities. We kept all the relationship types from the source database in the CBKH relations tables, and binary representation exists/non-existence. 1 indicates that the relationship exists between the entity pairs, and 0 indicates that the relationship does not exist. For example,
Drug | Disease | Palliates_Hetionet | Treats_Hetionet | Effect_KEGG | Association_CTD | ... 
--- | --- | --- | --- |--- |--- |--- 
DrugBank:DB00843 | DOID:10652 | 0 | 1 | 0 | 1 | ...
... | ... | ... | ... | ... | ... | ... 

From the above record, we can observe that the entity 'Donepezil' (primary ID is DrugBank:DB00843) and the entity 'Alzheimer's Disease' (primary ID is DOID:10652) have the relation 'Treats' and 'Association', and the relations come from the Hetionet and CTD respectively.
