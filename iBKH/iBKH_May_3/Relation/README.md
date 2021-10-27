## Download iBKH relations
To access the relations in the iBKH, you can directly download the iBKH relations by the following link.

```
https://wcm.box.com/s/dcq6lj4vxzs4rnxu6xx60ziwl62qrzyp
```

When you unzip the file, you will get the following .csv files.
```
./relation/A_G_res.csv
./relation/D_D_res.csv
./relation/D_Di_res.csv
./relation/D_G_res.csv
./relation/D_Pwy_res.csv
./relation/D_SE_res.csv
./relation/Di_Di_res.csv
./relation/Di_G_res.csv
./relation/Di_Pwy_res.csv
./relation/Di_Sy_res.csv
./relation/DSP_SDSI_res.csv
./relation/G_G_res.csv
./relation/G_Pwy_res.csv
./relation/SDSI_A_res.csv
./relation/SDSI_D_res.csv
./relation/SDSI_Di_res.csv
./relation/SDSI_Sy.csv
./relation/SDSI_TC_res.csv
```

## iBKH relations
Each row in the iBKH relationship describes the relationship between a pair of entities. We kept all the relationship types from the source database in the iBKH relations tables, and use binary to express exist/non-exist. 1 indicates that the relationship exists between the entity pairs, and 0 indicates that the relationship does not exist. The inference score reflects the degree of similarity between the drug-disease network in the CTD inferred relationship. The triplets will be assigned an inference score when the triplets are only an inferred relation from the CTD. 

| Drug | Disease | Treats | Palliates | Effect | Associate | Inferred_Relation | ... | Source | Inference_Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DrugBank:DB00843 | DOID:10652 | 1 | 0 | 1 | 1 | 0 | ... | CTD;DRKG;Hetionet;KEGG | ... |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

From the above example record, we can observe that the entity 'Donepezil' (primary ID is DrugBank:DB00843) and the entity 'Alzheimer's Disease' (primary ID is DOID:10652) have the relation 'Treats' and 'Association', and the relations come from the Hetionet and CTD curated relation respectively.

| Drug | Disease | Treats | Palliates | Effect | Associate | Inferred_Relation | ... | Source | Inference_Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DrugBank:DB06767 | DOID:10283 | 0 | 0 | 0 | 0 | 1 | ... |CTD | 342.19 |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

From the above example record, we can observe that the entity 'Ammonium chloride' (primary ID is DrugBank: DB06767) and the entity 'Prostate cancer' (primary ID is DOID:10283) have the relation from the CTD inferred relation. The relation is assigned an inference score (342.19).
