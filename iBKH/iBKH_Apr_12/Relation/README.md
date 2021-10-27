## Download iBKH relations
To access the relations in the iBKH, you can directly download the iBKH relations by the following link.

```
https://wcm.box.com/s/fzzsx9ldj8a64jsa04hyf8khple7js7n
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

## iBKH relations
Each row in the iBKH relationship describes the relationship between a pair of entities. We kept all the relationship types from the source database in the iBKH relations tables, and use binary to express exist/non-exist. 1 indicates that the relationship exists between the entity pairs, and 0 indicates that the relationship does not exist. For example,
Drug | Disease | Palliates | Treats | Effect | Association | Source 
--- | --- | --- | --- |--- |--- |--- 
DrugBank:DB00843 | DOID:10652 | 0 | 1 | 0 | 1 | Hetionet; CTD 
... | ... | ... | ... | ... | ... | ... 

From the above record, we can observe that the entity 'Donepezil' (primary ID is DrugBank:DB00843) and the entity 'Alzheimer's Disease' (primary ID is DOID:10652) have the relation 'Treats' and 'Association', and the relations come from the Hetionet and CTD respectively.
