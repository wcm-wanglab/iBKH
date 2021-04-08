## Download CBKH entities
To access the entity vocabulary in the CBKH, you can directly download the CBKH entities by the following link.
```
link...
```

When you unzip the file, you will get the following .csv files.
```
./entity/anatomy_vocab.csv
./entity/disease_vocab.csv
./entity/drug_vocab.csv
./entity/dsp_vocab.csv
./entity/gene_vocab.csv
./entity/molecule_vocab.csv
./entity/sdsi_vocab.csv
./entity/symptom_vocab.csv
./entity/tc_vocab.csv
```

## CBKH entities vocabulary
Each row in the CBKH entity vocabulary describes an entity, and each column in this row records the entity's information in different source databases (such as original ID, name, etc.). For example,
primary | do_id | do_name | kegg_id | kegg_name | pharmgkb_id | pharmgkb_name | umls_cui | mesh_id | ... 
--- | --- | --- | --- |--- |--- |--- |--- |--- |--- 
DOID:0001816 | 0001816 | angiosarcoma | H01666 | Angiosarcoma | PA444390 | Hemangiosarcoma | C0018923 | D006394 | ...
... | ... | ... | ... | ... | ... | ... | ... | ... | ... 

The above row comes from the disease vocabulary, which describes the entity 'Angiosarcoma'. We can observe that the entity 'Angiosarcoma' has the following information, Disease Ontology ID (DOID:0001816), KEGG ID (H01666), PharmGKB ID (PA444390), the name in PharmGKB ('Hemangiosarcoma'), UMLS CUI (C0018923) and MeSH ID (D006394).
primary | hgnc_id | symbol | ncbi_id | pharmgkb_id | ... 
--- | --- | --- | --- | --- | --- 
HGNC:5 | 5 | A1BG | H01666 | 1 | PA24356 | ...
... | ... | ... | ... | ... | ... 

