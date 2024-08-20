## Download iBKH entities
To access the entity vocabulary in the iBKH, you can directly download the iBKH entities using the following link.
```
https://wcm.box.com/s/gagu6yj2toyk4kirb6hpsb1qu4dm203p
```

When you unzip the file, you will get the following .csv files.
```
./entity/anatomy_vocab.csv
./entity/disease_vocab.csv
./entity/drug_vocab.csv
./entity/dsp_vocab.csv
./entity/gene_vocab.csv
./entity/molecule_vocab.csv
./entity/pathway_vocab.csv
./entity/sdsi_vocab.csv
./entity/side_effect_vocab.csv
./entity/symptom_vocab.csv
./entity/tc_vocab.csv
```

## iBKH entities vocabulary
Each row in the iBKH entity vocabulary describes an entity, and each column in this row records the entity's information in different source databases (such as original ID, name, etc.). For example,
| primary | name | do_id | kegg_id | pharmgkb_id | mesh_id | umls_cui | icd_10 |	icd_9 |	omim_id |	iDISK_id | 
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DOID:10652 | alzheimer's disease | DOID:10652 |	H00056 | PA443319 | D000544 | C0002395 | G30 | 331 | ... | ... |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

The above row comes from the disease vocabulary, which describes a disease entity 'alzheimer's disease'. We can observe that the entity 'alzheimer's disease' has the following information, Disease Ontology ID (DOID:10652), KEGG ID (H00056), PharmGKB ID (PA443319), UMLS CUI (C0002395), MeSH ID (D000544), ICD-10 code (G30) and ICD-9 code (331).

| primary | symbol | hgnc_id | ncbi_id | pharmgkb_id | 
| --- | --- | --- | --- | --- |
| HGNC:5 | A1BG | 5 | 1 | PA24356 |
| ... | ... | ... | ... | ... | 

This example comes from the gene vocabulary, it describes a gene entity 'A1BG'. The corresponding information of the entity 'A1BG' has, HGNC ID (HGNC:5), NCBI ID (NCBI:1), gene symbol (A1BG) and PharmGKB ID (PA24356).

We assigned the primary ID for each type of entity, for example, we used HGNC ID as the primary ID in gene entity vocabulary. And we used the entity's primary ID to describe the entities in the relationship. For example, there is a relation 'Treats' between entities 'Donepezil' and 'Alzheimer's Disease' in the iBKH. And we used the entity Donepezil's primary ID (DrugBank:DB00843) and AD's primary ID (DOID:10652) to describe them respectively. When an entity can't find the corresponding primary ID, we will follow the primary priority order to do the mapping. For example, the NCBI ID is the second primary ID for the Gene entity vocabulary. Currently, the existing entity vocabularies have the following primary ID order:
* Gene: HGNC ID, NCBI ID
* Disease: Disease Ontology ID, KEGG ID, PharmGKB ID, MeSH ID, OMIM ID, iDISK ID
* Drug: DrugBank ID, KEGG ID, PharmGKB ID, MeSH ID, iDISK ID
* Anatomy: Uberon ID, BTO ID, MeSH ID, CL ID
* Molecule: ChEMBL ID, ChEBI ID
* Symptom: MeSH ID, UMLS CUI
* DSI: iDISK
* DSP: iDISK
* TC: UMLS CUI
* Pathway: Reactome ID, KEGG ID
* Side-Effect: UMLS CUI
