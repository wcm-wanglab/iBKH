# Cornell Biomedical Knowledge Hub (CBKH)
CBKG integrates data from 18 publicly available biomedical databases. The current version of CBKG contains a total of 2,384,841 entities of 11 types. Specifically, the CBKH includes 22,963 anatomy entities, 18,817 disease entities, 36,759 drug entities, 90,431 gene entities, 2,065,015 molecule entities, 1,361 symptom entities, 4,101 DSI entities, 137,568 DSP entities, 605 TC entities, 2,970 pathway entities and 4,251 side-effect entities. For the relationships in the CBKG (Table 3), there are 101 relation types within 18 kinds of entity pairs, including Anatomy-Gene, Drug-Disease, Drug-Drug, Drug-Gene, Disease-Disease, Disease-Gene, Disease-Symptom, Gene-Gene, DSI-Disease, DSI-Symptom, DSI-Drug, DSI-Anatomy, DSI-DSP, DSI-TC, Disease-Pathway, Drug-Pathway, Gene-Pathway and Drug-Side Effect. In total, CBKH contains 49,709,611 relations.

![Schema](KG_Schema.png)

## Materials and Methods
Our ultimate goal was to build a biomedical knowledge graph via comprehensively incorporating biomedical knowledge as much as possible. To this end, we collected and integrated 18 publicly available data sources to curate a comprehensive one. Details of the used data resources were listed in [Table](https://github.com/houyurain/CBKH/blob/main/Source%20Information/README.md).

## Statistics of CBKH
| Entity Type    | Number    | Included Identifiers |
| ---------------|:---------:|:--------------------:|
| Anatomy        | 22,963    | Uberon ID, BTO ID, MeSH ID, Cell Ontology ID |
| Disease        | 18,817    | Disease Ontology ID, KEGG ID, PharmGKB ID, MeSH ID, OMIM ID |
| Drug           | 36,759    | DrugBank ID, KEGG ID, PharmGKB ID, MeSH ID |
| Gene           | 90,431    | HGNC ID, NCBI ID, PharmGKB ID |
| Molecule       | 2,065,015 | CHEMBL ID, CHEBI ID |
| Symptom        | 1,361       | MeSH ID |
| Dietary Supplement Ingredient |	4,101	| iDISK ID |
| Dietary Supplement Product |	137,568 |	iDISK ID |
| Therapeutic Class |	605 |	iDISK ID, UMLS CUI |
| Pathway | 2,970 | Reactome ID, KEGG ID |
| Side-Effect | 4,251 | UMLS CUI |
| **Total Entities** | **2,384,841** | - |

| Relation Type   |	Number     |
| ----------------|:----------:|
| Anatomy-Gene	  | 12,825,270 |
| Drug-Disease	  | 2,711,848  |
| Drug-Drug	      | 2,684,682  |
| Drug-Gene	      | 1,295,088  |
| Disease-Disease	| 11,072     |
| Disease-Gene	  | 27,541,618 |
| Disease-Symptom	| 3,357      |
| Gene-Gene	      | 1,605,716  |
| DSI-Symptom     |	2,093      |
| DSI-Disease	    | 5,134      |
| DSI-Drug        | 3,057      |
| DSI-Anatomy     |	4,334      |
| DSP-DSI         |	689,297    |
| DSI-TC          |	5,430      |
| Disease-Pathway | 1,942      |
| Drug-Pathway    | 3,231      |
| Gene-Pathway    | 153,236    |
| Drug-Side Effect| 163,206    |
| **Total Relations** | **49,709,611** |

## Licence
The data of CBKG is licensed under the [MIT License](https://github.com/houyurain/CBKH/blob/main/LICENSE). The CBKH integrated the data from many resources, and users should consider the licenses for each of them (see the detail in the [table](https://github.com/houyurain/CBKH/blob/main/Source%20Information/README.md)). 

## Cite
```
@article{su2021cbkh,
  title={CBKH: The Cornell Biomedical Knowledge Hub},
  author={Su, Chang and Hou, Yu and Guo, Winston and Chaudhry, Fayzan and Ghahramani, Gregory and Zhang, Haotan and Wang, Fei},
  journal={medRxiv},
  year={2021},
  publisher={Cold Spring Harbor Laboratory Press}，
  url = {https://www.medrxiv.org/content/10.1101/2021.03.12.21253461v1}
}
```

