# iBKH Case Study
## Overview
We enable high-quality knowledge discovery based on iBKH. We developed a knowledge discovery module based on [DGL-KE (Deep Graph Library – Knowledge Embedding)](https://github.com/awslabs/dgl-ke), a Python package for efficient and scalable graph learning. To demonstrate its potentials, we conducted two proof-of-concept studies: 1) Case Study I: in-silico hypothesis generation for Alzheimer’s disease (AD) drug repurposing, and 2) Case Study II: knowledge-enhanced cohort exploration for older adults with Apolipoprotein E (APOE) ε4 genotype (a significant genetic risk factor of AD).

## Python Dependencies
The codes mainly depends on the scientific stacks on the Python 3.7.
```
numpy 1.21.5
pandas 1.3.5
torch 1.2.0 (https://pytorch.org/)
sklearn 0.0
neo4j 5.2.0 (https://pypi.org/project/neo4j/5.2.0/)
matplotlib 3.1.1
statsmodels 0.11.1
```

## DGL-KE Platform for iBKH Setup
In this work, we used the [Deep Graph Library - Knowledge Embedding (DGL-KE)](https://github.com/awslabs/dgl-ke), a Python-based implementation for the advanced KGE algorithms, such as TransE, TransR, ComplEx, and DistMult. You may follow the [Installation Guide](https://dglke.dgl.ai/doc/install.html) to complete the DGL-KE installation.

## Case Study - Alzheimer's Disease (AD) drug repurposing
This is the implementation of AD drug repurposing based on iBKH. The task is to dicover drugs that potentially link to AD in the iBKH. Detailed information and codes can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Case_Study-AD_Drug_Repurposing.ipynb).

<!--## Case Study I
This is the implementation of Alzheimer's Disease (AD) drug repurposing based on iBKH. The task is to dicover drugs that potentially link to AD in the iBKH. Detailed information and codes can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Case_Study-AD_Drug_Repurposing.ipynb).

## Case Study II
We extended our knowledge discovery pipeline for enhancing data analysis for patient cohort context exploration. Specifically, we utilized the data from ALL-of-US, a nationwide research program in the United States, to build a cohort. Given characteristics of a cohort (INPUT), the module will return cohort context in iBKH, i.e., cohort context entities (CCE) such as genes, pathways, drugs, diseases, symptoms, etc. which are potentially associated with the query cohort. Detailed information and codes can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Cohort%20Context%20Exploration.ipynb).

### Step I: Generating cohort profiles in All-of-Us database
[The All of Us Research Program](https://www.researchallofus.org/) is a biomedical data platform and all data needs to be analyzed on the platform's secure cloud environment. Please find the tutorial and corresponding codes [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/All-of-Us/AllofUs_tutorial.ipynb) for building study cohort and generating cohort profiles in All-of-Us.

### Step II: Mapping clincal profiles to iBKH
Given the clinical profile for a specific patient cohort (called a query cohort), we then mapped them to the corresponding biomedical entities in iBKH (called cohort description entities). </br>

Code can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Cohort%20Context%20Exploration.ipynb).

<b>Note</b>: The mapping of entities in the code uses the UMLS API, so you will need to complete the UMLS registration and obtain the API key associated with your account to complete access to the UMLS API.

### Step III: Cohort Exploration
We then predicted the context entities of the query cohort, given the description entities and their weights in the query cohort. Code can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Cohort%20Context%20Exploration.ipynb).

### Step IV: Generate Context Network for Result Interpretation
To visualize the predicted context entities of the query cohort, we pull shortest paths between each pair of cohort description entity and context entity. Code can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Cohort%20Context%20Exploration.ipynb).

</br><b>Note</b>: Neo4j-Python Setup. To generate the context network, we need to conduct path queries in iBKH. Please refer to the [instruction](https://docs.google.com/document/d/1cLDPLp_nVCJ5xrDlJ-B-Q3wf24tb-Dyq55nAXxaNgTM/edit) for deploying iBKH with Neo4j. 

Please replace the 'uri' variable in the generate_network_triplets() function in the exploration_CC.py file with your own URL for Neo4j.-->


