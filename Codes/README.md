# iBKH Case Study
## Overview
TBD

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

## DGL-KE Platform Setup
In this work, we used the Deep Graph Library - Knowledge Embedding (DGL-KE) (https://github.com/awslabs/dgl-ke), a Python-based implementation for the advanced KGE algorithms, such as TransE, TransR, ComplEx, and DistMult. You can follow the [Installation Guide](https://dglke.dgl.ai/doc/install.html) to complete the DGL-KE installation.

## Case Study I
This is the implementation of Alzheimer's Disease (AD) drug repurposing based on iBKH. The task is to dicover drugs that potentially link to AD in the iBKH. All the detailed information and codes can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Case_Study-AD_Drug_Repurposing.ipynb).

## Case Study II
We extended our knowledge discovery pipeline for enhancing data analysis for patient cohort context exploration. Specifically, we utilized the data from ALL-of-US, a nationwide research program in the United States, to build a cohort. Given characteristics of a cohort (INPUT), the module will return cohort context in iBKH, i.e., cohort context entities (CCE) such as genes, pathways, drugs, diseases, symptoms, etc. which are potentially associated with the query cohort. All the detailed information and codes can be found [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/Cohort%20Context%20Exploration.ipynb).
### Step I: All-of-Us
[The All of Us Research Program](https://www.researchallofus.org/) is a biomedical data platform and all data needs to be analyzed on the platform's secure cloud environment. You can find the tutorial and corresponding codes [here](https://github.com/wcm-wanglab/iBKH/blob/main/Codes/All-of-Us/AllofUs_tutorial.ipynb).
### Step II: xxx
Note1: Neo4j-Python Setup...
Note2: We used UMLS to,,,



## Running Guide
Please refer to each of the Jupiter Notebook files for detailed code instructions. The runtime of all code will depend on the performance of the hardware device. 
## Neo4j Setup
Since some of the data needs to be obtained by communicating with Neo4j, please refer to the deployment instructions for Neo4j on the homepage to deploy iBKH data to Neo4j. And replace the 'uri' variable in the generate_network_triplets() function in the exploration_CC.py file with your own URL for Neo4j.
## UMLS API Setup
The mapping of entities in the code uses the UMLS API, so you will need to complete the UMLS registration and obtain the API key associated with your account to complete access to the UMLS API.
