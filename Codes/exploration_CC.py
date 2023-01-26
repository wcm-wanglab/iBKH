
import pickle

import neo4j
import pandas as pd
from tqdm import tqdm

from link_prediction_multi import *
from neo4j import GraphDatabase
import itertools
import os


def get_cohort_context(cohort_name, topk):
    with open('concept_iBKH.obj', 'rb') as f:
        concept_iBKH = pickle.load(f)
    f.close()
    g_vob = pd.read_csv('Data/iBKH/Entity/gene_vocab.csv')
    ibkh_symbol_list = g_vob['symbol'].tolist()

    cohort_disease = pd.read_csv(cohort_name + '_disease.csv')
    cohort_disease = cohort_disease.rename(columns={"Unnamed: 0": "disease"})
    cohort_gene = pd.read_csv(cohort_name + '_gene.csv')
    cohort_gene = cohort_gene.rename(columns={"Unnamed: 0": "gene"})
    input_entity_list = []
    input_count = 0
    for i in range(len(cohort_disease)):
        disease = cohort_disease.loc[i, 'disease']
        weight = cohort_disease.loc[i, 'proportion']
        if disease in concept_iBKH:
            ibkh_name = concept_iBKH[disease][1]
            ibkh_type = concept_iBKH[disease][2]
            if check_entity_in_entityMap(ibkh_name, ibkh_type):
                input_count += 1
                input_entity_list.append([ibkh_name, ibkh_type, weight])

    input_genes = {}
    input_gene_weights = []
    for i in range(len(cohort_gene)):
        gene = cohort_gene.loc[i, 'gene']
        weight = cohort_gene.loc[i, 'weight']
        if (gene in ibkh_symbol_list) and (weight >= 1) and (check_entity_in_entityMap(gene, 'Gene')):
            input_genes[gene] = weight
            input_gene_weights.append(weight)
    gene_weight_max = max(input_gene_weights)
    gene_weight_min = min(input_gene_weights)
    for gene in input_genes:
        input_count += 1
        weight_norm = (input_genes[gene] - gene_weight_min) / (gene_weight_max - gene_weight_min)
        input_entity_list.append([gene, 'Gene', weight_norm])

    with open("input_dx_gene_list_" + cohort_name + ".obj", "wb") as f:
        pickle.dump(input_entity_list, f)
    f.close()
    print('Input list completed...', 'Total: ', input_count)
    print(input_entity_list)

    input_emb_ids = map_input2embedding_id(input_entity_list)
    TransE_res = get_averaged_rank(input_emb_ids, 'TransE')
    TransR_res = get_averaged_rank(input_emb_ids, 'TransR')
    ComplEx_res = get_averaged_rank(input_emb_ids, 'ComplEx')
    DistMult_res = get_averaged_rank(input_emb_ids, 'DistMult')

    output_category = ['Gene', 'Pathway', 'Drug', 'Disease', 'Symptom', 'Side_Effect']
    for oc in output_category:
        TransE_oc_res = TransE_res[oc]
        TransR_oc_res = TransR_res[oc]
        ComplEx_oc_res = ComplEx_res[oc]
        DistMult_oc_res = DistMult_res[oc]
        if bool(TransE_oc_res):
            ensemble_oc_res = vote_result(TransE_oc_res, TransR_oc_res, ComplEx_oc_res, DistMult_oc_res)
        else:
            ensemble_oc_res = {}

        res_table = generate_predict_result(ensemble_oc_res, oc, topk)
        res_path = os.path.dirname(cohort_name + '/')
        if not os.path.exists(res_path):
            os.makedirs(res_path)
        if len(res_table) > 0:
            res_table.to_csv(cohort_name + '/dx_gene/top' + str(topk) + '/' + oc + '.csv', index=False)


def check_entity_in_entityMap(entity_name, entity_type):
    if entity_type == 'Disease':
        entity_id = disease_name_dict[entity_name]
    elif entity_type == 'Drug':
        entity_id = drug_name_dict[entity_name]
    elif entity_type == 'Gene':
        entity_id = gene_symbol_dict[entity_name]
    elif entity_type == 'Pathway':
        entity_id = pathway_name_dict[entity_name]
    elif entity_type == 'Side_Effect':
        entity_id = se_name_dict[entity_name]
    elif entity_type == 'Symptom':
        entity_id = symptom_name_dict[entity_name]

    entity_id = entity_id if 'MeSH' not in entity_id else entity_id.replace('MeSH', 'MESH')

    return entity_id in entity_map


def generate_predict_result(candidate_res, candidate_type, top_k):
    if top_k is None:
        candidate_ids = list(candidate_res.keys())
    else:
        candidate_ids = list(candidate_res.keys())[:int(top_k)]
    res = pd.DataFrame(columns=['primary', 'name', 'predict_score_norm', 'predict_score_raw', 'type'])
    idx = 0
    for candidate_id in candidate_ids:
        candidate_name = ''
        candidate_score = candidate_res[candidate_id][1]
        candidate_score = str(round(candidate_score, 4))
        candidate_raw_score = candidate_res[candidate_id][2]
        candidate_raw_score = str(round(candidate_raw_score, 4))
        if candidate_type == 'Drug':
            candidate_name = drug_dict[candidate_id]
        if candidate_type == 'Disease':
            candidate_name = disease_dict[candidate_id]
        if candidate_type == 'Gene':
            candidate_name = gene_dict[candidate_id]
        if candidate_type == 'Symptom':
            candidate_name = symptom_dict[candidate_id]
        if candidate_type == 'Pathway':
            candidate_name = pathway_dict[candidate_id]
        if candidate_type == 'Side_Effect':
            candidate_name = se_dict[candidate_id]

        res.loc[idx] = [candidate_id, candidate_name, candidate_score, candidate_raw_score, candidate_type]
        idx += 1

    return res


def network_data(cohort_name, target_type, topk):
    with open("input_dx_gene_list_" + cohort_name + ".obj", "rb") as f:
        input_entity_list = pickle.load(f)
    f.close()

    predict_res = pd.read_csv(cohort_name + '/dx_gene/top' + str(topk) + '/' + target_type + '.csv')
    candidate_list = predict_res.set_index('name')['type'].to_dict()

    triplets_list = []
    print("Add AD-related triplets...")
    for candidate in tqdm(candidate_list):
        if candidate != "alzheimer's disease":
            cypher_statement = "MATCH (pre:Disease {name: \"alzheimer's disease\"}), "
            if candidate_list[candidate] == 'Gene':
                cypher_statement += "(can:" + target_type + " {symbol: \"" + candidate + "\"}), "
            else:
                cypher_statement += "(can:" + target_type + " {name: \"" + candidate + "\"}), "
            cypher_statement += "path = allShortestPaths((pre)-[*..15]-(can)) RETURN path LIMIT 5"
            triplets_list += generate_network_triplets(cypher_statement)

    concepts_list = []
    print("Add concept-related triplets...")
    for input_entity in tqdm(input_entity_list):
        ibkh_name, ibkh_type, weight = input_entity
        concepts_list.append(ibkh_name)
        for candidate in tqdm(candidate_list):
            if ibkh_name != candidate:
                # if candidate == "SMIM34A" or candidate == "MIRLET7F1":
                #     continue
                if ibkh_type == 'Gene':
                    cypher_statement = "MATCH (pre:" + ibkh_type + " {symbol: \"" + ibkh_name + "\"}), "
                else:
                    cypher_statement = "MATCH (pre:" + ibkh_type + " {name: \"" + ibkh_name + "\"}), "
                if candidate_list[candidate] == 'Gene':
                    cypher_statement += "(can:" + target_type + " {symbol: \"" + candidate + "\"}), "
                else:
                    cypher_statement += "(can:" + target_type + " {name: \"" + candidate + "\"}), "
                cypher_statement += "path = allShortestPaths((pre)-[*..15]-(can)) RETURN path LIMIT 5"
                triplets_list += generate_network_triplets(cypher_statement)

    network_data = generate_network_data(triplets_list, concepts_list, candidate_list)
    with open(cohort_name + '/dx_gene//top' + str(topk) + '/' + target_type + '_network.obj', 'wb') as f:
        pickle.dump(network_data, f)
    f.close()


def generate_network_triplets(cypher_statement):
    uri = "bolt://***:7687" // Neo4j URL
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))
    session = driver.session()
    triplets_list = []
    try:
        neo4j_res = session.run(cypher_statement)
        for record in neo4j_res:
            nodes = record["path"].nodes
            temp_nodes = []
            for node in nodes:
                group, = node.labels
                node_name = node['symbol'] if group == 'Gene' else node['name']
                temp_nodes.append([node_name, group])
            triplets_list.append(temp_nodes)
    except:
        print(cypher_statement)

    return triplets_list


def generate_network_data(triplets_list, input_list, predict_res, primary):
    node_data = []
    rel_data = []
    node_name_id_dict = {}
    node_id = 0
    triplets_list.sort()
    triplets_list = list(nodes_list for nodes_list, _ in itertools.groupby(triplets_list))
    if primary == "AD":
        for triplets in triplets_list:
            for i in range(len(triplets) - 1):
                node_name, group = triplets[i]
                node_next_name, group_next = triplets[i + 1]
                if node_name not in node_name_id_dict:
                    if node_name == "alzheimer's disease":
                        node_type = "primary"
                    elif (node_name in input_list) and (node_name in predict_res):
                        node_type = "secondary / candidate"
                    elif (node_name in input_list) and (node_name not in predict_res):
                        node_type = "secondary"
                    elif (node_name in predict_res) and (node_name not in input_list):
                        node_type = "candidate"
                    else:
                        node_type = "path_node"
                    node_data.append({"id": node_id, "name": node_name, "node_type": node_type, "group": group})
                    node_name_id_dict[node_name] = node_id
                    node_id += 1
                if node_next_name not in node_name_id_dict:
                    if node_next_name == "alzheimer's disease":
                        node_type_next = "primary"
                    elif (node_next_name in input_list) and (node_next_name in predict_res):
                        node_type_next = "secondary / candidate"
                    elif (node_next_name in input_list) and (node_next_name not in predict_res):
                        node_type_next = "secondary"
                    elif (node_next_name in predict_res) and (node_next_name not in input_list):
                        node_type_next = "candidate"
                    else:
                        node_type_next = "path_node"
                    node_data.append(
                        {"id": node_id, "name": node_next_name, "node_type": node_type_next, "group": group_next})
                    node_name_id_dict[node_next_name] = node_id
                    node_id += 1
                rel_data.append({"source": node_name_id_dict[node_name], "target": node_name_id_dict[node_next_name]})
    elif primary == "APOE":
        for triplets in triplets_list:
            for i in range(len(triplets) - 1):
                node_name, group = triplets[i]
                node_next_name, group_next = triplets[i + 1]
                if node_name not in node_name_id_dict:
                    if node_name == "APOE":
                        node_type = "primary"
                    elif (node_name in input_list) and (node_name in predict_res):
                        node_type = "secondary / candidate"
                    elif (node_name in input_list) and (node_name not in predict_res):
                        node_type = "secondary"
                    elif (node_name in predict_res) and (node_name not in input_list):
                        node_type = "candidate"
                    else:
                        node_type = "path_node"
                    node_data.append({"id": node_id, "name": node_name, "node_type": node_type, "group": group})
                    node_name_id_dict[node_name] = node_id
                    node_id += 1
                if node_next_name not in node_name_id_dict:
                    if node_next_name == "APOE":
                        node_type_next = "primary"
                    elif (node_next_name in input_list) and (node_next_name in predict_res):
                        node_type_next = "secondary / candidate"
                    elif (node_next_name in input_list) and (node_next_name not in predict_res):
                        node_type_next = "secondary"
                    elif (node_next_name in predict_res) and (node_next_name not in input_list):
                        node_type_next = "candidate"
                    else:
                        node_type_next = "path_node"
                    node_data.append(
                        {"id": node_id, "name": node_next_name, "node_type": node_type_next, "group": group_next})
                    node_name_id_dict[node_next_name] = node_id
                    node_id += 1
                rel_data.append({"source": node_name_id_dict[node_name], "target": node_name_id_dict[node_next_name]})
    network_data = {"nodes": node_data, "links": rel_data}

    return network_data


def main():
    get_cohort_context("cohort_female", 10)
    cohort_name = 'cohort_male'
    for topk in [10, 100, 200]:
        print(topk)
        get_cohort_context(cohort_name, topk)

    for topk in [10, 100]:
        print(topk)
        for target_type in ['Disease', 'Drug', 'Pathway', 'Symptom', 'Gene']:
            network_data(cohort_name, target_type, topk)
            print(target_type + ' Completed...')

    network_data('cohort_female', 'Symptom', 100)
    network_data('cohort_female', 'Gene', 100)


if __name__ == '__main__':
    main()
