#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 17:48:29 2023

@author: changsu
"""


import networkx as nx
from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import os





def subgraph_visualization(target_type, target_list, predicted_type, predicted_list, 
                           excluded_r_type = [],                                  
                           neo4j_url = "neo4j://54.210.251.104:7687", 
                           username = "neo4j", password = "password",
                           alpha = 1, k=0.3, nsize=200, target_size_ratio=2.5,
                           with_node_label=True, node_label_size = 10,
                           with_edge_label=True, edge_label_size = 7,
                           figsize=(14, 10),
                           save=True, save_path='output', save_name=None):
    
    # Connect to the Neo4j database
    driver = GraphDatabase.driver(neo4j_url, 
                                  auth=(username, password), 
                                  encrypted=False)
    neo4j_res_list = []
    
    # Build Cypher statement
    for target in target_list:
        for predict in predicted_list:
            cypher = "MATCH (e1:" + target_type + " {Name: \"" + target + "\"})"
            cypher += ", (e2:" + predicted_type + " {Name: \"" + predict + "\"})"
            cypher += ", p = allShortestPaths((e1)-[*..5]-(e2)) RETURN p LIMIT 30"
    
            # Run the Neo4j query and retrieve the results
            session = driver.session()
            neo4j_res = session.run(cypher)
            neo4j_res_list.append(neo4j_res)
    
    # Create a NetworX Graph object
    g = nx.MultiGraph()

    # Define node groups and their corresponding colors
    group_colors = {
        "Disease": "#E0C3FC",
        "Drug": "#83B5D1",
        "Gene": "#F28482",
        "Symptom": "#7B967A",
        "Side-effect": "#9DA1DD",
        "Pathway": "#94D2BD"
    }
    
    
    node_id_map = {}
    id_node_map = {} 
    node_color = {}
    
    edge_label_map = {}
    # Iterate over the Neo4j query result and add nodes to the network
    idx = 0
    for neo4j_res in neo4j_res_list:
        for record in neo4j_res:
            path = record["p"]
            
            # adding node
            for node in path.nodes:
                node_type_list = list(node.labels)
                node_type = node_type_list[0] if node_type_list[0] != 'Node' else node_type_list[1]
                node_name = node['Name']
                
                if node_name not in node_id_map:
                    node_id_map[node_name] = idx
                    id_node_map[idx] = node_name
                    
                    g.add_node(node_name)                    
                    node_color[node_name] = group_colors[node_type]
                                        
                    idx += 1
            
            # adding edges
            for relation in path.relationships:
                start_node_type = list(relation.start_node.labels)[0]
                end_node_type = list(relation.end_node.labels)[0]
            
                r_type = relation.type
                
                if r_type in excluded_r_type:
                    continue
                
                start = relation.start_node['Name']
                end = relation.end_node["Name"]

                edge_label_map[(start, end)] = r_type 
                
                g.add_edge(start, end, label=r_type)
    
    
    color_map = []
    size_map = []
    for n in g.nodes():
        color_map.append(node_color[n])
        if (n in target_list) or (n in predicted_list):
            size_map.append(nsize * target_size_ratio)
        else:
            size_map.append(nsize)
            
        
    plt.figure(figsize=figsize)
    
    positions = nx.spring_layout(g, k=k)

    
    for node in target_list:
        positions[node][0] -= alpha  # adjust value as needed
    
    for node in predicted_list:
        positions[node][0] += alpha  # adjust value as needed
    
    nx.draw(g, with_labels = with_node_label, 
            node_color=color_map, node_size=size_map,
            edge_color='#E0E0E0', pos=positions,
            font_size=node_label_size)
            
    nx.draw_networkx_edge_labels(
            g, positions,
            edge_labels=edge_label_map,
            font_color='blue',
            font_size = edge_label_size
            )
      
    if save == True:
        
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        
        if save_name == None:
            save_name ='shortest_path_interpretation_%s_%s.pdf' % (target_type, predicted_type)
        plt.savefig(save_path + '/' + save_name)
        
    
    plt.show()
 
    return g
  

#excluded_r_type = [
#                   'Inferred_Relation_DDi', 'Semantic_Relation_DDi'
#                   'Semantic_Relation_DG', '19	Semantic_Relation_DiG',
#                   'Semantic_Relation_GG', 'Inferred_Relation_DiG']

