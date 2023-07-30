#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 10:06:14 2023

@author: changsu
"""

import torch as th
import torch.nn.functional as fn

import numpy as np
import pandas as pd

from sklearn.preprocessing import MinMaxScaler

import os


"""
The following codes define functions 
"""
# link prediction based on embeddings derived using TransE
def transE_l2(head, rel, tail, gamma=12.0):
    # Paper link: https://papers.nips.cc/paper/5071-translating-embeddings-for-modeling-multi-relational-data
    score = head + rel - tail
    
    return gamma - th.norm(score, p=2, dim=-1)


# link prediction based on embeddings derived using TransR
def transR(head, rel, tail, proj, rel_idx, gamma=12.0):
    # Paper link: https://www.aaai.org/ocs/index.php/AAAI/AAAI15/paper/download/9571/9523
    proj = proj.reshape(-1, head.shape[1], rel.shape[0])[rel_idx]
    head_r = th.einsum('ab,bc->ac', head, proj)
    tail_r = th.einsum('b,bc->c', th.tensor(tail), proj)
    score = head_r + rel - tail_r
    
    return gamma - th.norm(score, p=1, dim=-1)


# link prediction based on embeddings derived using DistMult
def DistMult(head, rel, tail):
    # Paper link: https://arxiv.org/abs/1412.6575
    score = head * rel * tail
    
    return th.sum(score, dim=-1)



# link prediction based on embeddings derived using complEx
def complEx(head, rel, tail, gamma=12.0):
    # Paper link: https://arxiv.org/abs/1606.06357
    real_head, img_head = th.chunk(head, 2, dim=-1)
    real_tail, img_tail = th.chunk(th.tensor(tail), 2, dim=-1)
    real_rel, img_rel = th.chunk(rel, 2, dim=-1)

    score = real_head * real_tail * real_rel \
            + img_head * img_tail * real_rel \
            + real_head * img_tail * img_rel \
            - img_head * real_tail * img_rel

    return th.sum(score, -1)


def generate_hypothesis(target_entity, candidate_entity_type, relation_type,
                        embedding_folder='data/embeddings', method='transE_l2', 
                        kg_folder = 'data/iBKH', triplet_folder = 'data/triplets',
                        without_any_rel=False, topK=100,
                        save_path='output', save=True):
    
    # load entity vocab 
    entities = {}
    for e in ['anatomy', 'disease', 'drug', 'dsp', 'gene',
              'molecule', 'pathway', 'sdsi', 'side_effect', 
              'symptom', 'tc']:
        e_df = pd.read_csv(kg_folder + '/entity/' + e + '_vocab.csv', header=0, low_memory=False)
        if e == 'gene':
            e_df = e_df.rename(columns={'symbol':'name'})
        if e == 'molecule':
            e_df = e_df.rename(columns={'chembl_id':'name'})
            
        entities[e] = e_df
        
    # get target entity vocab    
    target_entity_vocab = pd.DataFrame()
    for e in entities:
        e_df = entities[e][['primary', 'name']]
        target_entity_vocab = pd.concat([target_entity_vocab, e_df[e_df['name'].isin(target_entity)]])
        
    
    # load embeddings
    entity_emb = np.load(embedding_folder + '/' + method + '/iBKH_' + method + '_entity.npy')
    rel_emb = np.load(embedding_folder + '/' + method + '/iBKH_' + method + '_relation.npy')
    if method == 'transR':
        proj_np = np.load(embedding_folder + '/' + method + '/iBKH_TransRprojection.npy')
        proj_emb = th.tensor(proj_np)
    
    # load entity and relation embedding map
    entity_emb_map = pd.read_csv(embedding_folder + '/' + method + '/entities.tsv', sep='\t', header=None, low_memory=False)
    entity_emb_map.columns = ['id', 'primary']
    rel_emb_map = pd.read_csv(embedding_folder + '/' + method + '/relations.tsv', sep='\t', header=None, low_memory=False)
    rel_emb_map.columns = ['rid', 'relation']

    target_entity_vocab = pd.merge(target_entity_vocab, entity_emb_map, on='primary', how='left')
    
  
    target_entity_ids = []
    target_entity_names = []
    target_entity_primaries = []
    for idx, row in target_entity_vocab.iterrows():
        target_entity_ids.append(row['id'])
        target_entity_names.append(row['name'])
        target_entity_primaries.append(row['primary'])
        
    
    # get candidate entity embeddings
    candidate_entities = pd.merge(entities[candidate_entity_type], entity_emb_map, on='primary', how='inner')
    candidate_entity_ids = th.tensor(candidate_entities.id.tolist()).long()
    candidate_embs = th.tensor(entity_emb[candidate_entity_ids])

    
    # get target relation embeddings
    target_relations = rel_emb_map[rel_emb_map['relation'].isin(relation_type)]
    target_relation_ids = th.tensor(target_relations.rid.tolist()).long()
    target_relation_embs = [th.tensor(rel_emb[rid]) for rid in target_relation_ids]
    

    
    
    # rank candidate entities
    scores_per_target_ent = []
    candidate_ids = []
    for rid in range(len(target_relation_embs)):
        rel_emb=target_relation_embs[rid]
        for target_id in target_entity_ids:
            target_emb = entity_emb[target_id]
            
            if method == 'transE_l2':            
                score = fn.logsigmoid(transE_l2(candidate_embs, rel_emb, target_emb))
            elif method == 'transR':
                score = fn.logsigmoid(transR(candidate_embs, rel_emb, target_emb, proj_emb, target_relation_ids[rid]))
            elif method == 'complEx':
                score = fn.logsigmoid(complEx(candidate_embs, rel_emb, target_emb))
            elif method == 'DistMult':
                score = fn.logsigmoid(DistMult(candidate_embs, rel_emb, target_emb))
            else:
                print("Method name error!!! Please check name of the knowledge graph embedding method you used.")
                        
            scores_per_target_ent.append(score)
            candidate_ids.append(candidate_entity_ids)
    scores = th.cat(scores_per_target_ent)
    candidate_ids = th.cat(candidate_ids)
    
    idx = th.flip(th.argsort(scores), dims=[0])
    scores = scores[idx].numpy()
    candidate_ids = candidate_ids[idx].numpy()
    
    
    # de-duplication
    _, unique_indices = np.unique(candidate_ids, return_index=True)
    # sorting
    ranked_unique_indices = np.sort(unique_indices)
    proposed_candidate_ids = candidate_ids[ranked_unique_indices]
    proposed_scores = scores[ranked_unique_indices]
    proposed_scores_norm = MinMaxScaler().fit_transform(proposed_scores.reshape(-1, 1))
    
    
    proposed_df = pd.DataFrame()
    proposed_df['id'] = proposed_candidate_ids
    proposed_df['score'] = proposed_scores
    proposed_df['score_norm'] = proposed_scores_norm
    
#    proposed_df = pd.merge(proposed_df, candidate_entities, on='id', how='left')   
    proposed_df = pd.merge(candidate_entities, proposed_df, on='id', how='right')
    
    
    ### remove candidate entities who have already linked to the target entity
    rel_meta_type = relation_type[0].split('_')[-1]  # e.g., Treats_DDi => DDi
    # load triplet file
    triplet_df = pd.read_csv(triplet_folder + '/' + rel_meta_type + '_triplet.csv', header=0, low_memory=False)
    if without_any_rel == False:
        triplet_df = triplet_df[triplet_df['Relation'].isin(relation_type)]
    # only keep triplets that contain target entity
    triplet_df =  triplet_df[(triplet_df['Head'].isin(target_entity_primaries)) | (triplet_df['Tail'].isin(target_entity_primaries))]
    # candidate entities with known relation with target entity 
    candidates_known = triplet_df['Head'].tolist() + triplet_df['Tail'].tolist()
    candidates_known = list(set(candidates_known) - set(target_entity_primaries))

    # in results, filter out andidate entities with known relation with target entity
    proposed_df = proposed_df[~proposed_df['primary'].isin(candidates_known)]
    proposed_df = proposed_df[~proposed_df['name'].isin(target_entity_names)]
    
    proposed_df = proposed_df.reset_index(drop=True)
    
    if topK != None:
        proposed_df = proposed_df[: topK]
        
    if save == True:
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        proposed_df.to_csv(save_path + '/prediction_%s_top%s_%s.csv' % (candidate_entity_type, topK, method), index=False)
        
    return proposed_df
    
    

def generate_hypothesis_ensemble_model(target_entity, candidate_entity_type, relation_type,
                        embedding_folder='data/embeddings',
                        kg_folder = 'data/iBKH', triplet_folder = 'data/triplets',
                        without_any_rel=False, topK=100,
                        save_path='output', save=True):

    transE_res = generate_hypothesis(target_entity=target_entity, candidate_entity_type=candidate_entity_type, 
                                     relation_type=relation_type, embedding_folder=embedding_folder, method='transE_l2', 
                                     kg_folder = kg_folder, triplet_folder = triplet_folder,
                                     without_any_rel=without_any_rel, topK=None, save=False)
    transE_res['transE_vote'] = len(transE_res) - transE_res.index.values
    
    transR_res = generate_hypothesis(target_entity=target_entity, candidate_entity_type=candidate_entity_type, 
                                     relation_type=relation_type, embedding_folder=embedding_folder, method='transR', 
                                     kg_folder = kg_folder, triplet_folder = triplet_folder,
                                     without_any_rel=without_any_rel, topK=None, save=False)
    transR_res['transR_vote'] = len(transR_res) - transR_res.index.values
    
    complEx_res = generate_hypothesis(target_entity=target_entity, candidate_entity_type=candidate_entity_type, 
                                     relation_type=relation_type, embedding_folder=embedding_folder, method='complEx', 
                                     kg_folder = kg_folder, triplet_folder = triplet_folder,
                                     without_any_rel=without_any_rel, topK=None, save=False)
    complEx_res['complEx_vote'] = len(complEx_res) - complEx_res.index.values
    
    DistMult_res = generate_hypothesis(target_entity=target_entity, candidate_entity_type=candidate_entity_type, 
                                     relation_type=relation_type, embedding_folder=embedding_folder, method='DistMult', 
                                     kg_folder = kg_folder, triplet_folder = triplet_folder,
                                     without_any_rel=without_any_rel, topK=None, save=False)    
    DistMult_res['DistMult_vote'] = len(DistMult_res) - DistMult_res.index.values
    
    
    combined_res = pd.merge(transE_res, transR_res[['primary', 'transR_vote']], on='primary', how='left')
    combined_res = pd.merge(combined_res, complEx_res[['primary', 'complEx_vote']], on='primary', how='left')
    combined_res = pd.merge(combined_res, DistMult_res[['primary', 'DistMult_vote']], on='primary', how='left')
    
    combined_res['vote_score'] = combined_res['transE_vote'] + combined_res['transR_vote'] + combined_res['complEx_vote'] + combined_res['DistMult_vote']
    combined_res['vote_score_normed'] =  MinMaxScaler().fit_transform(combined_res['vote_score'].values.reshape(-1, 1))
    
    combined_res = combined_res.sort_values(by='vote_score_normed', ascending=False)
    
    combined_res = combined_res.reset_index(drop=True)
    
    if topK != None:
        combined_res = combined_res[: topK]
        
    if save == True:
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        combined_res.to_csv(save_path + '/prediction_%s_top%s_ensemble.csv' % (candidate_entity_type, topK), index=False)
        
    return combined_res
    