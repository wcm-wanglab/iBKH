#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: changsu

This script defines functions for iBKH-KG preprocessing.
"""

import numpy as np
import pandas as pd
import os
import pickle

# Extracting triplets between drug and disease entities
def DDi_triplets(kg_folder, triplet_path): 
    ddi = pd.read_csv(kg_folder + 'Relation/D_Di_res.csv')

    ddi_treats = ddi[ddi['Treats'] == 1]
    ddi_treats['Relation'] = ['Treats_DDi'] * len(ddi_treats)
    ddi_treats = ddi_treats[['Drug', 'Relation', 'Disease', 'Inference_Score']]

    ddi_palliates = ddi[ddi['Palliates'] == 1]
    ddi_palliates['Relation'] = ['Palliates_DDi'] * len(ddi_palliates)
    ddi_palliates = ddi_palliates[['Drug', 'Relation', 'Disease', 'Inference_Score']]

    ddi_effect = ddi[ddi['Effect'] == 1]
    ddi_effect['Relation'] = ['Effect_DDi'] * len(ddi_effect)
    ddi_effect = ddi_effect[['Drug', 'Relation', 'Disease', 'Inference_Score']]

    ddi_associate = ddi[ddi['Associate'] == 1]
    ddi_associate['Relation'] = ['Associate_DDi'] * len(ddi_associate)
    ddi_associate = ddi_associate[['Drug', 'Relation', 'Disease', 'Inference_Score']]

    ddi_IR = ddi[ddi['Inferred_Relation'] == 1]
    ddi_IR['Relation'] = ['Inferred_Relation_DDi'] * len(ddi_IR)
    ddi_IR = ddi_IR[['Drug', 'Relation', 'Disease', 'Inference_Score']]

    ddi_SR = ddi[
        (ddi['treatment/therapy (including investigatory)'] == 1) | (ddi['inhibits cell growth (esp. cancers)'] == 1) |
        (ddi['alleviates, reduces'] == 1) | (ddi['biomarkers (of disease progression)'] == 1) |
        (ddi['prevents, suppresses'] == 1) | (ddi['role in disease pathogenesis'] == 1)]
    ddi_SR['Relation'] = ['Semantic_Relation_DDi'] * len(ddi_SR)
    ddi_SR = ddi_SR[['Drug', 'Relation', 'Disease', 'Inference_Score']]

    ddi_res = pd.concat((ddi_treats, ddi_palliates, ddi_effect, ddi_associate, ddi_IR, ddi_SR))
    ddi_res = ddi_res.rename(columns={'Drug': 'Head', 'Disease': 'Tail'})

    ddi_res.loc[ddi_res['Relation'] != 'Inferred_Relation_DDi', 'Inference_Score'] = np.nan

    ddi_res.to_csv(triplet_path + 'DDi_triplet.csv', index=False)
    
    print('DDi triplets generated.')



    
# Extracting triplets between drug and gene entities.

def DG_triplets(kg_folder, triplet_path):
    dg = pd.read_csv(kg_folder + 'Relation/D_G_res.csv')

    dg_target = dg[dg['Target'] == 1]
    dg_target['Relation'] = ['Target_DG'] * len(dg_target)
    dg_target['Inference_Score'] = [''] * len(dg_target)
    dg_target = dg_target[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_transporter = dg[dg['Transporter'] == 1]
    dg_transporter['Relation'] = ['Transporter_DG'] * len(dg_transporter)
    dg_transporter['Inference_Score'] = [''] * len(dg_transporter)
    dg_transporter = dg_transporter[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_enzyme = dg[dg['Enzyme'] == 1]
    dg_enzyme['Relation'] = ['Enzyme_DG'] * len(dg_enzyme)
    dg_enzyme['Inference_Score'] = [''] * len(dg_enzyme)
    dg_enzyme = dg_enzyme[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_carrier = dg[dg['Carrier'] == 1]
    dg_carrier['Relation'] = ['Carrier_DG'] * len(dg_carrier)
    dg_carrier['Inference_Score'] = [''] * len(dg_carrier)
    dg_carrier = dg_carrier[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_downregulates = dg[dg['Downregulates'] == 1]
    dg_downregulates['Relation'] = ['Downregulates_DG'] * len(dg_downregulates)
    dg_downregulates['Inference_Score'] = [''] * len(dg_downregulates)
    dg_downregulates = dg_downregulates[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_upregulates = dg[dg['Upregulates'] == 1]
    dg_upregulates['Relation'] = ['Upregulates_DG'] * len(dg_upregulates)
    dg_upregulates['Inference_Score'] = [''] * len(dg_upregulates)
    dg_upregulates = dg_downregulates[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_associate = dg[dg['Associate'] == 1]
    dg_associate['Relation'] = ['Associate_DG'] * len(dg_associate)
    dg_associate['Inference_Score'] = [''] * len(dg_associate)
    dg_associate = dg_associate[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_binds = dg[dg['Binds'] == 1]
    dg_binds['Relation'] = ['Binds_DG'] * len(dg_binds)
    dg_binds['Inference_Score'] = [''] * len(dg_binds)
    dg_binds = dg_binds[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_interaction = dg[dg['Interaction'] == 1]
    dg_interaction['Relation'] = ['Interaction_DG'] * len(dg_interaction)
    dg_interaction['Inference_Score'] = [''] * len(dg_interaction)
    dg_interaction = dg_interaction[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_SR = dg[(dg['affects expression/production (neutral)'] == 1) | (dg['agonism, activation'] == 1) |
               (dg['inhibits'] == 1) | (dg['metabolism, pharmacokinetics'] == 1) | (dg['antagonism, blocking'] == 1) |
               (dg['increases expression/production'] == 1) | (dg['binding, ligand (esp. receptors)'] == 1) |
               (dg['decreases expression/production'] == 1) | (dg['transport, channels'] == 1) |
               (dg['enzyme activity'] == 1) | (dg['physical association'] == 1)]
    dg_SR['Relation'] = ['Semantic_Relation_DG'] * len(dg_SR)
    dg_SR['Inference_Score'] = [''] * len(dg_SR)
    dg_SR = dg_SR[['Drug', 'Relation', 'Gene', 'Inference_Score']]

    dg_res = pd.concat(
        (dg_target, dg_transporter, dg_enzyme, dg_carrier, dg_downregulates, dg_upregulates, dg_associate,
         dg_binds, dg_interaction, dg_SR))
    dg_res = dg_res.rename(columns={'Drug': 'Head', 'Gene': 'Tail'})

    dg_res.to_csv(triplet_path + 'DG_triplet.csv', index=False)  
    
    print('DG triplets generated.')
    
    
# Extracting triplets between drug entities.
def DD_triplets(kg_folder, triplet_path):
    dd = pd.read_csv(kg_folder + 'Relation/D_D_res.csv')

    dd_interaction = dd[dd['Interaction'] == 1]
    dd_interaction['Relation'] = ['Interaction_DD'] * len(dd_interaction)
    dd_interaction['Inference_Score'] = [''] * len(dd_interaction)
    dd_interaction = dd_interaction[['Drug_1', 'Relation', 'Drug_2', 'Inference_Score']]

    dd_resemble = dd[dd['Resemble'] == 1]
    dd_resemble['Relation'] = ['Resemble_DD'] * len(dd_resemble)
    dd_resemble['Inference_Score'] = [''] * len(dd_resemble)
    dd_resemble = dd_resemble[['Drug_1', 'Relation', 'Drug_2', 'Inference_Score']]

    dd_res = pd.concat((dd_interaction, dd_resemble))
    dd_res = dd_res.rename(columns={'Drug_1': 'Head', 'Drug_2': 'Tail'})

    dd_res.to_csv(triplet_path + 'DD_triplet.csv', index=False)
    
    print('DD triplets generated.')
    
    
# Extracting triplets between drug and pathway entities.
def DPwy_triplets(kg_folder, triplet_path):
    dpwy = pd.read_csv(kg_folder + 'Relation/D_Pwy_res.csv')

    dpwy['Relation'] = ['Association_DPwy'] * len(dpwy)
    dpwy['Inference_Score'] = [''] * len(dpwy)
    dpwy_res = dpwy[['Drug', 'Relation', 'Pathway', 'Inference_Score']]
    dpwy_res = dpwy_res.rename(columns={'Drug': 'Head', 'Pathway': 'Tail'})

    dpwy_res.to_csv(triplet_path + 'DPwy_triplet.csv', index=False)
    
    print('DPwy triplets generated.')
    
    

# Extracting triplets between drug and side-effect entities.
def DSE_triplets(kg_folder, triplet_path):
    dse = pd.read_csv(kg_folder + 'Relation/D_SE_res.csv')

    dse['Relation'] = ['Cause_DSE'] * len(dse)
    dse['Inference_Score'] = [''] * len(dse)
    dse_res = dse[['Drug', 'Relation', 'Side_Effect', 'Inference_Score']]
    dse_res = dse_res.rename(columns={'Drug': 'Head', 'Side_Effect': 'Tail'})

    dse_res.to_csv(triplet_path + 'DSE_triplet.csv', index=False)
    
    print('DSE triplets generated.')
    
    
# Extracting triplets between disease entities.
def DiDi_triplets(kg_folder, triplet_path):
    didi = pd.read_csv(kg_folder + 'Relation/Di_Di_res.csv')

    didi_is_a = didi[didi['is_a'] == 1]
    didi_is_a['Relation'] = ['is_a_DiDi'] * len(didi_is_a)
    didi_is_a['Inference_Score'] = [''] * len(didi_is_a)
    didi_is_a = didi_is_a[['Disease_1', 'Relation', 'Disease_2', 'Inference_Score']]

    didi_resemble = didi[didi['Resemble'] == 1]
    didi_resemble['Relation'] = ['Resemble_DiDi'] * len(didi_resemble)
    didi_resemble['Inference_Score'] = [''] * len(didi_resemble)
    didi_resemble = didi_resemble[['Disease_1', 'Relation', 'Disease_2', 'Inference_Score']]

    didi_res = pd.concat((didi_is_a, didi_resemble))
    didi_res = didi_res.rename(columns={'Disease_1': 'Head', 'Disease_2': 'Tail'})

    didi_res.to_csv(triplet_path + 'DiDi_triplet.csv', index=False)
    
    print('DiDi triplets generated.')
    
    
    
# Extracting triplets between disease and gene entities.
def DiG_triplets(kg_folder, triplet_path):
    dig = pd.read_csv(kg_folder + 'Relation/Di_G_res.csv')

    dig_associate = dig[dig['Associate'] == 1]
    dig_associate['Relation'] = ['Associate_DiG'] * len(dig_associate)
    dig_associate = dig_associate[['Disease', 'Relation', 'Gene', 'Inference_Score']]

    dig_downregulates = dig[dig['Downregulates'] == 1]
    dig_downregulates['Relation'] = ['Downregulates_DiG'] * len(dig_downregulates)
    dig_downregulates = dig_downregulates[['Disease', 'Relation', 'Gene', 'Inference_Score']]

    dig_upregulates = dig[dig['Upregulates'] == 1]
    dig_upregulates['Relation'] = ['Upregulates_DiG'] * len(dig_upregulates)
    dig_upregulates = dig_upregulates[['Disease', 'Relation', 'Gene', 'Inference_Score']]

    dig_IR = dig[dig['Inferred_Relation'] == 1]
    dig_IR['Relation'] = ['Inferred_Relation_DiG'] * len(dig_IR)
    dig_IR = dig_IR[['Disease', 'Relation', 'Gene', 'Inference_Score']]

    dig_SR = dig[(dig['improper regulation linked to disease'] == 1) | (dig['causal mutations'] == 1) |
                 (dig['polymorphisms alter risk'] == 1) | (dig['role in pathogenesis'] == 1) |
                 (dig['possible therapeutic effect'] == 1) | (dig['biomarkers (diagnostic)'] == 1) |
                 (dig['promotes progression'] == 1) | (dig['drug targets'] == 1) | (
                         dig['overexpression in disease'] == 1) |
                 (dig['mutations affecting disease course'] == 1)]
    dig_SR['Relation'] = ['Semantic_Relation_DiG'] * len(dig_SR)
    dig_SR = dig_SR[['Disease', 'Relation', 'Gene', 'Inference_Score']]

    dig_res = pd.concat((dig_associate, dig_downregulates, dig_upregulates, dig_IR, dig_SR))
    dig_res = dig_res.rename(columns={'Disease': 'Head', 'Gene': 'Tail'})

    dig_res.loc[dig_res['Relation'] != 'Inferred_Relation_DiG', 'Inference_Score'] = np.nan

    dig_res.to_csv(triplet_path + 'DiG_triplet.csv', index=False)
    
    print('DiG triplets generated.')


# Extracting triplets between disease and pathway entities.
def DiPwy_triplets(kg_folder, triplet_path):
    dipwy = pd.read_csv(kg_folder + 'Relation/Di_Pwy_res.csv')

    dipwy['Relation'] = ['Association_DiPwy'] * len(dipwy)
    dipwy['Inference_Score'] = [''] * len(dipwy)
    dipwy_res = dipwy[['Disease', 'Relation', 'Pathway', 'Inference_Score']]
    dipwy_res = dipwy_res.rename(columns={'Disease': 'Head', 'Pathway': 'Tail'})

    dipwy_res.to_csv(triplet_path + 'DiPwy_triplet.csv', index=False)
    
    print('DiPwy triplets generated.')
    

# Extracting triplets between disease and symptom entities.
def DiSy_triplets(kg_folder, triplet_path):
    disy = pd.read_csv(kg_folder + 'Relation/Di_Sy_res.csv')

    disy['Relation'] = ['Present_DiSy'] * len(disy)
    disy['Inference_Score'] = [''] * len(disy)
    disy_res = disy[['Disease', 'Relation', 'Symptom', 'Inference_Score']]
    disy_res = disy_res.rename(columns={'Disease': 'Head', 'Symptom': 'Tail'})

    disy_res.to_csv(triplet_path + 'DiSy_triplet.csv', index=False)
    
    print('DiSy triplets generated.')
    
    
# Extracting triplets between gene entities.
def GG_triplets(kg_folder, triplet_path):
    gg = pd.read_csv(kg_folder + 'Relation/G_G_res.csv')

    gg_covaries = gg[gg['Covaries'] == 1]
    gg_covaries['Relation'] = ['Covaries_GG'] * len(gg_covaries)
    gg_covaries['Inference_Score'] = [''] * len(gg_covaries)
    gg_covaries = gg_covaries[['Gene_1', 'Relation', 'Gene_2', 'Inference_Score']]

    gg_interacts = gg[gg['Interacts'] == 1]
    gg_interacts['Relation'] = ['Interacts_GG'] * len(gg_interacts)
    gg_interacts['Inference_Score'] = [''] * len(gg_interacts)
    gg_interacts = gg_interacts[['Gene_1', 'Relation', 'Gene_2', 'Inference_Score']]

    gg_regulates = gg[gg['Regulates'] == 1]
    gg_regulates['Relation'] = ['Regulates_GG'] * len(gg_regulates)
    gg_regulates['Inference_Score'] = [''] * len(gg_regulates)
    gg_regulates = gg_regulates[['Gene_1', 'Relation', 'Gene_2', 'Inference_Score']]

    gg_associate = gg[gg['Associate'] == 1]
    gg_associate['Relation'] = ['Associate_GG'] * len(gg_associate)
    gg_associate['Inference_Score'] = [''] * len(gg_associate)
    gg_associate = gg_associate[['Gene_1', 'Relation', 'Gene_2', 'Inference_Score']]

    gg_SR = gg[
        (gg['activates, stimulates'] == 1) | (gg['production by cell population'] == 1) | (gg['regulation'] == 1) |
        (gg['binding, ligand (esp. receptors)'] == 1) | (gg['signaling pathway'] == 1) |
        (gg['increases expression/production'] == 1) | (gg['same protein or complex'] == 1) |
        (gg['enhances response'] == 1) | (gg['affects expression/production (neutral)'] == 1) |
        (gg['physical association'] == 1) | (gg['association'] == 1) | (gg['colocalization'] == 1) |
        (gg['dephosphorylation reaction'] == 1) | (gg['cleavage reaction'] == 1) | (gg['direct interation'] == 1) |
        (gg['ADP ribosylation reaction'] == 1) | (gg['ubiquitination reaction'] == 1) |
        (gg['phosphorylation reaction'] == 1) | (gg['protein cleavage'] == 1)]
    gg_SR['Relation'] = ['Semantic_Relation_GG'] * len(gg_SR)
    gg_SR['Inference_Score'] = [''] * len(gg_SR)
    gg_SR = gg_SR[['Gene_1', 'Relation', 'Gene_2', 'Inference_Score']]

    gg_res = pd.concat((gg_covaries, gg_interacts, gg_regulates, gg_associate, gg_SR))
    gg_res = gg_res.rename(columns={'Gene_1': 'Head', 'Gene_2': 'Tail'})

    gg_res.to_csv(triplet_path + 'GG_triplet.csv', index=False)
    
    print('GG triplets generated.')
    
    
# Extracting triplets between gene and pathway entities.
def GPwy_triplets(kg_folder, triplet_path):
    gpwy = pd.read_csv(kg_folder + 'Relation/G_Pwy_res.csv')

    gpwy['Relation'] = ['Associate_GPwy'] * len(gpwy)
    gpwy['Inference_Score'] = [''] * len(gpwy)
    gpwy_res = gpwy[['Gene', 'Relation', 'Pathway', 'Inference_Score']]
    gpwy_res = gpwy_res.rename(columns={'Gene': 'Head', 'Pathway': 'Tail'})

    gpwy_res.to_csv(triplet_path + 'GPwy_triplet.csv', index=False)
    
    print('GPwy triplets generated.')
    

#############################################################################
# This function will return a csv file, which combines all triplets extracted from the above functions.
# triplet_path: data dir
# excluded_entity: a list of entities to exclude. Triplets containing any node in the list will be excluded 
def generate_triplet_set(triplet_path, 
                         included_pair_type = [
                                 'DDi', 'DiG', 'DG', 'GG', 'DD', 'DiDi',
                                 'GPwy', 'DiPwy', 'DPwy', 'DiSy',  'DSE'],
                         excluded_entity=[]):
    if len(excluded_entity) == 0:
        print('Combining triplets without excluding any information')
    else:
        print('Combining triplets, excluding tripltes regarding to entities: %s' % excluded_entity)
    
    if len(included_pair_type) == 0:
        print('Error!!! Please specify a list of triplet types you want to include for analysis!')
    
    triplet_set =pd.DataFrame(columns=['Head	', 'Relation	', 'Tail	', 'Inference_Score']) 
    
    data_df_list = []
    if 'DDi' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DDi_triplet.csv'))
        
    if 'DiG' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DiG_triplet.csv'))
        
    if 'DG' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DG_triplet.csv'))
        
    if 'GG' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'GG_triplet.csv')) 
    
    if 'DD' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DD_triplet.csv'))
    
    if 'DiDi' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DiDi_triplet.csv'))
        
    if 'GPwy' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'GPwy_triplet.csv'))
        
    if 'DiPwy' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DiPwy_triplet.csv'))
        
    if 'DPwy' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DPwy_triplet.csv'))
        
    if 'DiSy' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DiSy_triplet.csv'))
        
    if 'DSE' in included_pair_type:
        data_df_list.append(pd.read_csv(triplet_path + 'DSE_triplet.csv'))
    
#    for pt in included_pair_type:
#        temp_df = pd.read_csv(triplet_path + pt + '_triplet.csv')
#        triplet_set = pd.concat([triplet_set, temp_df])
#    DDi_triplet = pd.read_csv(triplet_path + 'DDi_triplet.csv')
#    DiG_triplet = pd.read_csv(triplet_path + 'DiG_triplet.csv')
#    DG_triplet = pd.read_csv(triplet_path + 'DG_triplet.csv')
#    GG_triplet = pd.read_csv(triplet_path + 'GG_triplet.csv')
#    DD_triplet = pd.read_csv(triplet_path + 'DD_triplet.csv')
#    DiDi_triplet = pd.read_csv(triplet_path + 'DiDi_triplet.csv')
#    GPwy_triplet = pd.read_csv(triplet_path + 'GPwy_triplet.csv')
#    DiPwy_triplet = pd.read_csv(triplet_path + 'DiPwy_triplet.csv')
#    DPwy_triplet = pd.read_csv(triplet_path + 'DPwy_triplet.csv')
#    DiSy_triplet = pd.read_csv(triplet_path + 'DiSy_triplet.csv')
#    DSE_triplet = pd.read_csv(triplet_path + 'DSE_triplet.csv')
#
    triplet_set = pd.concat(data_df_list)
    triplet_set = triplet_set[['Head', 'Relation', 'Tail']]
    
    triplet_set = triplet_set[~(triplet_set['Head'].isin(excluded_entity))]
    triplet_set = triplet_set[~(triplet_set['Tail'].isin(excluded_entity))]
    triplet_set = triplet_set.reset_index(drop=True)

    triplet_set.to_csv(triplet_path + 'triplet_whole.csv', index=False)
    
    print('---------------------------------')
    print('A total of %s triplets generated.' % len(triplet_set))
    print('---------------------------------')


#############################################################################
# This function converts the triplet csv file to tsv format, according to DGL requirment
def generate_DGL_training_set(triplet_path, output_path='Data/dataset/'):
    triplets_set = pd.read_csv(triplet_path + 'triplet_whole.csv')

    triples = triplets_set.values.tolist()
    train_set = np.arange(len(triples)).tolist()
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with open(output_path + "training_triplet_whole.tsv", 'w+') as f:
        for idx in train_set:
            f.writelines("{}\t{}\t{}\n".format(triples[idx][0], triples[idx][1], triples[idx][2]))
            
    print("A total of %s triplets have been converted to TSV file, following DGL requirments" % len(train_set))
    
#############################################################################
# This function converts the triplet csv file to tsv format, according to DGL requirment
def generate_DGL_data_set(triplet_path, output_path='Data/dataset/', train_val_test_ratio=[.9, .05, .05]):
    
    triplets_set = pd.read_csv(triplet_path + 'triplet_whole.csv')

    triples = triplets_set.values.tolist()
    num_triples = len(triples)
    
    seed = np.arange(num_triples)
    np.random.shuffle(seed)
    
    train_cnt = int(num_triples * train_val_test_ratio[0])
    valid_cnt = int(num_triples * train_val_test_ratio[1])
    train_set = seed[:train_cnt]
    train_set = train_set.tolist()
    valid_set = seed[train_cnt:train_cnt+valid_cnt].tolist()
    test_set = seed[train_cnt+valid_cnt:].tolist()
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    with open(output_path + "training_triplets.tsv", 'w+') as f:
        for idx in train_set:
            f.writelines("{}\t{}\t{}\n".format(triples[idx][0], triples[idx][1], triples[idx][2]))
            
    with open(output_path + "validation_triplets.tsv", 'w+') as f:
        for idx in valid_set:
            f.writelines("{}\t{}\t{}\n".format(triples[idx][0], triples[idx][1], triples[idx][2]))
    
    with open(output_path + "testing_triplets.tsv", 'w+') as f:
        for idx in test_set:
            f.writelines("{}\t{}\t{}\n".format(triples[idx][0], triples[idx][1], triples[idx][2]))
    
    
    whole_set = np.arange(len(triples)).tolist()
    with open(output_path + "whole_triplets.tsv", 'w+') as f:
        for idx in whole_set:
            f.writelines("{}\t{}\t{}\n".format(triples[idx][0], triples[idx][1], triples[idx][2]))
            
    print("Data set created successfully, including:")
    print("---- Training set: %s treiplets" % len(train_set))
    print("---- Validation set: %s treiplets" % len(valid_set))
    print("---- Testing set: %s treiplets" % len(test_set))
    print("---- Whole set: %s treiplets" % len(whole_set))

#############################################################################
# This function will return three obj files,
# 1. entity_map: the dictionary that map the entities to their ID
# 2. entity_id_map: the dictionary that map the ID to the corresponding entities.
# 3. relation_map: the dictionary that map the relations to their ID
def generate_entity_ID_dict():
    entity_df = pd.read_table('Data/UI_emb/entities.tsv', header=None)
    entity_df = entity_df.dropna().reset_index(drop=True)

    entity_map = entity_df.set_index(1)[0].to_dict()
    entity_id_map = entity_df.set_index(0)[1].to_dict()

    relation_df = pd.read_table('Data/UI_emb/relations.tsv', header=None)
    relation_df = relation_df.dropna().reset_index(drop=True)

    relation_map = relation_df.set_index(1)[0].to_dict()

    with open("'Data/entity_map.obj', 'wb'") as f:
        pickle.dump(entity_map, f)
    f.close()

    with open("'Data/entity_id_map.obj', 'wb'") as f:
        pickle.dump(entity_id_map, f)
    f.close()

    with open("'Data/relation_map.obj', 'wb'") as f:
        pickle.dump(relation_map, f)
    f.close()

#############################################################################
# This function will return two obj files,
# 1. **_head_dict: the dictionary that a head entity map to a list of tail entities.
# 2. **_tail_dict: the dictionary that a tail entity map to a list of head entities.
# ** indicates the triplets table's name
def generate_triplet_head_tail_dict(triplet_name):
    triplet = pd.read_csv('Data/triplets/' + triplet_name + '_triplet.csv')
    head_list = list(triplet.drop_duplicates(subset='Head', keep='first')['Head'])
    tail_list = list(triplet.drop_duplicates(subset='Tail', keep='first')['Tail'])

    head_dict = {}
    for head_entity in head_list:
        sub_df = triplet.loc[triplet['Head'] == head_entity]
        mapped_tail_list = list(sub_df.drop_duplicates(subset='Tail', keep='first')['Tail'])
        head_dict[head_entity] = mapped_tail_list

    tail_dict = {}
    for tail_entity in tail_list:
        sub_df = triplet.loc[triplet['Tail'] == tail_entity]
        mapped_head_list = list(sub_df.drop_duplicates(subset='Head', keep='first')['Head'])
        tail_dict[tail_entity] = mapped_head_list

    with open("Data/triplets_dict/" + triplet_name + '_head_dict.obj', 'wb') as f:
        pickle.dump(head_dict, f)
    f.close()

    with open("Data/triplets_dict/" + triplet_name + '_tail_dict.obj', 'wb') as f:
        pickle.dump(tail_dict, f)
    f.close()