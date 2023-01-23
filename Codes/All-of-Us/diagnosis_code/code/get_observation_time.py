###### This code is used to get observation time for all patients
###### and also patient x demographic matrix
###### only need to run once on AD datasets

import pandas
import os

# This query represents dataset "AD" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_57981269_person_sql = """
    SELECT
        person.person_id,
        person.gender_concept_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        person.race_concept_id,
        p_race_concept.concept_name as race,
        person.ethnicity_concept_id,
        p_ethnicity_concept.concept_name as ethnicity,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.person` person 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_ethnicity_concept 
            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (
            SELECT
                distinct person_id  
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (
                    SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id,
                            entry_date,
                            concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (
                                concept_id IN (35207357, 35207359, 44826537, 35207358) 
                                AND is_standard = 0 
                            )) criteria ) 
                )"""

dataset_57981269_person_df = pandas.read_gbq(
    dataset_57981269_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")


# This query represents dataset "AD" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_57981269_condition_sql = """
    SELECT
        c_occurrence.person_id,
        c_occurrence.condition_concept_id,
        c_standard_concept.concept_name as standard_concept_name,
        c_standard_concept.concept_code as standard_concept_code,
        c_standard_concept.vocabulary_id as standard_vocabulary,
        c_occurrence.condition_start_datetime,
        c_occurrence.condition_end_datetime,
        c_occurrence.condition_type_concept_id,
        c_type.concept_name as condition_type_concept_name,
        c_occurrence.stop_reason,
        c_occurrence.visit_occurrence_id,
        visit.concept_name as visit_occurrence_concept_name,
        c_occurrence.condition_source_value,
        c_occurrence.condition_source_concept_id,
        c_source_concept.concept_name as source_concept_name,
        c_source_concept.concept_code as source_concept_code,
        c_source_concept.vocabulary_id as source_vocabulary,
        c_occurrence.condition_status_source_value,
        c_occurrence.condition_status_concept_id,
        c_status.concept_name as condition_status_concept_name 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_source_concept_id IN  (
                    SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                    JOIN
                        (
                            select
                                cast(cr.id as string) as id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr 
                            WHERE
                                concept_id IN (
                                    35207359, 35207358, 44826537, 1568293, 35207357, 35207356
                                ) 
                                AND full_text LIKE '%_rank1]%'
                        ) a 
                            ON (
                                c.path LIKE CONCAT('%.',
                            a.id,
                            '.%') 
                            OR c.path LIKE CONCAT('%.',
                            a.id) 
                            OR c.path LIKE CONCAT(a.id,
                            '.%') 
                            OR c.path = a.id) 
                        WHERE
                            is_standard = 0 
                            AND is_selectable = 1
                        )
                )  
                AND (
                    c_occurrence.PERSON_ID IN (
                        SELECT
                            distinct person_id  
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                        WHERE
                            cb_search_person.person_id IN (
                                SELECT
                                    criteria.person_id 
                                FROM
                                    (SELECT
                                        DISTINCT person_id,
                                        entry_date,
                                        concept_id 
                                    FROM
                                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                                    WHERE
                                        (
                                            concept_id IN (35207357, 35207359, 44826537, 35207358) 
                                            AND is_standard = 0 
                                        )) criteria ) 
                            )
                        )
                ) c_occurrence 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
                    ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
                    ON c_occurrence.condition_type_concept_id = c_type.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                    ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
                    ON v.visit_concept_id = visit.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
                    ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
                    ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

dataset_57981269_condition_df = pandas.read_gbq(
    dataset_57981269_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

###################
import pandas as pd
import pickle

###input: person_df, condition_df, disease name feature, ob time diff
person_df=dataset_57981269_person_df
condition_df=dataset_57981269_condition_df
time_diff=5

#loop for each patient to find earliest AD condition_start_time 
all_patient_ids=person_df['person_id'].unique()
all_start_time={}
ob_time={}
for i in all_patient_ids:
    starts=condition_df[(condition_df['person_id']==i) & (condition_df['standard_concept_name'].str.contains('Alzheimer'))] 
    starts=starts['condition_start_datetime']
    starts=pd.to_datetime(starts)
    if starts.shape[0]>0:#has AD record
        earliest=starts.sort_values().iloc[0]
        all_start_time[i]=earliest
        ob_time[i]=earliest-pd.offsets.DateOffset(years=time_diff)

#save result
ob_f = open('AD_observe_time.pickle','wb')
pickle.dump(ob_time,ob_f)

#####################
### get patient by demographic matrix
person_df.to_csv('patient_demographic.csv', index=False)
