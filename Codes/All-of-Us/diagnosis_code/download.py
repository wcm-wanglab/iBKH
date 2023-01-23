#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas
import os


# This query represents dataset "Diabetes" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_person_sql = """
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

dataset_56126755_person_df = pandas.read_gbq(
    dataset_56126755_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_person_df.to_csv('../patient_disease/diabetes_person_df.csv', index=False)



# This query represents dataset "Diabetes" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    45572770, 45600362, 45582461, 45553483, 45539106, 44829590, 45582459, 44823798, 37200978, 35225328, 44821617, 45563059, 35225076, 37200977
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_56126755_observation_df = pandas.read_gbq(
    dataset_56126755_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_observation_df.to_csv('../patient_disease/diabetes_observation_df.csv', index=False)


# In[ ]:


# This query represents dataset "Diabetes" for domain "measurement" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_source_concept_id IN  (
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
                                    44821617
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
                    measurement.PERSON_ID IN (
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
                ) measurement 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
                    ON measurement.measurement_concept_id = m_standard_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
                    ON measurement.measurement_type_concept_id = m_type.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
                    ON measurement.operator_concept_id = m_operator.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
                    ON measurement.value_as_concept_id = m_value.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
                    ON measurement.unit_concept_id = m_unit.concept_id 
            LEFT JOIn
                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                    ON measurement.visit_occurrence_id = v.visit_occurrence_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
                    ON v.visit_concept_id = m_visit.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
                    ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_56126755_measurement_df = pandas.read_gbq(
    dataset_56126755_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_measurement_df.to_csv('../patient_disease/diabetes_measurement_df.csv', index=False)


# In[ ]:


# This query represents dataset "Diabetes" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_condition_sql = """
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
                                    37200202, 37200239, 45595790, 44836914, 35206879, 45600637, 37200149, 37200228, 45595789, 45537960, 37200046, 45566723, 45533015, 37200207, 37200225, 37200278, 45557106, 37200171, 37200032, 45581359, 44819503, 45606547, 37200233, 44831046, 35206885, 37200135, 37200235, 37200247, 37200258, 37200279, 45581349, 45576435, 45605392, 45533024, 37200216, 45561957, 37200180, 44832188, 45542736, 1567969, 45557112, 44829879, 45600639, 37200248, 45595805, 45547633, 37200224, 44835748, 37200192, 37200198, 37200036, 45581347, 45558215, 45534189, 37200211, 37200034, 45582461, 37200090, 1567966, 45543923, 45605402, 37200152, 37200191, 37200058, 1567949, 44837245, 37200144, 44832193, 37200052, 37200028, 45581344, 45576431, 37200303, 45561956, 44836913, 35206882, 45542738, 45566726, 45591034, 37200205, 1326493, 37200200, 45586136, 45571653, 44829880, 37200280, 37200076, 45539106, 45561946, 1567940, 1567971, 45537963, 45533022, 45576432, 45592198, 45552377, 45586138, 45571661, 37200157, 45547623, 44830221, 37200234, 44827616, 45557113, 37200168, 37200190, 37200030, 45595792, 45533012, 45552382, 44819498, 37200189, 45601785, 44833366, 37200160, 45587294, 44832194, 37200275, 37200306, 37200059, 45561943, 45561945, 45605403, 37200175, 45582457, 45581353, 37200269, 44820685, 45605394, 37200251, 37200035, 44826461, 45547625, 1326492, 45586145, 45533018, 37200147, 37200151, 37200222, 45586139, 44820681, 45572771, 37200210, 37200043, 37200083, 45587291, 44833368, 37200201, 45561955, 45534190, 44824074, 45571662, 37200163, 37200170, 45576438, 37200217, 37200291, 37200042, 44836917, 44832533, 37200029, 45582458, 1567975, 37200146, 37200150, 37200311, 44835750, 45537955, 45533019, 37200977, 45595804, 44825349, 45587292, 1567988, 45572770, 37200156, 45552372, 45595794, 45561953, 45561948, 45571650, 45563060, 45548715, 45606548, 37200262, 45552374, 44819499, 37200187, 37200074, 45586133, 45595802, 44832187, 45576430, 45542730, 37200158, 37200249, 37200281, 37200053, 45547624, 44820684, 45533021, 45533023, 45561947, 45558214, 45605401, 37200049, 37200063, 45552376, 45576443, 37200227, 45557110, 45605404, 44829877, 45591027, 37200229, 37200301, 45547627, 45567896, 37200132, 37200148, 37200077, 44819501, 37200040, 37200081, 44828790, 44828794, 45605393, 37200047, 45539105, 44829876, 37200130, 37200238, 45581355, 45557116, 37200302, 44835751, 37200209, 44832532, 1567922, 37200143, 37200284, 45553484, 45566734, 45537962, 37200246, 44819504, 37200045, 45587293, 44828792, 45576439, 44835749, 37200038, 45561944, 44820680, 45566736, 45566731, 37200161, 45552383, 35206884, 44829881, 45600640, 44828795, 45547622, 37200223, 45561949, 45547632, 45537954, 44822933, 37200261, 45595797, 45552386, 45533020, 45567897, 37200245, 37200085, 45542728, 45595791, 45557107, 37200188, 37200199, 45547617, 44827617, 37200031, 45595798, 1567943, 45571649, 45566724, 45571659, 45563059, 45605398, 37200134, 45547620, 44833364, 45563058, 44826460, 44832191, 45566735, 44821787, 37200072, 37200082, 45605395, 37200252, 45537957, 45552375, 1567960, 1567958, 45537961, 44821785, 45600641, 37200166, 44827615, 45591029, 45547626, 44836918, 37200263, 37200078, 45552381, 44828793, 44825264, 45586142, 45576433, 1567939, 37200230, 37200044, 37200056, 45600642, 45586132, 44829878, 45600635, 45600636, 37200176, 37200057, 37200091, 45552379, 44834548, 45542731, 1567964, 45586135, 35206911, 37200304, 45561942, 45587295, 44836084, 45600633, 37200039, 37200088, 1567944, 37200979, 37200265, 44831047, 44832192, 45581343, 45595793, 45533017, 37200237, 37200050, 45595799, 45547618, 1567959, 37200221, 44819500, 45571654, 44829882, 37200203, 37200232, 45553483, 45533013, 37200244, 45577566, 45542733, 1567965, 37200219, 45586140, 45561941, 45582459, 1571691, 45533011, 37200159, 45586144, 45543921, 37200033, 45552373, 45605405, 45557109, 45600638, 44836915, 45542741, 37200172, 44824072, 37200242, 45576434, 45606546, 44822936, 45552388, 44832190, 45542737, 37200220, 45547635, 37200141, 45576437, 45576440, 45576447, 37200240, 45563061, 37200154, 37200181, 37200260, 45534188, 45581354, 44833365, 45600634, 45581352, 37200110, 45595795, 44819502, 37200145, 37200071, 37200102, 44836916, 45582460, 44820683, 35206881, 44822934, 37200254, 44822935, 45591030, 45591033, 37200215, 45566733, 45571652, 45537958, 45577567, 37200162, 37200067, 45591024, 37200142, 37200218, 44831045, 45581348, 45542732, 37200255, 37200305, 45547621, 45576446, 35206878, 45571658, 45533009, 37200186, 37200978, 37200243, 44826459, 45581342, 37200204, 37200206, 37200027, 45596929, 45605397, 37200073, 1571690, 45571651, 44822932, 45591026, 37200214, 45561940, 37200253, 37200257, 37200041, 45561954, 45600644, 45595803, 44825263, 44834549, 37200177, 44833367, 45533010, 45591023, 37200165, 37200212, 45581346, 37200155, 37200213, 45586143, 1567956, 37200164, 44822938, 44822099, 45566729, 45591031, 37200153, 37200310, 45552385, 37200051, 37200208, 45581350, 37200266, 45537953, 37200274, 37200054, 44824073, 45557108, 37200167, 37200037, 37200075, 44824071, 44820682, 45581358, 37200185
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

dataset_56126755_condition_df = pandas.read_gbq(
    dataset_56126755_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_condition_df.to_csv('../patient_disease/diabetes_condition_df.csv', index=False)


# This query represents dataset "Diabetes" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_person_sql = """
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

dataset_56126755_person_df = pandas.read_gbq(
    dataset_56126755_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_person_df.to_csv('../patient_disease/diabetes_person_df.csv', index=False)



# This query represents dataset "Diabetes" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    45572770, 45600362, 45582461, 45553483, 45539106, 44829590, 45582459, 44823798, 37200978, 35225328, 44821617, 45563059, 35225076, 37200977
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_56126755_observation_df = pandas.read_gbq(
    dataset_56126755_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_observation_df.to_csv('../patient_disease/diabetes_observation_df.csv', index=False)


# In[ ]:


# This query represents dataset "Diabetes" for domain "measurement" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_source_concept_id IN  (
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
                                    44821617
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
                    measurement.PERSON_ID IN (
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
                ) measurement 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
                    ON measurement.measurement_concept_id = m_standard_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
                    ON measurement.measurement_type_concept_id = m_type.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
                    ON measurement.operator_concept_id = m_operator.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
                    ON measurement.value_as_concept_id = m_value.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
                    ON measurement.unit_concept_id = m_unit.concept_id 
            LEFT JOIn
                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                    ON measurement.visit_occurrence_id = v.visit_occurrence_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
                    ON v.visit_concept_id = m_visit.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
                    ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_56126755_measurement_df = pandas.read_gbq(
    dataset_56126755_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_measurement_df.to_csv('../patient_disease/diabetes_measurement_df.csv', index=False)


# In[ ]:


# This query represents dataset "Diabetes" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_56126755_condition_sql = """
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
                                    37200202, 37200239, 45595790, 44836914, 35206879, 45600637, 37200149, 37200228, 45595789, 45537960, 37200046, 45566723, 45533015, 37200207, 37200225, 37200278, 45557106, 37200171, 37200032, 45581359, 44819503, 45606547, 37200233, 44831046, 35206885, 37200135, 37200235, 37200247, 37200258, 37200279, 45581349, 45576435, 45605392, 45533024, 37200216, 45561957, 37200180, 44832188, 45542736, 1567969, 45557112, 44829879, 45600639, 37200248, 45595805, 45547633, 37200224, 44835748, 37200192, 37200198, 37200036, 45581347, 45558215, 45534189, 37200211, 37200034, 45582461, 37200090, 1567966, 45543923, 45605402, 37200152, 37200191, 37200058, 1567949, 44837245, 37200144, 44832193, 37200052, 37200028, 45581344, 45576431, 37200303, 45561956, 44836913, 35206882, 45542738, 45566726, 45591034, 37200205, 1326493, 37200200, 45586136, 45571653, 44829880, 37200280, 37200076, 45539106, 45561946, 1567940, 1567971, 45537963, 45533022, 45576432, 45592198, 45552377, 45586138, 45571661, 37200157, 45547623, 44830221, 37200234, 44827616, 45557113, 37200168, 37200190, 37200030, 45595792, 45533012, 45552382, 44819498, 37200189, 45601785, 44833366, 37200160, 45587294, 44832194, 37200275, 37200306, 37200059, 45561943, 45561945, 45605403, 37200175, 45582457, 45581353, 37200269, 44820685, 45605394, 37200251, 37200035, 44826461, 45547625, 1326492, 45586145, 45533018, 37200147, 37200151, 37200222, 45586139, 44820681, 45572771, 37200210, 37200043, 37200083, 45587291, 44833368, 37200201, 45561955, 45534190, 44824074, 45571662, 37200163, 37200170, 45576438, 37200217, 37200291, 37200042, 44836917, 44832533, 37200029, 45582458, 1567975, 37200146, 37200150, 37200311, 44835750, 45537955, 45533019, 37200977, 45595804, 44825349, 45587292, 1567988, 45572770, 37200156, 45552372, 45595794, 45561953, 45561948, 45571650, 45563060, 45548715, 45606548, 37200262, 45552374, 44819499, 37200187, 37200074, 45586133, 45595802, 44832187, 45576430, 45542730, 37200158, 37200249, 37200281, 37200053, 45547624, 44820684, 45533021, 45533023, 45561947, 45558214, 45605401, 37200049, 37200063, 45552376, 45576443, 37200227, 45557110, 45605404, 44829877, 45591027, 37200229, 37200301, 45547627, 45567896, 37200132, 37200148, 37200077, 44819501, 37200040, 37200081, 44828790, 44828794, 45605393, 37200047, 45539105, 44829876, 37200130, 37200238, 45581355, 45557116, 37200302, 44835751, 37200209, 44832532, 1567922, 37200143, 37200284, 45553484, 45566734, 45537962, 37200246, 44819504, 37200045, 45587293, 44828792, 45576439, 44835749, 37200038, 45561944, 44820680, 45566736, 45566731, 37200161, 45552383, 35206884, 44829881, 45600640, 44828795, 45547622, 37200223, 45561949, 45547632, 45537954, 44822933, 37200261, 45595797, 45552386, 45533020, 45567897, 37200245, 37200085, 45542728, 45595791, 45557107, 37200188, 37200199, 45547617, 44827617, 37200031, 45595798, 1567943, 45571649, 45566724, 45571659, 45563059, 45605398, 37200134, 45547620, 44833364, 45563058, 44826460, 44832191, 45566735, 44821787, 37200072, 37200082, 45605395, 37200252, 45537957, 45552375, 1567960, 1567958, 45537961, 44821785, 45600641, 37200166, 44827615, 45591029, 45547626, 44836918, 37200263, 37200078, 45552381, 44828793, 44825264, 45586142, 45576433, 1567939, 37200230, 37200044, 37200056, 45600642, 45586132, 44829878, 45600635, 45600636, 37200176, 37200057, 37200091, 45552379, 44834548, 45542731, 1567964, 45586135, 35206911, 37200304, 45561942, 45587295, 44836084, 45600633, 37200039, 37200088, 1567944, 37200979, 37200265, 44831047, 44832192, 45581343, 45595793, 45533017, 37200237, 37200050, 45595799, 45547618, 1567959, 37200221, 44819500, 45571654, 44829882, 37200203, 37200232, 45553483, 45533013, 37200244, 45577566, 45542733, 1567965, 37200219, 45586140, 45561941, 45582459, 1571691, 45533011, 37200159, 45586144, 45543921, 37200033, 45552373, 45605405, 45557109, 45600638, 44836915, 45542741, 37200172, 44824072, 37200242, 45576434, 45606546, 44822936, 45552388, 44832190, 45542737, 37200220, 45547635, 37200141, 45576437, 45576440, 45576447, 37200240, 45563061, 37200154, 37200181, 37200260, 45534188, 45581354, 44833365, 45600634, 45581352, 37200110, 45595795, 44819502, 37200145, 37200071, 37200102, 44836916, 45582460, 44820683, 35206881, 44822934, 37200254, 44822935, 45591030, 45591033, 37200215, 45566733, 45571652, 45537958, 45577567, 37200162, 37200067, 45591024, 37200142, 37200218, 44831045, 45581348, 45542732, 37200255, 37200305, 45547621, 45576446, 35206878, 45571658, 45533009, 37200186, 37200978, 37200243, 44826459, 45581342, 37200204, 37200206, 37200027, 45596929, 45605397, 37200073, 1571690, 45571651, 44822932, 45591026, 37200214, 45561940, 37200253, 37200257, 37200041, 45561954, 45600644, 45595803, 44825263, 44834549, 37200177, 44833367, 45533010, 45591023, 37200165, 37200212, 45581346, 37200155, 37200213, 45586143, 1567956, 37200164, 44822938, 44822099, 45566729, 45591031, 37200153, 37200310, 45552385, 37200051, 37200208, 45581350, 37200266, 45537953, 37200274, 37200054, 44824073, 45557108, 37200167, 37200037, 37200075, 44824071, 44820682, 45581358, 37200185
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

dataset_56126755_condition_df = pandas.read_gbq(
    dataset_56126755_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56126755_condition_df.to_csv('../patient_disease/diabetes_condition_df.csv', index=False)



# This query represents dataset "amnesia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_27217580_person_sql = """
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

dataset_27217580_person_df = pandas.read_gbq(
    dataset_27217580_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_27217580_person_df.to_csv('../patient_disease/amnesia_person_df.csv', index=False)




# This query represents dataset "amnesia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_27217580_condition_sql = """
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
                                    44826504, 35211347, 35211348, 35207178, 35207397, 35211349, 44828985
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

dataset_27217580_condition_df = pandas.read_gbq(
    dataset_27217580_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_27217580_condition_df.to_csv('../patient_disease/amnesia_condition_df.csv', index=False)



# This query represents dataset "anorexia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_81799257_person_sql = """
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

dataset_81799257_person_df = pandas.read_gbq(
    dataset_81799257_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_81799257_person_df.to_csv('../patient_disease/anorexia_person_df.csv', index=False)



# This query represents dataset "anorexia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_81799257_condition_sql = """
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
                                    44826521, 35211405, 45581463, 45552490, 45605500, 44832712
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

dataset_81799257_condition_df = pandas.read_gbq(
    dataset_81799257_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_81799257_condition_df.to_csv('../patient_disease/anorexia_condition_df.csv', index=False)



# This query represents dataset "anxiety" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_88852753_person_sql = """
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

dataset_88852753_person_df = pandas.read_gbq(
    dataset_88852753_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_88852753_person_df.to_csv('../patient_disease/anxiety_person_df.csv', index=False)


# This query represents dataset "anxiety" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_88852753_condition_sql = """
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
                                    45552480, 44827661, 35207173, 45533102, 35207171, 44828835, 45566777, 45538021, 45552452, 45547707, 44832241, 45542847, 44821824, 44824121, 44820725, 44829928, 44828834, 45566843, 44832240, 45566844, 44836970, 44821812, 44826507, 45600693, 45538033, 45581459, 45586208, 45533078, 45600743, 44835791, 44836973, 44829927, 44822995, 44824120, 44836972, 45562031, 45562045, 45586242, 44834611, 45538068, 35207168, 44826505, 44819544, 35207120, 45591124, 44829926, 44835792, 45600742, 45557205, 45562046, 35207167, 45538067, 45533069, 44828833, 45562013, 45547691, 44833403, 35207172, 45566781, 35207169, 44826503, 45586241, 45538069, 45600744, 44826506, 1568221, 45566802, 45595860, 35207272, 44832253, 44831093, 44826504, 45576544, 45581417, 35207170, 45533104, 44836971, 45538040, 44831094, 44822985, 44825298, 45571762, 45533103, 44832239
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

dataset_88852753_condition_df = pandas.read_gbq(
    dataset_88852753_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_88852753_condition_df.to_csv('../patient_disease/anxiety_condition_df.csv', index=False)



# This query represents dataset "aphrenia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_79720817_person_sql = """
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

dataset_79720817_person_df = pandas.read_gbq(
    dataset_79720817_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_79720817_person_df.to_csv('../patient_disease/aphrenia_person_df.csv', index=False)


# This query represents dataset "aphrenia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_79720817_condition_sql = """
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
                                    44821814, 44831078, 45566776, 1568088, 45547675, 44836959, 44819535, 1568295, 45538000, 44827641, 44829914, 44835772, 44819534, 44831122, 45547690, 45538103, 44820709, 44826489, 45547730, 45600684, 44824106, 44831083, 44821810, 44821811, 45552458, 35207114, 1568090, 45605533, 45595842, 44836954, 44835825, 44824152, 44824105, 44832219, 44831079, 44833435, 44834581, 45591076, 44829917, 45595843, 44829915, 45591073, 44827644, 44820708, 44835773
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

dataset_79720817_condition_df = pandas.read_gbq(
    dataset_79720817_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_79720817_condition_df.to_csv('../patient_disease/aphrenia_condition_df.csv', index=False)



# This query represents dataset "apnoea" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_34069531_person_sql = """
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

dataset_34069531_person_df = pandas.read_gbq(
    dataset_34069531_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_34069531_person_df.to_csv('../patient_disease/apnoea_person_df.csv', index=False)


# This query represents dataset "apnoea" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_34069531_condition_sql = """
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
                                    44828145, 44819570, 1568366, 45595947, 44823443, 45581499, 44827686, 44819568, 35210514, 45576591, 45552539, 44835823, 45600793, 45605558, 45571812, 44837423, 44826960, 45562095, 44826532, 44835822, 44820747, 45606793, 44829286, 44822298, 44832262, 35210513, 44825323, 44819569, 45581498
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

dataset_34069531_condition_df = pandas.read_gbq(
    dataset_34069531_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_34069531_condition_df.to_csv('../patient_disease/apnoea_condition_df.csv', index=False)



# This query represents dataset "atrial fibrillation" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_24881878_person_sql = """
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

dataset_24881878_person_df = pandas.read_gbq(
    dataset_24881878_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_24881878_person_df.to_csv('../patient_disease/atrialFibrillation_person_df.csv', index=False)



# This query represents dataset "atrial fibrillation" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_24881878_condition_sql = """
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
                                    1569170, 1569171, 1553752, 45576876, 1553751, 1553753, 35207784, 44821957, 44820868, 35207785, 44824248, 1553754, 1569172, 45572094, 1569173
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

dataset_24881878_condition_df = pandas.read_gbq(
    dataset_24881878_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_24881878_condition_df.to_csv('../patient_disease/atrialFibrillation_condition_df.csv', index=False)



# This query represents dataset "cancer" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_37851189_person_sql = """
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

dataset_37851189_person_df = pandas.read_gbq(
    dataset_37851189_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_37851189_person_df.to_csv('../patient_disease/cancer_person_df.csv', index=False)



# This query represents dataset "cancer" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_37851189_condition_sql = """
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
                                    44829312, 45537931, 45597213
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

dataset_37851189_condition_df = pandas.read_gbq(
    dataset_37851189_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_37851189_condition_df.to_csv('../patient_disease/cancer_condition_df.csv', index=False)



# This query represents dataset "circadian clock disruption" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_67802858_person_sql = """
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

dataset_67802858_person_df = pandas.read_gbq(
    dataset_67802858_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_67802858_person_df.to_csv('../patient_disease/circadianClockDisruption_person_df.csv', index=False)



# This query represents dataset "circadian clock disruption" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_67802858_condition_sql = """
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
                                    44828859, 44824150, 45566885, 45566886, 44825324, 45557240, 45552538, 45591186, 45533147, 45538121, 44826533, 44836999, 44827687, 44825325, 44824135, 44835824, 44832263, 45538122, 45552537
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

dataset_67802858_condition_df = pandas.read_gbq(
    dataset_67802858_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_67802858_condition_df.to_csv('../patient_disease/circadianClockDisruption_condition_df.csv', index=False)



# This query represents dataset "cognitive decline" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_75464226_person_sql = """
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

dataset_75464226_person_df = pandas.read_gbq(
    dataset_75464226_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_75464226_person_df.to_csv('../patient_disease/cognitiveDecline_person_df.csv', index=False)




# This query represents dataset "cognitive decline" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_75464226_condition_sql = """
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
                                    45553736
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

dataset_75464226_condition_df = pandas.read_gbq(
    dataset_75464226_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_75464226_condition_df.to_csv('../patient_disease/cognitiveDecline_condition_df.csv', index=False)



# This query represents dataset "dementia with Lewy bodies" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_42942235_person_sql = """
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

dataset_42942235_person_df = pandas.read_gbq(
    dataset_42942235_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_42942235_person_df.to_csv('../patient_disease/dementiaWithLewyBodies_person_df.csv', index=False)



# This query represents dataset "dementia with Lewy bodies" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_42942235_condition_sql = """
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
                                    45547730, 44833435
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

dataset_42942235_condition_df = pandas.read_gbq(
    dataset_42942235_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_42942235_condition_df.to_csv('../patient_disease/dementiaWithLewyBodies_condition_df.csv', index=False)



# This query represents dataset "falls" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_79589804_person_sql = """
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

dataset_79589804_person_df = pandas.read_gbq(
    dataset_79589804_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_79589804_person_df.to_csv('../patient_disease/falls_person_df.csv', index=False)




# This query represents dataset "falls" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_79589804_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    45551848, 45561400, 45585584, 45590454, 45609699, 45561401, 45580791, 45575913, 45547064, 44823721, 45580788, 45542185, 45551850, 45571101, 44830685, 44835349, 45571099, 45551852, 45590455, 44827239, 44823720, 45571098, 45551851, 44835348, 45575915, 45600118, 44834176, 45600117, 45542184, 45547062, 44834177, 45595246, 45590452, 45571100, 45580789, 45575916, 44829540, 45609700
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_79589804_observation_df = pandas.read_gbq(
    dataset_79589804_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_79589804_observation_df.to_csv('../patient_disease/falls_observation_df.csv', index=False)


# This query represents dataset "falls" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_79589804_condition_sql = """
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
                                    35211335, 45609701, 45551853, 45547063, 45556604, 45575914, 45580792, 45595248
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

dataset_79589804_condition_df = pandas.read_gbq(
    dataset_79589804_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_79589804_condition_df.to_csv('../patient_disease/falls_condition_df.csv', index=False)



# This query represents dataset "frontotemporal dementia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_92520095_person_sql = """
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

dataset_92520095_person_df = pandas.read_gbq(
    dataset_92520095_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_92520095_person_df.to_csv('../patient_disease/frontotemporalDementia_person_df.csv', index=False)





# This query represents dataset "frontotemporal dementia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_92520095_condition_sql = """
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
                                    1568295, 45538103, 44824152, 45605533, 44831122, 44835825
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

dataset_92520095_condition_df = pandas.read_gbq(
    dataset_92520095_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_92520095_condition_df.to_csv('../patient_disease/frontotemporalDementia_condition_df.csv', index=False)




# This query represents dataset "hallucinations" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_85039232_person_sql = """
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

dataset_85039232_person_df = pandas.read_gbq(
    dataset_85039232_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_85039232_person_df.to_csv('../patient_disease/hallucinations_person_df.csv', index=False)



# This query represents dataset "hallucinations" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_85039232_condition_sql = """
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
                                    45595846, 45542820, 45586213, 45600726, 45595893, 45533071, 45571713, 44833913, 35211356, 45581413, 45600708, 35211358, 35211355, 45591090, 45533077, 44832220, 45595879, 45542810, 45557155, 45595871, 45591108, 45605464, 45581442, 44832221, 44819536, 45562012, 35207117, 45562021, 45571725, 45591087, 35211357
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

dataset_85039232_condition_df = pandas.read_gbq(
    dataset_85039232_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_85039232_condition_df.to_csv('../patient_disease/hallucinations_condition_df.csv', index=False)



# This query represents dataset "hearing loss" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_91914065_person_sql = """
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

dataset_91914065_person_df = pandas.read_gbq(
    dataset_91914065_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_91914065_person_df.to_csv('../patient_disease/hearingLoss_person_df.csv', index=False)




# This query represents dataset "hearing loss" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_91914065_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    35225321, 44833551, 44823107, 44824230, 44836627, 1569072
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_91914065_observation_df = pandas.read_gbq(
    dataset_91914065_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_91914065_observation_df.to_csv('../patient_disease/hearingLoss_observation_df.csv', index=False)




# This query represents dataset "hearing loss" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_91914065_condition_sql = """
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
                                    44820849, 44823104, 44833552, 44828967, 45581758, 35207622, 45605514, 44823105, 35207619, 45581757, 1569071, 44831224, 44821946, 45533422, 44832361, 45581755, 37200474, 45581752, 44830074, 44834713, 44823106, 44819688, 37200475, 44832362, 45557525, 45552762, 45605766, 45538358, 44830073, 44833551, 45581754, 44831225, 45567154, 44821945, 45552763, 35207618, 44824231, 37200478, 44820848, 45586556, 1569081, 45581756, 44831223, 37200481, 44820846, 45567153, 45591445, 45543145, 45576851, 45552761, 44819564, 45543146, 37200477, 44827776, 35207620, 37200480, 44825424, 45605767, 45576849, 44837093, 35207616, 35207621, 44821944, 45576853, 44835918, 45605768, 44835920, 45562325, 1569075, 35207617, 45547995
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

dataset_91914065_condition_df = pandas.read_gbq(
    dataset_91914065_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_91914065_condition_df.to_csv('../patient_disease/hearingLoss_condition_df.csv', index=False)




# This query represents dataset "heart failure" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_45532666_person_sql = """
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

dataset_45532666_person_df = pandas.read_gbq(
    dataset_45532666_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_45532666_person_df.to_csv('../patient_disease/heartFailure_person_df.csv', index=False)




# This query represents dataset "heart failure" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_45532666_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    35224718
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_45532666_observation_df = pandas.read_gbq(
    dataset_45532666_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_45532666_observation_df.to_csv('../patient_disease/heartFailure_observation_df.csv', index=False)




# This query represents dataset "heart failure" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_45532666_condition_sql = """
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
                                    44824251, 45599830, 44835944, 44819692, 44827794, 44826642, 44835943, 44823110, 44831248, 44819695, 44824250, 35207669, 44827778, 44831250, 44824235, 44827795, 44835924, 45586587, 44832381, 1326603, 44833557, 44823108, 45591469, 1326602, 45596188, 45601038, 1326604, 44830086, 1326609, 44819696, 45609394, 44831232, 1569180, 45543164, 44820870, 45567181, 45586654, 45557612, 44833560, 35207793, 35207674, 44823119, 45548022, 45543182, 35207670, 44819694, 44820856, 45576878, 44827796, 45605777, 1326606, 45567180, 45562355, 1326605, 45533456, 44834732, 44833573, 35207673, 44831230, 44821950, 1326601, 1326608, 35207792, 1569179, 1326607, 45586588, 44820869, 44832369, 44828970, 44819693, 44831249, 45533457, 1569178, 44827779
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

dataset_45532666_condition_df = pandas.read_gbq(
    dataset_45532666_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_45532666_condition_df.to_csv('../patient_disease/heartFailure_condition_df.csv', index=False)





# This query represents dataset "hyperglycaemia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_93276959_person_sql = """
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

dataset_93276959_person_df = pandas.read_gbq(
    dataset_93276959_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_93276959_person_df.to_csv('../patient_disease/hyperglycaemia_person_df.csv', index=False)




# This query represents dataset "hyperglycaemia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_93276959_condition_sql = """
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
                condition_concept_id IN  (
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
                                    40480068, 4029421, 4214376, 37016348, 4129517, 4295363, 4029420, 37016349, 37311673, 45769462, 4016046
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
                            is_standard = 1 
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

dataset_93276959_condition_df = pandas.read_gbq(
    dataset_93276959_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_93276959_condition_df.to_csv('../patient_disease/hyperglycaemia_condition_df.csv', index=False)




# This query represents dataset "hypertension" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_12408553_person_sql = """
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

dataset_12408553_person_df = pandas.read_gbq(
    dataset_12408553_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12408553_person_df.to_csv('../patient_disease/hypertension_person_df.csv', index=False)




# This query represents dataset "hypertension" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_12408553_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    35211268, 44829313
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_12408553_observation_df = pandas.read_gbq(
    dataset_12408553_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12408553_observation_df.to_csv('../patient_disease/hypertension_observation_df.csv', index=False)




# This query represents dataset "hypertension" for domain "measurement" and was generated for All of Us Controlled Tier Dataset v6
dataset_12408553_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_source_concept_id IN  (
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
                                    44832016
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
                    measurement.PERSON_ID IN (
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
                ) measurement 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
                    ON measurement.measurement_concept_id = m_standard_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
                    ON measurement.measurement_type_concept_id = m_type.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
                    ON measurement.operator_concept_id = m_operator.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
                    ON measurement.value_as_concept_id = m_value.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
                    ON measurement.unit_concept_id = m_unit.concept_id 
            LEFT JOIn
                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                    ON measurement.visit_occurrence_id = v.visit_occurrence_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
                    ON v.visit_concept_id = m_visit.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
                    ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_12408553_measurement_df = pandas.read_gbq(
    dataset_12408553_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12408553_measurement_df.to_csv('../patient_disease/hypertension_measurement_df.csv', index=False)




# This query represents dataset "hypertension" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_12408553_condition_sql = """
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
                                    45567885, 44836074, 45576740, 44824395, 37200975, 44822040, 37200964, 44819852, 45548704, 44825592, 44830210, 45553476, 44821020, 44822082, 45572765, 44831376, 44820879, 45596270, 44824391, 45558201, 35207677, 45596915, 45548702, 35207679, 44821019, 45576936, 44829108, 44832370, 35207675, 45567887, 44832523, 45548095, 44824390, 44826776, 44834878, 45601107, 37200963, 45548697, 44830211, 44825590, 44832522, 35208365, 45552868, 44824389, 1326593, 45591333, 45563050, 44821023, 45534171, 44832524, 44821021, 45534172, 35207709, 45596923, 45548096, 44827929, 44834876, 44836078, 37200962, 45534174, 44827815, 35207678, 44836076, 44827931, 45567252, 44830215, 37200976, 44830637, 44822084, 45563048, 44837235, 44823231, 37200961, 44836075, 37200960, 44819850, 44834879, 45539086, 44821949, 45601778, 45587281, 45592191, 45539090, 44834715, 44826775, 44823113, 45534170, 44830212, 44825589, 44827781, 44837234, 44833556, 45596918, 44824236, 45543246, 44823236, 45592187, 44819849, 44823230, 45558200, 45587280, 1326595, 44825456, 45567886, 37200958, 44827930, 44833447, 45582447, 45605875, 44834877, 44832371, 35207668, 44825591, 45567254, 45558203, 45539087, 1571659, 45533530, 44831381, 44833713, 44827935, 44827941, 44837238, 45548699, 35207707, 45596916, 45553475, 37200966, 44827934, 45567888, 45596920, 45587282, 44824394, 44834763, 35207676, 45592188, 37200959, 44824393, 45582445, 45548703, 45577559, 44835925, 44833714, 1326594, 45563051, 45548707, 1571652, 45582446, 45533529, 45539089, 45563047, 45596271, 45576935, 45552869, 37200957, 45534173, 45557414, 45543245, 1326773, 44837233, 45596917, 44823109, 45601108, 45577558, 44829109, 45543044, 35207504, 45592185, 44827933, 44823229, 44824392, 44827932, 45596919, 44837038, 45582442, 45539085, 44819858, 45567253, 44837098, 45581837, 44819851, 1326597, 44833715, 35210518, 44831234, 45606537, 45548701, 44822083, 1326596, 44836077, 44833595, 45562423, 45592183, 45605870, 45563052, 45543913, 44830078, 44826774, 44837126, 37200965, 1326592
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

dataset_12408553_condition_df = pandas.read_gbq(
    dataset_12408553_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12408553_condition_df.to_csv('../patient_disease/hypertension_condition_df.csv', index=False)




# This query represents dataset "hyperthyroidism" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_73638323_person_sql = """
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

dataset_73638323_person_df = pandas.read_gbq(
    dataset_73638323_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_73638323_person_df.to_csv('../patient_disease/hyperthyroidism_person_df.csv', index=False)


#

# This query represents dataset "hyperthyroidism" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_73638323_condition_sql = """
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
                                    45581340, 45576428, 45595788, 1567901, 1567895, 1567896, 1567902, 35210623, 45576427, 45537949, 45600630, 45600631, 45581339, 45537951, 45557103, 45547615, 45605390, 45595787, 45542725
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

dataset_73638323_condition_df = pandas.read_gbq(
    dataset_73638323_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_73638323_condition_df.to_csv('../patient_disease/hyperthyroidism_condition_df.csv', index=False)



# This query represents dataset "hypothyroidism" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_78399502_person_sql = """
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

dataset_78399502_person_df = pandas.read_gbq(
    dataset_78399502_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_78399502_person_df.to_csv('../patient_disease/hypothyroidism_person_df.csv', index=False)




# This query represents dataset "hypothyroidism" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_78399502_condition_sql = """
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
                                    35206851, 44819497, 35206858, 44828786, 44821784, 44825259, 35206854, 35206853, 44826456, 44827613, 1567893, 35206855, 35206856, 35206859, 44829873, 35206857, 44829874, 35207108, 35206852
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

dataset_78399502_condition_df = pandas.read_gbq(
    dataset_78399502_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_78399502_condition_df.to_csv('../patient_disease/hypothyroidism_condition_df.csv', index=False)




# This query represents dataset "ischaemia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_43581946_person_sql = """
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

dataset_43581946_person_df = pandas.read_gbq(
    dataset_43581946_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_43581946_person_df.to_csv('../patient_disease/ischaemia_person_df.csv', index=False)




# This query represents dataset "ischaemia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_43581946_condition_sql = """
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
                                    44819716, 37200618, 45575528, 35207705, 45581787, 45567050, 44819715, 44820875, 35209297, 45551451, 35210661, 44834738, 37200628, 37200619, 44832387, 37200627, 37200636, 37200620, 45585211, 37200634, 37200626, 44827801, 44832306, 44832386
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

dataset_43581946_condition_df = pandas.read_gbq(
    dataset_43581946_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_43581946_condition_df.to_csv('../patient_disease/ischaemia_condition_df.csv', index=False)




# This query represents dataset "kidney disease" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_22396978_person_sql = """
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

dataset_22396978_person_df = pandas.read_gbq(
    dataset_22396978_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_22396978_person_df.to_csv('../patient_disease/kidneyDisease_person_df.csv', index=False)




# This query represents dataset "kidney disease" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_22396978_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    44827332, 44835439, 44836623
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_22396978_observation_df = pandas.read_gbq(
    dataset_22396978_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_22396978_observation_df.to_csv('../patient_disease/kidneyDisease_observation_df.csv', index=False)




# This query represents dataset "kidney disease" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_22396978_condition_sql = """
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
                                    44826634, 44835056, 45577738, 45563255, 44821950, 45567886, 45561945, 44830172, 35211024, 725442, 45561955, 44835923, 35211021, 44836035, 35211023, 44822258, 44824574, 45552372, 44830402, 45548653, 44837191, 35209279, 44824235, 44833558, 44831231, 44819692, 35209277, 44828971, 35209278, 35209300, 45596188, 44832368, 45539086, 45539087, 44819695, 35209275, 1571486, 44825427, 44834717, 45534172, 35209276, 1572108, 35211020, 44826939, 44835924, 44835922, 35207673, 44819696, 45587467, 44830173, 44832687, 45587280, 44820970, 44825426, 44819694, 45534171, 44828106, 44837192, 35207674, 44821203, 35211022, 44820856, 44827888, 44832369, 44831068, 45606750, 44831232, 44830077, 35209274, 45543164, 44821204, 35211019, 45587281, 44819693, 45553686, 45557089, 725441, 45548697, 44833560, 44833559, 35207672, 35207671, 725440, 44827889, 45595797, 45558201, 45547621, 44820018
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

dataset_22396978_condition_df = pandas.read_gbq(
    dataset_22396978_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_22396978_condition_df.to_csv('../patient_disease/kidneyDisease_condition_df.csv', index=False)




# This query represents dataset "memory loss" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_19636646_person_sql = """
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

dataset_19636646_person_df = pandas.read_gbq(
    dataset_19636646_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_19636646_person_df.to_csv('../patient_disease/memoryLoss_person_df.csv', index=False)




# This query represents dataset "memory loss" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_19636646_condition_sql = """
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
                                    44832709
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

dataset_19636646_condition_df = pandas.read_gbq(
    dataset_19636646_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_19636646_condition_df.to_csv('../patient_disease/memoryLoss_condition_df.csv', index=False)



# This query represents dataset "obesity" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_12867469_person_sql = """
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

dataset_12867469_person_df = pandas.read_gbq(
    dataset_12867469_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12867469_person_df.to_csv('../patient_disease/obesity_person_df.csv', index=False)




# This query represents dataset "obesity" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_12867469_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    44834569, 45586164, 44821800, 44824092
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_12867469_observation_df = pandas.read_gbq(
    dataset_12867469_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12867469_observation_df.to_csv('../patient_disease/obesity_observation_df.csv', index=False)




# This query represents dataset "obesity" for domain "measurement" and was generated for All of Us Controlled Tier Dataset v6
dataset_12867469_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_source_concept_id IN  (
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
                                    44828608
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
                    measurement.PERSON_ID IN (
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
                ) measurement 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
                    ON measurement.measurement_concept_id = m_standard_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
                    ON measurement.measurement_type_concept_id = m_type.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
                    ON measurement.operator_concept_id = m_operator.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
                    ON measurement.value_as_concept_id = m_value.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
                    ON measurement.unit_concept_id = m_unit.concept_id 
            LEFT JOIn
                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                    ON measurement.visit_occurrence_id = v.visit_occurrence_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
                    ON v.visit_concept_id = m_visit.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
                    ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_12867469_measurement_df = pandas.read_gbq(
    dataset_12867469_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12867469_measurement_df.to_csv('../patient_disease/obesity_measurement_df.csv', index=False)



# This query represents dataset "obesity" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_12867469_condition_sql = """
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
                                    45591051, 45577673, 45568006, 44827635, 44824093, 44834569, 44836090, 44834896, 1568023, 44822107, 44833387, 44819525, 1568022, 44831059, 44836089, 44826786, 35207022, 45600659, 44821032, 44824092, 44819524, 35207024, 45553618, 44822953, 35207021, 35207023, 45606671, 45606672, 44829903, 45534322
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

dataset_12867469_condition_df = pandas.read_gbq(
    dataset_12867469_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_12867469_condition_df.to_csv('../patient_disease/obesity_condition_df.csv', index=False)



# This query represents dataset "Parkinson's disease" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_94721788_person_sql = """
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

dataset_94721788_person_df = pandas.read_gbq(
    dataset_94721788_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_94721788_person_df.to_csv('../patient_disease/ParkinsonsDisease_person_df.csv', index=False)




# This query represents dataset "Parkinson's disease" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_94721788_condition_sql = """
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
                                    44825328, 44819574, 44826540, 35207329
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

dataset_94721788_condition_df = pandas.read_gbq(
    dataset_94721788_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_94721788_condition_df.to_csv('../patient_disease/ParkinsonsDisease_condition_df.csv', index=False)




# This query represents dataset "parkinsonism" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_56200147_person_sql = """
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

dataset_56200147_person_df = pandas.read_gbq(
    dataset_56200147_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56200147_person_df.to_csv('../patient_disease/parkinsonism_person_df.csv', index=False)




# This query represents dataset "parkinsonism" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_56200147_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    44830702, 44836566, 44830701
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_56200147_observation_df = pandas.read_gbq(
    dataset_56200147_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56200147_observation_df.to_csv('../patient_disease/parkinsonism_observation_df.csv', index=False)



# This query represents dataset "parkinsonism" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_56200147_condition_sql = """
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
                                    35207333, 35207331, 44823629, 44828328, 45566862, 35207330, 35207334, 44828400, 44829457, 44837617, 35207332, 45581482, 35207335, 44825328
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

dataset_56200147_condition_df = pandas.read_gbq(
    dataset_56200147_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_56200147_condition_df.to_csv('../patient_disease/parkinsonism_condition_df.csv', index=False)




# This query represents dataset "seizures" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_78726741_person_sql = """
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

dataset_78726741_person_df = pandas.read_gbq(
    dataset_78726741_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_78726741_person_df.to_csv('../patient_disease/seizures_person_df.csv', index=False)




# This query represents dataset "seizures" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_78726741_condition_sql = """
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
                                    45595937, 44820766, 45600781, 45586283, 44824160, 45605538, 44819586, 44819587, 45538107, 45586287, 45566870, 45581489, 44828876, 45547731, 44825337, 44820054, 45581490, 44833444, 44826555, 45605537, 45605540, 45600782, 45538105, 44837012, 45557234, 45542884, 45581487, 1568329, 45557228, 45591173, 45542881, 45600778, 45600779, 45576574, 44832279, 45557229, 44823026, 45586286, 44829971, 45557233, 45552524, 45571799, 45595938, 45576573, 45591171, 44828877, 45595936, 44834634, 35207182, 44820767, 44829972, 44826554, 1568300, 45576572, 44825338, 44823025, 45605542, 45577800, 45605539, 45571801, 45571800, 44825339, 45591175, 45605541, 45542882, 1568327, 44826556, 44831129, 45542885, 45571798, 44835838, 45566871, 44834635, 45533135, 44835840, 45566872, 45600783, 45557230
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

dataset_78726741_condition_df = pandas.read_gbq(
    dataset_78726741_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_78726741_condition_df.to_csv('../patient_disease/seizures_condition_df.csv', index=False)




# This query represents dataset "sleep disorder" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_18556183_person_sql = """
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

dataset_18556183_person_df = pandas.read_gbq(
    dataset_18556183_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_18556183_person_df.to_csv('../patient_disease/sleepDisorder_person_df.csv', index=False)




# This query represents dataset "sleep disorder" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_18556183_condition_sql = """
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
                                    45595947, 45581498, 44823005, 45538123, 45605559, 45542893, 45566847, 45533147, 44835808, 44819568, 45566884, 45552537, 45562054, 44832261, 1568364, 44820747, 44825325, 44824135, 35207199, 45581497, 45581499, 44832260, 44824149, 45538122, 45533149, 45538121, 45542798, 44827685, 35207198, 1568366, 44822992, 45605502, 44834625, 45542812, 45566848, 44827687, 45576592, 45595875, 44824150, 44824151, 45600793, 44835775, 44825324, 35207410, 45547743, 45533148, 45576591, 44831119, 44825326, 35207409, 45586297, 44824134, 45542807, 35207200, 45571812, 44826535, 44828845, 44826522, 44827643, 44832264, 44826532, 44835822, 44825323, 44827669, 44832262, 44819572, 44831110, 44819570, 45538006, 45595911, 44826533, 45538071, 44836999, 45591186, 44823007, 44819569, 1568363, 44825322, 45566886, 45552539, 45542834, 45562005, 45566849, 44825311, 45576593, 1568242, 44834626, 44835823, 45571766, 45562055, 44827686, 45581500, 44826969, 45571714, 45533067, 44826534, 44826531, 45562096, 44831120, 44828859, 45552538, 44828857, 44820746, 45562095, 35207196, 44836998, 45557240, 44835821, 1568362, 45566885, 45571811, 45571810, 45586299, 45600753, 45605556, 45552442, 45605558, 45576511, 44835809, 44832263, 45605560, 45600696, 45586298, 35207197, 45586205, 45576590, 44835824, 45566887, 45591185, 45547744, 45586214, 44823006, 45562093, 44828858, 44836997, 44829958, 44820736, 45605483
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

dataset_18556183_condition_df = pandas.read_gbq(
    dataset_18556183_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_18556183_condition_df.to_csv('../patient_disease/sleepDisorder_condition_df.csv', index=False)




# This query represents dataset "vascular disease" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_66097269_person_sql = """
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

dataset_66097269_person_df = pandas.read_gbq(
    dataset_66097269_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_66097269_person_df.to_csv('../patient_disease/vascularDisease_person_df.csv', index=False)




# This query represents dataset "vascular disease" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_66097269_condition_sql = """
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
                                    35207405, 44835959, 35207859, 35207403, 725216, 45567223, 44821962, 45538443, 44835958, 35207404, 44830094, 45581814, 35207400, 44825451, 44826655, 45538444, 35207860, 1569325, 44826654, 44831258, 35207402, 35207407, 35207408, 44828994, 44827805, 35207401, 44831139, 44833583, 44819724, 35207406
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

dataset_66097269_condition_df = pandas.read_gbq(
    dataset_66097269_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_66097269_condition_df.to_csv('../patient_disease/vascularDisease_condition_df.csv', index=False)




# This query represents dataset "fractures" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_06729897_person_sql = """
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

dataset_06729897_person_df = pandas.read_gbq(
    dataset_06729897_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_06729897_person_df.to_csv('../patient_disease/fractures_person_df.csv', index=False)




# This query represents dataset "fractures" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_06729897_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    44820099
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_06729897_observation_df = pandas.read_gbq(
    dataset_06729897_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_06729897_observation_df.to_csv('../patient_disease/fractures_observation_df.csv', index=False)




# This query represents dataset "fractures" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_06729897_condition_sql = """
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
                                    44822335, 45584351, 45574704, 45597582, 44829347, 44829351, 45584370, 45608483, 45540957, 45587583, 45608469, 44835155, 45593223, 45560104, 45563568, 45589180, 44820099, 1553919, 1553907, 45594060, 45598905, 45554573, 45574727, 45602830, 45555416, 45560090, 45536150, 45565004, 44820139, 45597446, 45593226, 45535312, 45536147, 45579493, 44832745, 45550572, 45554150, 45565015, 45608464, 45597448, 45608459, 45555419, 45574692, 44822333, 44820105, 45598903, 44820114, 45536140, 45574709, 45587758, 45593224, 45584343, 45589200, 1553871, 45536165, 44824679, 45573287, 45574697, 44829344, 45589212, 45540954, 45607074, 45549240, 37201410, 45560117, 45540955, 45550596, 44836348, 44824686, 45579504, 45594065, 45584339, 45608494, 45555422, 45579497, 45603730, 45569826, 45554149, 45539609, 45540948, 45584342, 44823512, 45584368, 45545811, 45560112, 45560118, 45545818, 45563565, 45594049, 45545809, 45550575, 44823510, 45555421, 45555399, 45540950, 1553893, 45573288, 44825845, 45540977, 45550594, 1553880, 45540951, 45554151, 45560100, 45560089, 45579505, 45536163, 45584365, 45589184, 45582960, 45550574, 1553915, 45540973, 45589199, 44823520, 45536138, 45564999, 44829346, 44831659, 45569831, 44823515, 45608465, 45545808, 45587894, 37201412, 37201424, 45579511, 44831662, 44827013, 44822347, 44821287, 44824666, 45608500, 44827038, 44822352, 45589213, 45589182, 44823491, 1553900, 45584371, 45606882, 44833946, 45579503, 45536154, 45598886, 1553883, 45540148, 45594067, 44824673, 45584338, 45563567, 45597447, 44828203, 44823509, 45555418, 44820130, 45536173, 44822334, 44833980, 44833983, 44832782, 45589203, 44836349, 45574696, 45598029, 45565013, 45568402, 45589202, 45598883, 45545826, 44831639, 45603715, 45550598, 44828216, 44824685, 44821291, 44830496, 45608466, 45608476, 45608470, 11211, 45597583, 45569842, 45554148, 45573865, 45584345, 44820102, 45594064, 44832767, 44835128, 45583119, 45607644, 1553885, 45550600, 45589197, 44835154, 37201422, 45574703, 45579506, 45589191, 44836350, 45594051, 45582961, 45608462, 1553916, 45560093, 45573289, 44821278, 44821292, 37201407, 45598888, 15501, 45582785, 45569850, 45560110, 45550592, 45589204, 37201409, 45555392, 45536164, 45574719, 45544563, 44827003, 45539740, 45574701, 45584349, 45579502, 45550573, 45598904, 45560111, 45569847, 37201405, 45555410, 1553873, 45536145, 45607642, 44835158, 45603729, 45564129, 44831657, 45594061, 45584346, 45544977, 45589192, 45560092, 45579495, 45574728, 45578210, 1553879, 45574695, 45597584, 45579513, 44832756, 45558736, 37201414, 45555389, 1553914, 45550595, 44835153, 45564997, 37201417, 1553918, 45540974, 45569846, 45555394, 45540971, 45540976, 45582959, 45584369, 45550597, 44833950, 1553895, 45545823, 45569833, 45579486, 44836327, 45564984, 45574724, 45579490, 45589211, 44825846, 44822372, 45574702, 45598885, 45555414, 45607075, 44837473, 44831646, 45560091, 45534861, 45573869, 1553917, 44821290, 1553887, 45536151, 45573868, 45565011, 45603720, 45549242, 45584347, 45589181, 45555420, 45579485, 37201423, 45534862, 45536152, 45553817, 45568534, 45560101, 45569827, 37201413, 45598890, 45545827, 45544564, 37201420, 37201419, 1553878, 44822353, 45549241, 45569830, 45592683, 45540149, 45584372, 45574723, 44827028, 45558879, 44821300, 45535313, 44832770, 45564989, 45536156, 45550581, 45540975, 45584360, 45598892, 45560106, 45540952, 44820101, 37201421, 45554576, 45589179, 37201406, 1553894, 44825843, 45568533, 45563566, 45550571, 45550589, 45569849, 45608482
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

dataset_06729897_condition_df = pandas.read_gbq(
    dataset_06729897_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_06729897_condition_df.to_csv('../patient_disease/fractures_condition_df.csv', index=False)


# This query represents dataset "elevated blood pressure" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_27024490_person_sql = """
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

dataset_27024490_person_df = pandas.read_gbq(
    dataset_27024490_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_27024490_person_df.to_csv('../patient_disease/elevatedBloodPressure_person_df.csv', index=False)



# This query represents dataset "elevated blood pressure" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_27024490_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    35211268, 44829313
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_27024490_observation_df = pandas.read_gbq(
    dataset_27024490_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_27024490_observation_df.to_csv('../patient_disease/elevatedBloodPressure_observation_df.csv', index=False)






# This query represents dataset "dysphagia" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_32624798_person_sql = """
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

dataset_32624798_person_df = pandas.read_gbq(
    dataset_32624798_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_32624798_person_df.to_csv('../patient_disease/dysphagia_person_df.csv', index=False)




# This query represents dataset "dysphagia" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_32624798_condition_sql = """
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
                                    45592409, 45538416, 1572204, 45572109, 35206693, 45543197, 44832718, 44822300, 45602010, 45568116, 45601058, 44819717, 45557564, 45563292, 45563291, 44828158, 44831592, 44821251, 45539319, 45581797, 45534431, 44829301, 44826973
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

dataset_32624798_condition_df = pandas.read_gbq(
    dataset_32624798_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_32624798_condition_df.to_csv('../patient_disease/dysphagia_condition_df.csv', index=False)



# This query represents dataset "dysphagia" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_32624798_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    45577784
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_32624798_observation_df = pandas.read_gbq(
    dataset_32624798_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_32624798_observation_df.to_csv('../patient_disease/dysphagia_observation_df.csv', index=False)



# This query represents dataset "depression" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_95857109_person_sql = """
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

dataset_95857109_person_df = pandas.read_gbq(
    dataset_95857109_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_95857109_person_df.to_csv('../patient_disease/depression_person_df.csv', index=False)



# This query represents dataset "depression" for domain "observation" and was generated for All of Us Controlled Tier Dataset v6
dataset_95857109_observation_sql = """
    SELECT
        observation.person_id,
        observation.observation_concept_id,
        o_standard_concept.concept_name as standard_concept_name,
        o_standard_concept.concept_code as standard_concept_code,
        o_standard_concept.vocabulary_id as standard_vocabulary,
        observation.observation_datetime,
        observation.observation_type_concept_id,
        o_type.concept_name as observation_type_concept_name,
        observation.value_as_number,
        observation.value_as_string,
        observation.value_as_concept_id,
        o_value.concept_name as value_as_concept_name,
        observation.qualifier_concept_id,
        o_qualifier.concept_name as qualifier_concept_name,
        observation.unit_concept_id,
        o_unit.concept_name as unit_concept_name,
        observation.visit_occurrence_id,
        o_visit.concept_name as visit_occurrence_concept_name,
        observation.observation_source_value,
        observation.observation_source_concept_id,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.vocabulary_id as source_vocabulary,
        observation.unit_source_value,
        observation.qualifier_source_value,
        observation.value_source_concept_id,
        observation.value_source_value,
        observation.questionnaire_response_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.observation` observation 
        WHERE
            (
                observation_source_concept_id IN (
                    1595786, 1595785
                )
            )  
            AND (
                observation.PERSON_ID IN (
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
            ) observation 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
                ON observation.observation_concept_id = o_standard_concept.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_type 
                ON observation.observation_type_concept_id = o_type.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
                ON observation.value_as_concept_id = o_value.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_qualifier 
                ON observation.qualifier_concept_id = o_qualifier.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_unit 
                ON observation.unit_concept_id = o_unit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                ON observation.visit_occurrence_id = v.visit_occurrence_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_visit 
                ON v.visit_concept_id = o_visit.concept_id 
        LEFT JOIN
            `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
                ON observation.observation_source_concept_id = o_source_concept.concept_id"""

dataset_95857109_observation_df = pandas.read_gbq(
    dataset_95857109_observation_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_95857109_observation_df.to_csv('../patient_disease/depression_observation_df.csv', index=False)



# This query represents dataset "depression" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_95857109_condition_sql = """
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
                                    766349, 44832706, 1595539, 44836954
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

dataset_95857109_condition_df = pandas.read_gbq(
    dataset_95857109_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_95857109_condition_df.to_csv('../patient_disease/depression_condition_df.csv', index=False)



# This query represents dataset "depression" for domain "measurement" and was generated for All of Us Controlled Tier Dataset v6
dataset_95857109_measurement_sql = """
    SELECT
        measurement.person_id,
        measurement.measurement_concept_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        m_standard_concept.vocabulary_id as standard_vocabulary,
        measurement.measurement_datetime,
        measurement.measurement_type_concept_id,
        m_type.concept_name as measurement_type_concept_name,
        measurement.operator_concept_id,
        m_operator.concept_name as operator_concept_name,
        measurement.value_as_number,
        measurement.value_as_concept_id,
        m_value.concept_name as value_as_concept_name,
        measurement.unit_concept_id,
        m_unit.concept_name as unit_concept_name,
        measurement.range_low,
        measurement.range_high,
        measurement.visit_occurrence_id,
        m_visit.concept_name as visit_occurrence_concept_name,
        measurement.measurement_source_value,
        measurement.measurement_source_concept_id,
        m_source_concept.concept_name as source_concept_name,
        m_source_concept.concept_code as source_concept_code,
        m_source_concept.vocabulary_id as source_vocabulary,
        measurement.unit_source_value,
        measurement.value_source_value 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_source_concept_id IN  (
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
                                    44830862
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
                    measurement.PERSON_ID IN (
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
                ) measurement 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
                    ON measurement.measurement_concept_id = m_standard_concept.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
                    ON measurement.measurement_type_concept_id = m_type.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
                    ON measurement.operator_concept_id = m_operator.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
                    ON measurement.value_as_concept_id = m_value.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
                    ON measurement.unit_concept_id = m_unit.concept_id 
            LEFT JOIn
                `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
                    ON measurement.visit_occurrence_id = v.visit_occurrence_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_visit 
                    ON v.visit_concept_id = m_visit.concept_id 
            LEFT JOIN
                `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
                    ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_95857109_measurement_df = pandas.read_gbq(
    dataset_95857109_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_95857109_measurement_df.to_csv('../patient_disease/depression_measurement_df.csv', index=False)



# This query represents dataset "Atherosclerosis" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_94997024_person_sql = """
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

dataset_94997024_person_df = pandas.read_gbq(
    dataset_94997024_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_94997024_person_df.to_csv('../patient_disease/atherosclerosis_person_df.csv', index=False)


# This query represents dataset "Atherosclerosis" for domain "condition" and was generated for All of Us Controlled Tier Dataset v6
dataset_94997024_condition_sql = """
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
                                    44828991, 45605828, 44831239, 45596233, 45552826, 45591496, 45591495, 44830080, 45601066, 45538422, 45548058, 45605787, 45605842, 45567207, 45562387, 45562343, 45605785, 45581807, 45567210, 44833579, 45548057, 44824257, 45552835, 45548012, 45538438, 45538374, 45538426, 44825447, 45572116, 45591458, 45596247, 44828975, 45605829, 44823125, 45591501, 45567208, 45567209, 45572124, 45586574, 45538431, 45591498, 35207830, 45552823, 45591503, 45538427, 45567212, 45557569, 44819703, 45562393, 45596244, 45591499, 45533483, 45581766, 45557539, 45581810, 45576904, 45605784, 45557568, 45548059, 44821951, 35207841, 45591494, 45548060, 45552831, 45605839, 45548056, 45557572, 44824238, 45552839, 45601075, 44823124, 45552830, 45562386, 45548070, 44831257, 45601080, 45552825, 45586623, 45581801, 45596232, 45576903, 45576908, 45557578, 44834739, 45586617, 45596236, 45596235, 45576868, 45601071, 45548010, 45601028, 45596239, 45562396, 45548076, 44835957, 45586573, 45586619, 45605831, 45601069, 45543209, 45581799, 45562388, 45548011, 45601026, 44837116, 44828992, 45562399, 45562344, 44834726, 45581808, 45605833, 45538425, 45557571, 44825448, 45538375, 45591500, 44833563, 45605830, 44819719, 44831256, 45548013, 45557588, 45596234, 45576906, 45533484, 45591459, 44819718, 44835932, 45576909, 45605832, 45591497, 45572119, 45576905, 45591460, 45538435, 45567168, 45601072, 45576907, 45601065, 45538377, 45557579, 45533491, 45605838, 45591504, 45557585, 45601027, 45562395, 45557575, 44827804, 45581798, 45557570, 45596240, 45543168, 45605840, 45552827, 45601070, 45586615, 44825446, 44835931, 45581800, 45605834, 45548061, 1569271, 45543167, 45567216, 45552824, 45533490, 45567214, 45533488, 45572121, 45596198, 45605786, 45552832, 44826651, 45572117, 35207842, 45567211, 44821952, 45601073, 45581809, 45586575, 45567167, 35207843, 45591461, 45562385, 45557581, 45552833, 45586614, 45533439, 45557576
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

dataset_94997024_condition_df = pandas.read_gbq(
    dataset_94997024_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_94997024_condition_df.to_csv('../patient_disease/atherosclerosis_condition_df.csv', index=False)



# This query represents dataset "Osteoporosis" for domain "person" and was generated for All of Us Controlled Tier Dataset v6
dataset_70729898_person_sql = """
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

dataset_70729898_person_df = pandas.read_gbq(
    dataset_70729898_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_70729898_person_df.to_csv('../patient_disease/osteoporosis_person_df.csv', index=False)

