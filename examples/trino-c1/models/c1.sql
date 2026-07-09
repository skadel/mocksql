WITH TMP_MR AS (
  SELECT DISTINCT
    ID_IMMATRICULATION AS NO_SIRET
  FROM monetique_dataset_mr.ds_mr_dashboard_reseau
), BANQUES AS (
  SELECT
    *
    EXCEPT (groupe),
    CASE
      WHEN groupe IN (
        'Banque Populaire 2',
        'Caisse d''Epargne2',
        'BPCE',
        'Caisse d''Epargne',
        'Banque Populaire'
      )
      THEN 'BPCE'
      ELSE groupe
    END AS groupe
  FROM marketing_referentiels.banques_france
  WHERE
    partition_date = (
      SELECT
        MAX(partition_date)
      FROM marketing_referentiels.banques_france
      WHERE
        partition_date <= CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
), RESEAU AS (
  SELECT DISTINCT
    code_banque,
    reseau
  FROM marketing_referentiels.banques
  WHERE
    reseau IN ('BP', 'CE')
    AND partition_date = (
      SELECT
        MAX(partition_date)
      FROM marketing_referentiels.banques_france
      WHERE
        partition_date <= CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
), COFACE AS (
  SELECT
    *
  FROM marketing_gr_source_ref_bpce.coface
  WHERE
    partition_date = (
      SELECT
        MAX(partition_date)
      FROM marketing_gr_source_ref_bpce.coface
      WHERE
        partition_date <= CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
), NAF AS (
  SELECT
    *
  FROM marketing_gr_source_ref_bpce.naf2
  WHERE
    partition_date = (
      SELECT
        MAX(partition_date)
      FROM marketing_gr_source_ref_bpce.naf2
      WHERE
        partition_date <= CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
), CATEG_JURIDIQUE AS (
  SELECT
    *
  FROM marketing_gr_source_ref_bpce.categories_juridiques
  WHERE
    partition_date = (
      SELECT
        MAX(partition_date)
      FROM marketing_gr_source_ref_bpce.categories_juridiques
      WHERE
        partition_date <= CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
), MCC AS (
  SELECT
    code_professionnel_mcc,
    domaine
  FROM marketing_gr_source_ref_bpce.code_mcc
  WHERE
    partition_date = (
      SELECT
        MAX(partition_date)
      FROM marketing_gr_source_ref_bpce.code_mcc
      WHERE
        partition_date <= CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
), RCOMP AS (
  SELECT
    DT_EXTRACTION,
    DT_TRANSACTION,
    CD_NATURE_OPERATION,
    CD_CYCLE_OPERATION,
    CD_PAYS_COMMERCANT,
    CD_BANQUE_EMETTEUR,
    CD_BANQUE_ACQUEREUR_CALCULE,
    NO_SIRET,
    CD_ERT,
    CD_MCC,
    MT_BRUT_TRANSACTION
  FROM monetique_dataset_porteur.ds_rcomp_dashboard_reseau
  WHERE
    DATE(DT_EXTRACTION) >= DATE_ADD(
      'MONTH',
      CAST('12' AS BIGINT) * -1,
      CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
    AND DATE(DT_TRANSACTION) < CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    AND DATE(DT_TRANSACTION) >= DATE_ADD(
      'MONTH',
      CAST('12' AS BIGINT) * -1,
      CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE)
    )
    AND DATE(DT_EXTRACTION) < DATE_ADD('MONTH', CAST('1' AS BIGINT), CAST(DATE_PARSE('01-01-2026', '%d-%m-%Y') AS DATE))
    AND CD_NATURE_OPERATION IN ('D', 'Q')
    AND CD_CYCLE_OPERATION = 'I'
    AND CD_PAYS_COMMERCANT IN ('250', '258', '540', '666', '175', '474', '638', '312', '254')
), COUNT_SIRET_GROUPE_BANQUE AS (
  SELECT
    NO_SIRET,
    CASE WHEN groupe IS NULL THEN 'INCONNU' ELSE groupe END AS groupe,
    LISTAGG(DISTINCT CD_BANQUE_ACQUEREUR_CALCULE, ', ') WITHIN GROUP (ORDER BY
      CD_BANQUE_ACQUEREUR_CALCULE NULLS FIRST) AS "liste_des_Banques_reseau",
    COUNT(*) AS nb_trs,
    SUM(MT_BRUT_TRANSACTION) AS mt_total
  FROM RCOMP AS rcomp
  LEFT JOIN BANQUES AS banques
    ON rcomp.CD_BANQUE_ACQUEREUR_CALCULE = banques.code_banque
  GROUP BY
    1,
    2
), PROP_SIRET_BANQUE AS (
  SELECT
    NO_SIRET,
    groupe,
    nb_trs,
    "liste_des_Banques_reseau",
    ROUND(CAST(nb_trs AS DOUBLE) / SUM(nb_trs) OVER (PARTITION BY NO_SIRET) * 100, 2) AS proportion_nb,
    ROUND(CAST(mt_total AS DOUBLE) / SUM(mt_total) OVER (PARTITION BY NO_SIRET) * 100, 2) AS proportion_ca
  FROM COUNT_SIRET_GROUPE_BANQUE
  ORDER BY
    NO_SIRET DESC
), SIRET_ONUS AS (
  SELECT DISTINCT
    NO_SIRET
  FROM PROP_SIRET_BANQUE
  WHERE
    (
      groupe IN (
        'Banque Populaire 2',
        'Caisse d''Epargne2',
        'BPCE',
        'Caisse d''Epargne',
        'Banque Populaire'
      )
    )
    AND proportion_ca >= 50
), LISTE_GRP_PART AS (
  SELECT
    NO_SIRET,
    LISTAGG(DISTINCT "liste_des_Banques_reseau", ', ') WITHIN GROUP (ORDER BY
      "liste_des_Banques_reseau" NULLS FIRST) AS "liste_des_Banques_reseau",
    LISTAGG(
      
        CONCAT(
          CAST(groupe AS VARCHAR),
          CAST(' (' AS VARCHAR),
          CAST(proportion_nb AS VARCHAR),
          CAST('%)' AS VARCHAR)
        ),
        ', '
      
    ) WITHIN GROUP (ORDER BY
      groupe ASC NULLS FIRST) AS proportion_nb,
    LISTAGG(
      
        CONCAT(
          CAST(groupe AS VARCHAR),
          CAST(' (' AS VARCHAR),
          CAST(proportion_ca AS VARCHAR),
          CAST('%)' AS VARCHAR)
        ),
        ', '
      
    ) WITHIN GROUP (ORDER BY
      groupe ASC NULLS FIRST) AS proportion_ca,
    MAX(proportion_ca) AS part_groupe_principal,
    COUNT(DISTINCT groupe) AS "nombre_de_groupe_bancaire_acquereur"
  FROM PROP_SIRET_BANQUE
  GROUP BY
    1
), COUNT_SIRET_CANAL_RESEAU AS (
  SELECT
    NO_SIRET,
    CASE
      WHEN CD_ERT IN ('00', '10')
      THEN 'proxi'
      WHEN CD_ERT IN ('24', '27', '64')
      THEN 'internet'
      WHEN CAST(CD_ERT AS BIGINT) BETWEEN 40 AND 59
      THEN 'automate'
      WHEN (
        CAST(CD_ERT AS BIGINT) BETWEEN 20 AND 30
      )
      AND (
        NOT CAST(cd_ert AS BIGINT) IN (24, 27)
      )
      THEN 'VAD'
      ELSE 'Autres'
    END AS canal,
    COUNT(*) AS nb_trs,
    SUM(MT_BRUT_TRANSACTION) AS mt_total
  FROM RCOMP AS rcomp
  LEFT JOIN RESEAU AS reseau
    ON rcomp.CD_BANQUE_ACQUEREUR_CALCULE = reseau.code_banque
  WHERE
    NOT reseau.reseau IS NULL
  GROUP BY
    1,
    2
), PROP_SIRET_CANAL_RESEAU AS (
  SELECT
    NO_SIRET,
    canal,
    nb_trs,
    ROUND(CAST(nb_trs AS DOUBLE) / SUM(nb_trs) OVER (PARTITION BY NO_SIRET) * 100, 2) AS proportion_nb,
    ROUND(CAST(mt_total AS DOUBLE) / SUM(mt_total) OVER (PARTITION BY NO_SIRET) * 100, 2) AS proportion_ca
  FROM COUNT_SIRET_CANAL_RESEAU
  ORDER BY
    NO_SIRET DESC
), LISTE_CANAL_PART_RESEAU AS (
  SELECT
    NO_SIRET,
    LISTAGG(
      
        CONCAT(
          CAST(canal AS VARCHAR),
          CAST(' (' AS VARCHAR),
          CAST(proportion_nb AS VARCHAR),
          CAST('%)' AS VARCHAR)
        ),
        ', '
      
    ) WITHIN GROUP (ORDER BY
      canal NULLS FIRST) AS proportion_nb,
    LISTAGG(
      
        CONCAT(
          CAST(canal AS VARCHAR),
          CAST(' (' AS VARCHAR),
          CAST(proportion_ca AS VARCHAR),
          CAST('%)' AS VARCHAR)
        ),
        ', '
      
    ) WITHIN GROUP (ORDER BY
      canal NULLS FIRST) AS proportion_ca
  FROM PROP_SIRET_CANAL_RESEAU
  GROUP BY
    1
), COUNT_SIRET_CANAL_HORSRESEAU AS (
  SELECT
    NO_SIRET,
    CASE
      WHEN CD_ERT IN ('00', '10')
      THEN 'proxi'
      WHEN CD_ERT IN ('24', '27', '64')
      THEN 'internet'
      WHEN CAST(CD_ERT AS BIGINT) BETWEEN 40 AND 59
      THEN 'automate'
      WHEN (
        CAST(CD_ERT AS BIGINT) BETWEEN 20 AND 30
      )
      AND (
        NOT CAST(cd_ert AS BIGINT) IN (24, 27)
      )
      THEN 'VAD'
      ELSE 'Autres'
    END AS canal,
    COUNT(*) AS nb_trs,
    SUM(MT_BRUT_TRANSACTION) AS mt_total
  FROM RCOMP AS rcomp
  GROUP BY
    1,
    2
), PROP_SIRET_CANAL_HORSRESEAU AS (
  SELECT
    NO_SIRET,
    canal,
    nb_trs,
    ROUND(CAST(nb_trs AS DOUBLE) / SUM(nb_trs) OVER (PARTITION BY NO_SIRET) * 100, 2) AS proportion_nb,
    ROUND(CAST(mt_total AS DOUBLE) / SUM(mt_total) OVER (PARTITION BY NO_SIRET) * 100, 2) AS proportion_ca
  FROM COUNT_SIRET_CANAL_HORSRESEAU
  ORDER BY
    NO_SIRET DESC
), LISTE_CANAL_PART_HORSRESEAU AS (
  SELECT
    NO_SIRET,
    LISTAGG(
      
        CONCAT(
          CAST(canal AS VARCHAR),
          CAST(' (' AS VARCHAR),
          CAST(proportion_nb AS VARCHAR),
          CAST('%)' AS VARCHAR)
        ),
        ', '
      
    ) WITHIN GROUP (ORDER BY
      canal NULLS FIRST) AS proportion_nb,
    LISTAGG(
      
        CONCAT(
          CAST(canal AS VARCHAR),
          CAST(' (' AS VARCHAR),
          CAST(proportion_ca AS VARCHAR),
          CAST('%)' AS VARCHAR)
        ),
        ', '
      
    ) WITHIN GROUP (ORDER BY
      canal NULLS FIRST) AS proportion_ca
  FROM PROP_SIRET_CANAL_HORSRESEAU
  GROUP BY
    1
), TERRIROIRE_PROSPECT AS (
  SELECT
    *
  FROM marketing_referentiels.territoire_prospect
), BANQUE_PDV_STEP_1 AS (
  SELECT
    NO_SIRET,
    CD_BANQUE_ACQUEREUR_CALCULE,
    libelle,
    groupe,
    SUM(MT_BRUT_TRANSACTION) AS MT_BRUT_TRANSACTION
  FROM RCOMP AS rcomp
  LEFT JOIN BANQUES AS banques
    ON rcomp.CD_BANQUE_ACQUEREUR_CALCULE = banques.code_banque
  WHERE
    NOT rcomp.NO_SIRET IS NULL
  GROUP BY
    1,
    2,
    3,
    4
), BANQUE_PDV_STEP_2 AS (
  SELECT
    NO_SIRET,
    CD_BANQUE_ACQUEREUR_CALCULE,
    LIBELLE,
    GROUPE,
    MT_BRUT_TRANSACTION,
    ROW_NUMBER() OVER (PARTITION BY no_siret ORDER BY MT_BRUT_TRANSACTION DESC) AS rn
  FROM BANQUE_PDV_STEP_1
), BANQUE_PDV_STEP_3 AS (
  SELECT
    NO_SIRET,
    CD_BANQUE_ACQUEREUR_CALCULE,
    LIBELLE,
    GROUPE
  FROM BANQUE_PDV_STEP_2
  WHERE
    rn = 1
), TMP_FINAL_BP AS (
  SELECT
    rcomp.NO_SIRET AS SIRET,
    SUBSTR(rcomp.NO_SIRET, 1, 9) AS SIREN,
    coface.LIRASO AS Raison_sociale,
    SUBSTR(coface.COPOST, 1, 2) AS Departement_du_PDV,
    LIB_territoire_BP AS libelle_BP_CE_du_territoire,
    CB_territoire_BP AS code_banque_de_la_BP_CE_du_territoire,
    "nombre_de_groupe_bancaire_acquereur",
    l_canal_part_reseau.proportion_ca AS liste_canaux_actifs_reseau,
    "liste_des_Banques_reseau",
    '' AS liste_des_matricules_reseau,
    l_grp_part.proportion_ca AS liste_groupes_bancaires_avec_part_CA,
    l_grp_part.proportion_nb AS liste_groupes_bancaires_avec_part_NB,
    l_canal_part_hors_reseau.proportion_ca AS liste_canaux_hors_groupe_BPCE,
    banque_pdv.CD_BANQUE_ACQUEREUR_CALCULE AS Code_banque_acquereur_du_PDV,
    banque_pdv.libelle AS Libelle_banque_acquereur_du_PDV,
    banque_pdv.groupe AS Groupe_bancaire_principal,
    part_groupe_principal AS Part_groupe_bancaire_principal,
    '' AS part_CIP_DI,
    '' AS part_CIP_DD,
    '' AS part_CIP_PRO,
    '' AS part_CB_DI,
    '' AS part_CB_DD,
    '' AS part_CB_PRO,
    CASE
      WHEN CB_territoire_BP = '10107'
      THEN '001'
      WHEN CB_territoire_BP = '17169'
      THEN '537'
      WHEN CB_territoire_BP = '17679'
      THEN '972'
      WHEN CB_territoire_BP = '10207'
      THEN '002'
      WHEN CB_territoire_BP = '19707'
      THEN '078'
      WHEN CB_territoire_BP = '18707'
      THEN '087'
      WHEN CB_territoire_BP = '10548'
      THEN '552'
      WHEN CB_territoire_BP = '40978'
      THEN '46B'
      WHEN CB_territoire_BP = '14889'
      THEN '747'
      WHEN CB_territoire_BP = '12239'
      THEN '42B'
      WHEN CB_territoire_BP = '13335'
      THEN '327'
      WHEN CB_territoire_BP = '18715'
      THEN '328'
      WHEN CB_territoire_BP = '11425'
      THEN '329'
      WHEN CB_territoire_BP = '12135'
      THEN '330'
      WHEN CB_territoire_BP = '14445'
      THEN '331'
      WHEN CB_territoire_BP = '18315'
      THEN '332'
      WHEN CB_territoire_BP = '17515'
      THEN '334'
      WHEN CB_territoire_BP = '13485'
      THEN '335'
      WHEN CB_territoire_BP = '14505'
      THEN '336'
      WHEN CB_territoire_BP = '14265'
      THEN '337'
      WHEN CB_territoire_BP = '15135'
      THEN '338'
      WHEN CB_territoire_BP = '13135'
      THEN '339'
      WHEN CB_territoire_BP = '16275'
      THEN '340'
      WHEN CB_territoire_BP = '11315'
      THEN '342'
      WHEN CB_territoire_BP = '13825'
      THEN '344'
      WHEN CB_territoire_BP = '12579'
      THEN '41B'
      WHEN CB_territoire_BP = '42559'
      THEN '213'
    END AS COETB,
    DATE_FORMAT(
      CAST(CAST(CAST(DATE_PARSE(coface.ddentr, '%d%b%Y') AS DATE) AS TIMESTAMP) AS DATE),
      '%d/%m/%Y'
    ) AS Date_de_creation_de_lentreprise,
    coface.liras2 AS Raison_sociale_2,
    coface.liensc AS Enseigne_commerciale,
    coface.licoan AS Libelle_3eme_ligne_adresse,
    coface.livoin AS Libelle_4eme_ligne_adresse,
    coface.lilien AS Libelle_5eme_ligne_dadresse,
    coface.licomm AS Libelle_6eme_ligne_dadresse,
    coface.copost AS Code_postal_ou_code_cedex,
    coface.coiris AS Code_IRIS,
    coface.dacaht AS Date_chiffre_daffaires,
    coface.mtcaht AS Chiffre_daffaires_HT,
    coface.cotefa AS No_Fax_10_car,
    coface.cotela AS No_telephone_10_car,
    coface.coapna AS Code_APE_NAF_REV2_2008,
    naf.libelle_niv1_section AS Libelle_APE_niv1_section,
    naf.libelle_niv2_division AS Libelle_APE_niv2_division,
    naf.libelle_niv3_groupe AS Libelle_APE_niv3_groupe,
    naf.libelle_niv4_classe AS Libelle_APE_niv4_classe,
    naf.libelle_niv5_sous_classe AS Libelle_APE_niv5_sous_classe,
    coface.ctcatj AS Categorie_juridique,
    categ_juridique.libl_natr_jurd_niv3 AS Libelle_CATJUR3,
    categ_juridique.libl_natr_jurd_niv2 AS Libelle_CATJUR2,
    categ_juridique.libl_natr_jurd_niv1 AS Libelle_CATJUR1,
    CASE WHEN NOT mr.NO_SIRET IS NULL THEN 1 ELSE 0 END AS IS_CLIENT,
    CAST(SUM(MT_BRUT_TRANSACTION) AS DOUBLE) / COUNT(*) AS panier_moyen,
    CAST(5 * SUM(1) AS VARCHAR) AS estimation_NB_monetique_total,
    CAST(5 * SUM(MT_BRUT_TRANSACTION) AS VARCHAR) AS estimation_CA_monetique_total,
    SUM(CASE WHEN CD_ERT IN ('00', '10') THEN 1 * 5 ELSE 0 END) AS NB_proxi,
    ROUND(SUM(CASE WHEN CD_ERT IN ('00', '10') THEN MT_BRUT_TRANSACTION * 5 ELSE 0 END), 0) AS MT_proxi,
    SUM(CASE WHEN CD_ERT IN ('24', '27', '64') THEN 1 * 5 ELSE 0 END) AS NB_internet,
    ROUND(
      SUM(CASE WHEN CD_ERT IN ('24', '27', '64') THEN MT_BRUT_TRANSACTION * 5 ELSE 0 END),
      0
    ) AS MT_internet,
    SUM(CASE WHEN CAST(CD_ERT AS BIGINT) BETWEEN 40 AND 59 THEN 1 * 5 ELSE 0 END) AS NB_automate,
    ROUND(
      SUM(
        CASE
          WHEN CAST(CD_ERT AS BIGINT) BETWEEN 40 AND 59
          THEN MT_BRUT_TRANSACTION * 5
          ELSE 0
        END
      ),
      0
    ) AS MT_automate,
    SUM(
      CASE
        WHEN CAST(CD_ERT AS BIGINT) BETWEEN 20 AND 30
        AND (
          NOT CAST(cd_ert AS BIGINT) IN (24, 27)
        )
        THEN 1 * 5
        ELSE 0
      END
    ) AS NB_VAD,
    ROUND(
      SUM(
        CASE
          WHEN CAST(CD_ERT AS BIGINT) BETWEEN 20 AND 30
          AND (
            NOT CAST(cd_ert AS BIGINT) IN (24, 27)
          )
          THEN MT_BRUT_TRANSACTION * 5
          ELSE 0
        END
      ),
      0
    ) AS MT_VAD,
    MAX(mcc.domaine) AS domaine_MCC
  FROM RCOMP AS rcomp
  LEFT JOIN TMP_MR AS mr
    ON rcomp.NO_SIRET = mr.NO_SIRET
  LEFT JOIN BANQUES AS banques
    ON rcomp.CD_BANQUE_ACQUEREUR_CALCULE = banques.code_banque
  LEFT JOIN COFACE AS coface
    ON rcomp.NO_SIRET = coface.cosirt
  LEFT JOIN NAF AS naf
    ON coface.coapna = naf.niv5
  LEFT JOIN CATEG_JURIDIQUE AS categ_juridique
    ON coface.ctcatj = categ_juridique.catg_jurd_niv3
  LEFT JOIN LISTE_GRP_PART AS l_grp_part
    ON rcomp.NO_SIRET = l_grp_part.NO_SIRET
  LEFT JOIN LISTE_CANAL_PART_RESEAU AS l_canal_part_reseau
    ON rcomp.NO_SIRET = l_canal_part_reseau.NO_SIRET
  LEFT JOIN LISTE_CANAL_PART_HORSRESEAU AS l_canal_part_hors_reseau
    ON rcomp.NO_SIRET = l_canal_part_hors_reseau.NO_SIRET
  LEFT JOIN MCC AS mcc
    ON rcomp.CD_MCC = mcc.code_professionnel_mcc
  LEFT JOIN TERRIROIRE_PROSPECT AS territoire_prospect
    ON territoire_prospect.DPT = SUBSTR(coface.copost, 1, 2)
  LEFT JOIN RESEAU AS reseau
    ON rcomp.CD_BANQUE_EMETTEUR = reseau.code_banque
  LEFT JOIN BANQUE_PDV_STEP_3 AS banque_pdv
    ON rcomp.NO_SIRET = banque_pdv.NO_SIRET
  LEFT JOIN SIRET_ONUS AS onus
    ON rcomp.NO_SIRET = onus.NO_SIRET
  WHERE
    NOT rcomp.NO_SIRET IS NULL
    AND RESEAU.reseau IN ('BP', 'CE')
    AND SUBSTR(rcomp.NO_SIRET, 1, 4) <> '0500'
    AND REGEXP_LIKE(rcomp.NO_SIRET, '^[0-9]+$')
    AND LENGTH(rcomp.NO_SIRET) = 14
    AND onus.NO_SIRET IS NULL
  GROUP BY ALL
  ORDER BY
    MT_proxi DESC
), TMP_FINAL_CE AS (
  SELECT
    rcomp.NO_SIRET AS SIRET,
    SUBSTR(rcomp.NO_SIRET, 1, 9) AS SIREN,
    coface.LIRASO AS Raison_sociale,
    SUBSTR(coface.COPOST, 1, 2) AS Departement_du_PDV,
    LIB_territoire_CE AS libelle_BP_CE_du_territoire,
    CB_territoire_CE AS code_banque_de_la_BP_CE_du_territoire,
    "nombre_de_groupe_bancaire_acquereur",
    l_canal_part_reseau.proportion_ca AS liste_canaux_actifs_reseau,
    "liste_des_Banques_reseau",
    '' AS liste_des_matricules_reseau,
    l_grp_part.proportion_ca AS liste_groupes_bancaires_avec_part_CA,
    l_grp_part.proportion_nb AS liste_groupes_bancaires_avec_part_NB,
    l_canal_part_hors_reseau.proportion_ca AS liste_canaux_hors_groupe_BPCE,
    banque_pdv.CD_BANQUE_ACQUEREUR_CALCULE AS Code_banque_acquereur_du_PDV,
    banque_pdv.libelle AS Libelle_banque_acquereur_du_PDV,
    banque_pdv.groupe AS Groupe_bancaire_principal,
    part_groupe_principal AS Part_groupe_bancaire_principal,
    '' AS part_CIP_DI,
    '' AS part_CIP_DD,
    '' AS part_CIP_PRO,
    '' AS part_CB_DI,
    '' AS part_CB_DD,
    '' AS part_CB_PRO,
    CASE
      WHEN CB_territoire_CE = '10107'
      THEN '001'
      WHEN CB_territoire_CE = '17169'
      THEN '537'
      WHEN CB_territoire_CE = '17679'
      THEN '972'
      WHEN CB_territoire_CE = '10207'
      THEN '002'
      WHEN CB_territoire_CE = '10807'
      THEN '008'
      WHEN CB_territoire_CE = '10907'
      THEN '009'
      WHEN CB_territoire_CE = '13507'
      THEN '035'
      WHEN CB_territoire_CE = '13807'
      THEN '038'
      WHEN CB_territoire_CE = '14607'
      THEN '046'
      WHEN CB_territoire_CE = '15135'
      THEN '338'
      WHEN CB_territoire_CE = '13135'
      THEN '339'
      WHEN CB_territoire_CE = '16275'
      THEN '340'
      WHEN CB_territoire_CE = '11315'
      THEN '342'
      WHEN CB_territoire_CE = '13825'
      THEN '344'
      WHEN CB_territoire_CE = '12579'
      THEN '41B'
      WHEN CB_territoire_CE = '42559'
      THEN '213'
    END AS COETB,
    DATE_FORMAT(
      CAST(CAST(CAST(DATE_PARSE(coface.ddentr, '%d%b%Y') AS DATE) AS TIMESTAMP) AS DATE),
      '%d/%m/%Y'
    ) AS Date_de_creation_de_lentreprise,
    coface.liras2 AS Raison_sociale_2,
    coface.liensc AS Enseigne_commerciale,
    coface.licoan AS Libelle_3eme_ligne_adresse,
    coface.livoin AS Libelle_4eme_ligne_adresse,
    coface.lilien AS Libelle_5eme_ligne_dadresse,
    coface.licomm AS Libelle_6eme_ligne_dadresse,
    coface.copost AS Code_postal_ou_code_cedex,
    coface.coiris AS Code_IRIS,
    coface.dacaht AS Date_chiffre_daffaires,
    coface.mtcaht AS Chiffre_daffaires_HT,
    coface.cotefa AS No_Fax_10_car,
    coface.cotela AS No_telephone_10_car,
    coface.coapna AS Code_APE_NAF_REV2_2008,
    naf.libelle_niv1_section AS Libelle_APE_niv1_section,
    naf.libelle_niv2_division AS Libelle_APE_niv2_division,
    naf.libelle_niv3_groupe AS Libelle_APE_niv3_groupe,
    naf.libelle_niv4_classe AS Libelle_APE_niv4_classe,
    naf.libelle_niv5_sous_classe AS Libelle_APE_niv5_sous_classe,
    coface.ctcatj AS Categorie_juridique,
    categ_juridique.libl_natr_jurd_niv3 AS Libelle_CATJUR3,
    categ_juridique.libl_natr_jurd_niv2 AS Libelle_CATJUR2,
    categ_juridique.libl_natr_jurd_niv1 AS Libelle_CATJUR1,
    CASE WHEN NOT mr.NO_SIRET IS NULL THEN 1 ELSE 0 END AS IS_CLIENT,
    CAST(SUM(MT_BRUT_TRANSACTION) AS DOUBLE) / COUNT(*) AS panier_moyen,
    CAST(5 * SUM(1) AS VARCHAR) AS estimation_NB_monetique_total,
    CAST(5 * SUM(MT_BRUT_TRANSACTION) AS VARCHAR) AS estimation_CA_monetique_total,
    SUM(CASE WHEN CD_ERT IN ('00', '10') THEN 1 * 5 ELSE 0 END) AS NB_proxi,
    ROUND(SUM(CASE WHEN CD_ERT IN ('00', '10') THEN MT_BRUT_TRANSACTION * 5 ELSE 0 END), 0) AS MT_proxi,
    SUM(CASE WHEN CD_ERT IN ('24', '27', '64') THEN 1 * 5 ELSE 0 END) AS NB_internet,
    ROUND(
      SUM(CASE WHEN CD_ERT IN ('24', '27', '64') THEN MT_BRUT_TRANSACTION * 5 ELSE 0 END),
      0
    ) AS MT_internet,
    SUM(CASE WHEN CAST(CD_ERT AS BIGINT) BETWEEN 40 AND 59 THEN 1 * 5 ELSE 0 END) AS NB_automate,
    ROUND(
      SUM(
        CASE
          WHEN CAST(CD_ERT AS BIGINT) BETWEEN 40 AND 59
          THEN MT_BRUT_TRANSACTION * 5
          ELSE 0
        END
      ),
      0
    ) AS MT_automate,
    SUM(
      CASE
        WHEN CAST(CD_ERT AS BIGINT) BETWEEN 20 AND 30
        AND (
          NOT CAST(cd_ert AS BIGINT) IN (24, 27)
        )
        THEN 1 * 5
        ELSE 0
      END
    ) AS NB_VAD,
    ROUND(
      SUM(
        CASE
          WHEN CAST(CD_ERT AS BIGINT) BETWEEN 20 AND 30
          AND (
            NOT CAST(cd_ert AS BIGINT) IN (24, 27)
          )
          THEN MT_BRUT_TRANSACTION * 5
          ELSE 0
        END
      ),
      0
    ) AS MT_VAD,
    MAX(mcc.domaine) AS domaine_MCC
  FROM RCOMP AS rcomp
  LEFT JOIN TMP_MR AS mr
    ON rcomp.NO_SIRET = mr.NO_SIRET
  LEFT JOIN BANQUES AS banques
    ON rcomp.CD_BANQUE_ACQUEREUR_CALCULE = banques.code_banque
  LEFT JOIN COFACE AS coface
    ON rcomp.NO_SIRET = coface.cosirt
  LEFT JOIN NAF AS naf
    ON coface.coapna = naf.niv5
  LEFT JOIN CATEG_JURIDIQUE AS categ_juridique
    ON coface.ctcatj = categ_juridique.catg_jurd_niv3
  LEFT JOIN LISTE_GRP_PART AS l_grp_part
    ON rcomp.NO_SIRET = l_grp_part.NO_SIRET
  LEFT JOIN LISTE_CANAL_PART_RESEAU AS l_canal_part_reseau
    ON rcomp.NO_SIRET = l_canal_part_reseau.NO_SIRET
  LEFT JOIN LISTE_CANAL_PART_HORSRESEAU AS l_canal_part_hors_reseau
    ON rcomp.NO_SIRET = l_canal_part_hors_reseau.NO_SIRET
  LEFT JOIN MCC AS mcc
    ON rcomp.CD_MCC = mcc.code_professionnel_mcc
  LEFT JOIN TERRIROIRE_PROSPECT AS territoire_prospect
    ON territoire_prospect.DPT = SUBSTR(coface.copost, 1, 2)
  LEFT JOIN RESEAU AS reseau
    ON rcomp.CD_BANQUE_EMETTEUR = reseau.code_banque
  LEFT JOIN BANQUE_PDV_STEP_3 AS banque_pdv
    ON rcomp.NO_SIRET = banque_pdv.NO_SIRET
  LEFT JOIN SIRET_ONUS AS onus
    ON rcomp.NO_SIRET = onus.NO_SIRET
  WHERE
    NOT rcomp.NO_SIRET IS NULL
    AND RESEAU.reseau IN ('BP', 'CE')
    AND SUBSTR(rcomp.NO_SIRET, 1, 4) <> '0500'
    AND REGEXP_LIKE(rcomp.NO_SIRET, '^[0-9]+$')
    AND LENGTH(rcomp.NO_SIRET) = 14
    AND onus.NO_SIRET IS NULL
  GROUP BY ALL
  ORDER BY
    MT_proxi DESC
), TMP_FINAL AS (
  SELECT
    *
  FROM TMP_FINAL_BP
  UNION ALL
  SELECT
    *
  FROM TMP_FINAL_CE
)
SELECT
  *
FROM TMP_FINAL