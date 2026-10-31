-- PDFs "unprocessed or unmatched" du pipeline GenAI (1 ligne par fichier).
-- Placeholder {{CATALOG}} substitué à l'apply selon l'env (ltsa_dev / ltsa_prod).
--
-- Deux catégories agrégées via UNION ALL (ensembles disjoints : un fichier
-- "unmatch" est forcément extrait, donc jamais dans "unprocessed") :
--   tag = 'unprocessed' : en quarantaine et jamais extrait (TOUTES causes
--                         d'échec, y compris "0 availability rows" / non parsable)
--   tag = 'unmatch'     : extrait mais aucune turbine rattachée à Telemetry
--                         (silver.availability_genai_telemetry_no_match)
--
-- Le mail est TRONQUÉ pour tenir dans Teams : au plus ~`max_rows` lignes (voir
-- "Plafond" ci-dessous) ; n_pdfs donne toujours le total réel. extrait_llm est
-- limité aux 5 premières clés (+ compteur du reste).
--
-- Colonnes (dans l'ordre du tri) :
--   tag          STRING  'unprocessed' | 'unmatch'
--   maintainer   STRING  2e segment du path (ex. DWT, Vestas, Nordex Acciona)
--   pays         STRING  1er segment du path (ex. Belgium)
--   cause        STRING  cause succincte (erreur quarantaine classifiée, ou 'non matché Telemetry')
--   date_pdf     STRING  YYYY-MM extrait du path (pattern /YYYY/M-MMM/), '' si introuvable
--   fichier      STRING  nom du fichier seul (dernier segment du path)
--   extrait_llm  STRING  (unmatch) clé(s) brute(s) extraite(s) par le LLM = turbine_id
--                        (ex. 'AE1|61004509' = nom|serial), 1 token par turbine du
--                        parc, séparés par ' ; ' (le '|' interne reste le sépa
--                        nom|serial) ; NULL en unprocessed. Permet de voir ce que
--                        le LLM a lu et pourquoi le match Telemetry a échoué.
--   n_pdfs       BIGINT  total RÉEL (non plafonné) répété — condition d'alerte: n_pdfs > 0
--
-- Plafond par catégorie : on garde les X lignes les plus récentes par
-- (tag, maintainer, pays), avec X proportionnel au budget `max_rows` :
--   X = max( floor(max_rows * taille_groupe / total_brut), 1 )
-- (au moins 1 ligne par groupe pour que chaque catégorie soit représentée).

WITH q AS (
  SELECT file, error, run_date,
    ROW_NUMBER() OVER (PARTITION BY file ORDER BY run_date DESC) AS rn
  FROM {{CATALOG}}.silver.files_genai_quarantine
),
latest AS (SELECT file, error, run_date FROM q WHERE rn = 1),
ok AS (SELECT DISTINCT file FROM {{CATALOG}}.silver.availability_genai),
-- Allowlist of reports the business does not want raised as errors (event
-- reports, component reports, annual reports, empty/corrupt uploads…).
-- Maintained by the business in the `_ignored_errors` tab of
-- SGRE_Mapping_Table.xlsx and loaded by src/silver/turbine_alias_load.py.
-- `pattern` is matched as a case-insensitive substring of the report path: a
-- full path targets one file, a partial text targets every file containing it.
-- (`error_type` is informational only — the suppression applies to both tags.)
ignored AS (SELECT pattern FROM {{CATALOG}}.silver.files_genai_ignored),
unprocessed AS (
  -- Tout fichier en quarantaine jamais extrait, quelle que soit l'erreur.
  SELECT 'unprocessed' AS tag, latest.file AS file, latest.run_date AS run_date, latest.error AS error,
         CAST(NULL AS STRING) AS extrait_llm
  FROM latest
  LEFT ANTI JOIN ok ON latest.file = ok.file
),
unmatch AS (
  -- Extrait mais non rattaché à Telemetry. On agrège par fichier les clé(s) brute(s)
  -- extraite(s) par le LLM (turbine_id, ex. 'AE1|61004509' = nom|serial), pour
  -- voir ce que le LLM a lu et diagnostiquer l'échec de match.
  -- Séparateur ' ; ' entre turbines (le '|' est déjà utilisé DANS un turbine_id
  -- comme séparateur nom|serial — SERIAL_NUMBER_SEP — donc on n'y touche pas ici).
  SELECT 'unmatch' AS tag, file, MAX(run_date) AS run_date, CAST(NULL AS STRING) AS error,
         concat(
           array_join(slice(array_sort(array_distinct(collect_list(turbine_id))), 1, 5), ' ; '),
           CASE WHEN size(array_distinct(collect_list(turbine_id))) > 5
                THEN concat(' ; … (+', size(array_distinct(collect_list(turbine_id))) - 5, ')')
                ELSE '' END
         ) AS extrait_llm
  FROM {{CATALOG}}.silver.availability_genai_telemetry_no_match
  GROUP BY file
),
combined AS (
  SELECT tag, file, run_date, error, extrait_llm FROM (
    SELECT tag, file, run_date, error, extrait_llm FROM unprocessed
    UNION ALL
    SELECT tag, file, run_date, error, extrait_llm FROM unmatch
  ) u
  -- Suppression de l'allowlist, appliquée aux deux catégories.
  WHERE NOT EXISTS (
    SELECT 1 FROM ignored ig
    WHERE lower(u.file) LIKE concat('%', lower(ig.pattern), '%')
  )
),
enriched AS (
  SELECT
    tag,
    split(file, '/')[1] AS maintainer,
    split(file, '/')[0] AS pays,
    CASE
      WHEN tag = 'unmatch' THEN 'non matché Telemetry'
      WHEN error IS NULL THEN 'jamais tenté'
      WHEN lower(error) LIKE '%429%' OR lower(error) LIKE '%too many requests%' THEN '429 rate-limit'
      WHEN lower(error) LIKE '%403%' THEN '403 accès OpenAI/réseau'
      WHEN lower(error) LIKE '%400%' THEN '400 requête LLM'
      WHEN lower(error) LIKE '%401%' OR lower(error) LIKE '%unauthorized%' THEN '401 SharePoint'
      WHEN lower(error) LIKE '%positional indexers%' OR lower(error) LIKE '%out-of-bounds%' THEN 'df vide (OCR)'
      WHEN lower(error) LIKE '%nonetype%' THEN 'NoneType (parsing)'
      WHEN lower(error) LIKE '%empty file%' THEN 'fichier vide (source)'
      WHEN lower(error) LIKE '%corrupt%' OR lower(error) LIKE '%unsupported%' THEN 'fichier corrompu (source)'
      WHEN lower(error) LIKE '%invalid request%' THEN 'DI invalid request'
      WHEN lower(error) LIKE '%worksheet%' OR lower(error) LIKE '%does not exist%' THEN 'onglet Excel manquant'
      WHEN lower(error) LIKE '%0 availability rows%' OR lower(error) LIKE '%empty or unparsable%' THEN '0 ligne extraite'
      WHEN lower(error) LIKE '%future-dated%' OR lower(error) LIKE '%hallucination%' THEN 'date future (LLM)'
      WHEN lower(error) LIKE '%invalid date%' THEN 'date invalide LLM'
      WHEN lower(error) LIKE '%pickle%' THEN 'pickle/APIStatusError'
      ELSE substr(error, 1, 40)
    END AS cause,
    CASE
      WHEN regexp_extract(file, '/(\\d{4})/(\\d{1,2})-', 1) <> ''
      THEN concat(
        regexp_extract(file, '/(\\d{4})/(\\d{1,2})-', 1),
        '-',
        lpad(regexp_extract(file, '/(\\d{4})/(\\d{1,2})-', 2), 2, '0')
      )
      ELSE ''
    END AS date_pdf,
    element_at(split(file, '/'), -1) AS fichier,
    extrait_llm,
    run_date
  FROM combined
),
-- Nombre de lignes max affichables dans le mail (à ajuster selon la limite
-- Teams/Outlook ; sert à dimensionner l'échantillon par catégorie).
params AS (SELECT 50 AS max_rows),
ranked AS (
  SELECT
    e.*,
    COUNT(*) OVER () AS total_rows,
    COUNT(*) OVER (PARTITION BY tag, maintainer, pays) AS grp_size,
    ROW_NUMBER() OVER (
      PARTITION BY tag, maintainer, pays
      ORDER BY date_pdf DESC, run_date DESC
    ) AS rn
  FROM enriched e
)
SELECT
  tag,
  maintainer,
  pays,
  cause,
  date_pdf,
  fichier,
  extrait_llm,
  CAST(total_rows AS BIGINT) AS n_pdfs
FROM ranked CROSS JOIN params
-- X lignes les plus récentes par (tag, maintainer, pays), proportionnel au budget.
WHERE rn <= GREATEST(CAST(FLOOR(max_rows * grp_size / total_rows) AS INT), 1)
ORDER BY tag, maintainer, pays, cause, date_pdf DESC, fichier
