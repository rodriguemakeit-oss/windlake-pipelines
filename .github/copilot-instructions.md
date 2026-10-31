# Copilot instructions — ltsa_data

## Databricks jobs — utiliser le Makefile

- **Toujours préférer `make test`** au lieu d'un `databricks jobs submit` manuel. Le Makefile :
  - upload le notebook + helpers (`silver/helpers`, `silver/compute_generative_ai`, `gold/utils`)
  - soumet en **serverless** (pas de `existing_cluster_id` dans la spec) avec `--no-wait`
  - affiche `Run URL` immédiatement
  - poll jusqu'à completion + dump `result_state` / traceback si fail
- Usage typique :
  ```bash
  make test NOTEBOOK=gold/pm_budget_tracker_compute \
    PARAMS='{"catalog":"ltsa_dev","gold_bucket":"demo-data-bucket"}'
  ```
- Config via `.env.preprod` / `.env.prod` (`PROFILE`, `USER_EMAIL`, ...). Override d'environnement : `ENV=prod make test ...`.
- Si auth expirée : `databricks auth login --host https://workspace.example.invalid --profile preprod` (le script `authenticate_preprod.sh` est cassé — cherche `main_bundle/.env` qui n'existe pas).

## Jira — utiliser `make jira-comment`

- Après un run réussi sur un ticket, **poster un commentaire Jira** avec l'URL du run via :
  ```bash
  make jira-comment TICKET=LTSA-153 \
    COMMENT="run URL: https://workspace.example.invalid/?o=...#job/.../run/... — SUCCESS"
  ```
- Nécessite `JIRA_URL` + `JIRA_TOKEN` (PAT portfolio) dans `.env.$(ENV)`. Jira on-prem portfolio → API v2 + auth Bearer (pas Basic / pas Atlassian Cloud).
- Endpoint utilisé : `POST $JIRA_URL/rest/api/2/issue/$TICKET/comment` avec body `{"body": "..."}`. Retour 201 + JSON contenant `id` / `author`.

## GitHub PR — utiliser `make pr`

- Pour ouvrir une PR depuis la branche courante :
  ```bash
  make pr BASE=release/ltsa-121/migration \
    TITLE="feat(gold): split code into farm/substation/turbine (LTSA-153)" \
    BODY="LTSA-153 — see Jira"
  ```
- Pousse la branche puis ouvre la PR via `gh pr create`. Détecte automatiquement le host (`example.invalid` chez portfolio) depuis `git remote origin`.
- Nécessite `GH_TOKEN` (PAT GitHub Enterprise, scope `repo`) dans `.env.$(ENV)`. La cible exporte `GH_HOST` + `GH_TOKEN` à la volée → pas besoin de `gh auth login` séparé.
- `BASE` par défaut : `main`. Pour portfolio LTSA, généralement `release/ltsa-121/migration`. `DRAFT=1` pour un draft.

### Fallback : submit manuel (rare)

Si besoin de bypass le Makefile (ex. spec exotique, debug réseau) :

- **Serverless par défaut** : ne pas mettre `existing_cluster_id` dans la task spec. N'utiliser `existing_cluster_id` (ex. `0511-101438-ad2yvi8d`) que sur demande explicite ou dépendance incompatible serverless (libs custom, init scripts).
- **`--no-wait` systématique** : le CLI rend la main avec le `run_id`, puis on récupère l'URL via l'API.
- **Toujours afficher le `run_page_url`** dans la réponse à l'utilisateur, pas juste le `run_id`.
  ```bash
  databricks jobs submit --profile preprod --no-wait --json '<spec sans existing_cluster_id>'
  databricks api get "/api/2.1/jobs/runs/get?run_id=<id>" --profile preprod \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print('url:', d.get('run_page_url')); print('state:', d.get('state',{}).get('life_cycle_state'))"
  ```

## Notebooks (.ipynb)

- Les `.ipynb` stockent `source` comme array de lignes JSON : `replace_string_in_file` peut **échouer silencieusement** si l'ancien texte ne matche pas exactement la représentation JSON (échappements `\n`, `\"`).
- Pour modifier le contenu d'une cellule, **toujours éditer via un petit script Python** dans `tools/_scratch/` (json.load → mutate `cell["source"]` → json.dump indent=1).
- **Vérifier** la mutation avant de redéployer : `grep -o <token> file.ipynb | wc -l`.

## File system

- **Never write to `/tmp`** (ni `/private/tmp`). Cela empêche l'auto-validation des commandes par l'utilisateur.
- Pour les fichiers temporaires (scripts ad-hoc, JSON intermédiaires), utiliser `tools/_scratch/` (gitignored).
- Préférer `create_file` / `replace_string_in_file` plutôt que des heredocs shell.

## Terminal

- **Jamais de heredoc multi-ligne** (`<< 'EOF' ... EOF`) envoyé en une seule commande — zsh bloque sur `cmdand heredoc>`. Toujours passer par un fichier de script.
- Lancer une commande à la fois, attendre la sortie avant la suivante.
