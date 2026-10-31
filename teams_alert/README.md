# teams_alert — LTSA "PDFs non traités" Databricks SQL alert

Code IaaC pour provisionner l'alerte Databricks SQL qui envoie un mail Teams
listant les fichiers du pipeline GenAI en anomalie : `unprocessed` (jamais
extraits) et `unmatch` (extraits mais non rattachés à Telemetry).

## Composants

| Fichier | Rôle |
|---|---|
| `config.py` | Config par env (workspace URL, warehouse, catalog, destination Teams) |
| `sql/pdfs_non_traites.sql` | Requête (placeholder `{{CATALOG}}` substitué à l'apply) |
| `templates/subject.txt` | Subject du mail (variables Databricks `{{...}}`) |
| `templates/body.html` | Body HTML du mail |
| `_databricks_api.py` | Helpers CLI/API (lookups par display_name, upserts idempotents) |
| `apply.py` | Crée ou met à jour query + alert + template |
| `disable.py` | Supprime alert + query d'un env |

## Pré-requis

- `databricks` CLI configuré avec les profils `preprod` et `prod` (`~/.databrickscfg`)
- Profil avec droits read/write sur SQL queries + alerts
- Destination Teams déjà créée par l'admin Databricks (IDs dans `config.py`)

## URLs des alertes

| Env | URL |
|-----|-----|
| **Preprod** | https://workspace.example.invalid/sql/alerts/00000000-0000-0000-0000-000000000000 |
| **Prod** | https://workspace.example.invalid/sql/alerts/00000000-0000-0000-0000-000000000000 |

## Apply (idempotent)

```bash
# Provision en preprod
python -m teams_alert.apply preprod

# Provision en prod
python -m teams_alert.apply prod
```

Le script :
1. Charge le SQL en substituant `{{CATALOG}}` (`ltsa_dev` ou `ltsa_prod`)
2. Cherche une query existante par display_name `LTSA - PDFs non traités (<env>)`
   → create ou update
3. Cherche une alert existante par display_name `LTSA — PDFs non traités (<env>)`
   → create ou update (condition `n_pdfs > 0`, retrigger 60s)
4. Set custom_subject + custom_body via la legacy API
5. Re-applique la condition v2 numérique (la legacy PUT écrase en string)

**Affiche ensuite l'URL de l'alerte + les 2 étapes UI manuelles** (l'API publique
n'expose pas les destinations ni les schedules des SQL Alerts V2).

## Étapes UI après le 1er apply

1. Ouvrir l'URL affichée par `apply.py`
2. Panneau de droite → **Add destination** → choisir
   `LTSA Daily Error Tracking (Teams)` (l'ID est dans `config.py`)
3. Panneau de droite → **Add schedule** → Every 1 day à l'heure UTC souhaitée

(À refaire si on recrée l'alerte ; sinon l'apply préserve schedule + destination
en update.)

## Disable

```bash
python -m teams_alert.disable preprod
python -m teams_alert.disable prod
```

Supprime alert + query. Le schedule sous-jacent (Job caché) disparaît avec l'alert.

## Comportement de l'alerte

- **Query** : 1 ligne par fichier en anomalie, avec une colonne `tag`
  (`unprocessed` = en quarantaine jamais extrait, **toute cause** ; `unmatch` =
  extrait mais non matché Telemetry). **Aucune troncature** : tous les fichiers des
  deux catégories sont affichés. `n_pdfs` (dernière colonne) = total répété
  utilisé par la condition. Colonne `fichier` = nom du fichier seul.
  Trié `unprocessed` d'abord (prioritaires), puis `unmatch`, chacun par date DESC.
- **Trigger** : `n_pdfs > 0` (numérique, robuste — sans aggregation côté alerte)
- **Re-notif** : `seconds_to_retrigger = 60` → mail envoyé à **chaque run du
  schedule** tant que `TRIGGERED` (au lieu du "just once" par défaut)
- **Empty result** : `OK` → 0 PDF en quarantaine = pas de mail
- **Template** : tableau HTML simple (le wrapper logo + footer Databricks
  reste, non désactivable côté alerte)

## Limites connues

- **Schedule + Destination = UI only**. L'API publique Databricks ne permet pas
  de les piloter (les SQL Alerts V2 utilisent un Job interne caché).
- **Wrapper Databricks** dans le mail (logo, "You're receiving this email…",
  footer adresse) : non désactivable côté alerte.
- **Legacy PUT clobber** : la legacy API écrase la condition v2 ; `apply.py`
  re-applique la condition après chaque modif template.
