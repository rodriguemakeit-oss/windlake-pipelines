# Pipeline IA - Extraction de Disponibilités Turbines

## Vue d'ensemble

Ce document explique le fonctionnement du pipeline **LLM Document to DataFrame** qui extrait automatiquement les données de disponibilité des turbines à partir de rapports de maintenance en PDF/Excel.

**Objectif** : Convertir des documents non-structurés (rapports maintaineurs) en données structurées et exploitables.

---

## High-level architecture

```
┌─────────────────────────────────────────────────────────────┐
│            AI Pipeline — Extraction Flow                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Databricks configuration                                │
│     ↓                                                         │
│  2. Initialise Azure services                                │
│     • Document Intelligence (OCR)                            │
│     • Azure OpenAI GPT-4o (LLM)                             │
│     ↓                                                         │
│  3. Smart file filtering                                     │
│     • Load processing history (tracking)                    │
│     • Identify new files                                    │
│     ↓                                                         │
│  4. Metadata preparation                                     │
│     • Extract context (country, maintainer, year, month)    │
│     ↓                                                         │
│  5. AI extraction                                            │
│     • OCR → raw text                                          │
│     • LLM → structured data                                   │
│     ↓                                                         │
│  6. Persist results                                          │
│     • availability_genai (extracted data)                    │
│     • files_genai_processed (history)                        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## The 6 Phases in Detail

### 🚀 Phase 1: Configuration and Initialisation

**Responsibility**: Load parameters and start cloud services.

**Key parameters**:
- AI model to use (e.g., `gpt-4o`)
- Number of files processed per run
- Processing options (Tesseract, Excel mode, etc.)

**Output**:
- ✅ Azure clients initialised and ready
- ✅ Configuration loaded in memory

---

### 📋 Phase 2: Smart File Filtering

**Responsibility**: Avoid reprocessing the same files.

**Steps**:
1. Load the **tracking table** with the history
2. List new files
3. Filter out already-processed ones
4. Keep only the first `N` (configurable limit)

**Benefits**:
- ⏱️ Time savings (no rework)
- 💰 Cost savings (fewer API calls)
- 🔄 Idempotent pipeline (safe to re-run)

**Output**:
- 📝 List of new files to process

---

### 📊 Phase 3: Metadata Preparation

**Responsibility**: Extract the context of each file from its path.

**Automatic extraction**:
- Country, maintainer, year, month → parsed from file path
- File extension
- Format validation

**Output**:
- 📑 DataFrame with enriched metadata

---

### 🔍 Phase 4: Text Extraction (Optional)

**Responsibility**: Pre-process documents before AI analysis.

**Option 1: Tesseract OCR (Optional)**
- Extracts text locally
- Reduces pages sent to Document Intelligence
- Saves API costs

**Option 2: Direct to LLM (Default)**
- Document Intelligence processes the full document
- Simpler, no pre-filtering

**Output**:
- 📄 Raw text ready for AI analysis

---

### 🤖 Phase 5: AI Extraction (Pipeline Core)

**Responsibility**: Turn raw text into structured data.

**Step 5a: Document Intelligence (OCR)**
- Service: Azure Document Intelligence
- Task: extract raw text from the PDF/Excel
- Returns: text structured by document zone

**Step 5b: Azure OpenAI GPT-4o (LLM)**
- Service: Azure OpenAI API
- Task: analyse the text and extract business data
- Questions handled:
  - Which turbines are mentioned?
  - What are the availability dates?
  - Were there any maintenance events? Which?
  - What is the state of each turbine?

**Prompt system**:
- A generic default prompt
- Per-maintainer custom prompts (Vestas, Siemens, GE, etc.)
- Stored in `compute_generative_ai/resources/prompts.yaml`

**Error handling**:
- Automatic retry on API failure (configurable)
- Delay between files (throttling)
- Errors recorded for follow-up

**Output**:
- ✅ Structured data (results)
- ❌ Errors (for audit)

---

### 💾 Phase 6: Persist Results

**Responsibility**: Persist data and history.

#### Table 1: `silver.availability_genai`
- **Contains**: data extracted by the LLM
- **Schema**: turbine_id, availability_date, status, maintainer, country, year, month, etc.
- **Mode**: Append
- **Format**: Delta Lake (Apache Spark)

#### Table 2: `silver.files_genai_processed`
- **Contains**: processing history
- **Schema**: file, run_date, error
- **Mode**: Append
- **Purpose**: avoid reprocessing on the next run

**Output**:
- ✅ Data saved to Delta tables

---

## Per-maintainer handling

The pipeline adapts its behaviour to the maintainer identified from the file path (e.g. `.../vestas/2025/03/report.pdf`). Nine maintainers are supported.

| Maintainer | Internal value | Specifics |
|---|---|---|
| Deutsche Windtechnik | `dwt` | Availability regex: `availability\|verfugbarkeiten` |
| Vestas | `vestas` | Availability regex: `availability\|disponibilidad` |
| Enercon | `enercon` | Standard handling |
| Nordex Acciona | `nordex acciona` | ⚠️ Stop keyword `memo` + **15-day** time shift applied to dates |
| Siemens Gamesa (SGRE) | `sgre` | Extended regex: `monthly availability`, `serial number`, `plant description`, `disponibilidad` |
| Gamesa | `gamesa` | Availability regex: `disponibilidad` |
| GE | `ge` | Excel reads target sheets `[0, "Turbine Summary"]` |
| Suzlon | `suzlon` | Excel reads target sheets `[0, "Availability Year"]` |
| Unknown | `unknown` | Fallback — generic `availability` regex |

**Limits and formats**:
- `PAGE_LIMIT = 23`: maximum pages sent to Azure Document Intelligence per file.
- Supported formats: `pdf`, `xlsx`, `xls`, `xlsm`, `xlsb`.
- Prompts are customisable per maintainer in `src/silver/compute_generative_ai/resources/prompts.yaml`.

---

## Configuration

| Parameter | Default | Description |
|-----------|--------|-------------|
| `ia_model` | `gpt-4o` | OpenAI model to use |
| `ia_max_retries` | `3` | Retry attempts on error |
| `ia_files_limit_per_run` | `10` | Max files per run |
| `ia_sleep_sec` | `60` | Delay between two files (seconds) |
| `allowed_file_extensions` | `["pdf", "xlsx", "xls"]` | Allowed extensions |
| `excel_mode` | `openpyxl` | Excel reader mode |
| `use_tesseract` | `false` | Enable upstream Tesseract OCR |
| `debug` | `false` | Debug mode (verbose logs) |

---

## Simplified Execution Flow

```
START
  │
  ├─→ [INIT] Load config + Azure clients
  │
  ├─→ [FILTER] Load tracking + list files
  │
  ├─→ [METADATA] Extract metadata from path
  │
  ├─→ [OCR] Tesseract (optional) → raw text
  │
  ├─→ [LLM] GPT-4o → structured data
  │
  ├─→ [SAVE] Write to availability_genai
  │
  ├─→ [TRACK] Update files_genai_processed
  │
  └─→ END ✅
```

---

## Key Points for the Product Owner

### ✅ Benefits

1. **Full automation**: no manual data entry
2. **Smart deduplication**: tracking prevents reprocessing
3. **Scalable**: can handle hundreds of files
4. **Flexible**: prompts customisable per provider
5. **Cost-optimised**: Tesseract reduces calls to paid APIs
6. **Traceable**: complete processing history

### ⚠️ Limitations

1. **Imperfect OCR**: may fail on poor-quality images
2. **LLM dependency**: requires a valid Azure OpenAI key
3. **Latency**: about 1 file per minute (throttle recommended)
4. **API cost**: Document Intelligence + OpenAI = recurring costs
5. **Volume limit**: 2 MB max per file (Azure limit)

### 🎯 Success Metrics

- **Success rate**: % of files processed without error
- **Processing time**: minutes per file
- **Cost**: $ per processed file
- **Extraction quality**: % of usable records

---

## Use Cases

### ✅ Good for:
- Structured PDF/Excel reports
- Repetitive data (same format per maintainer)
- Moderate volumes (hundreds/thousands)
- Semi-structured data

### ❌ Not good for:
- Handwritten document scans
- Very low quality documents
- Highly variable formats
- Highly unstructured data

---

## Future Improvements

- [ ] Parallelisation (process 5 files in parallel)
- [ ] Model fine-tuning (specialised on wind reports)
- [ ] Validation of extracted data
- [ ] Monitoring dashboard
- [ ] Alerts on repeated errors

---

*Last updated: 27 April 2026*
