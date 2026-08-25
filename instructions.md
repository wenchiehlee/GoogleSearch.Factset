# FactSet Pipeline v3.6.1 - System Integration Guide

## System Overview

**Architecture**: Two-Stage Production Pipeline  
**Version**: Search Group v3.5.1 + Process Group v3.6.1  
**Communication**: File-based via `data/md/` directory  
**Date**: 2025-07-01

### Component Responsibilities

| Stage | Version | Purpose | Detailed Guide |
|-------|---------|---------|----------------|
| **Search Group** | v3.5.1 | Data collection with API key rotation and content validation | [search_group/instructions.md](search_group/instructions.md) |
| **Process Group** | v3.6.1 | Analysis, quality scoring, and multi-format reporting | [process_group/instructions.md](process_group/instructions.md) |

### Communication Pattern

```
Input: StockID_TWSE_TPEX.csv (116+ Taiwan companies)
    ↓
┌─────────────────────────────────┐
│ Search Group (v3.5.1)          │ 
│ ├─ API Key Rotation (7 keys)   │
│ ├─ Content Validation          │
│ ├─ REFINED_SEARCH_PATTERNS     │
│ └─ Pure Hash Filenames         │
└─────────────────────────────────┘
    ↓ data/md/*.md (File-based communication)
┌─────────────────────────────────┐
│ Process Group (v3.6.1)         │
│ ├─ MD File Analysis            │
│ ├─ Query Pattern Analysis      │
│ ├─ Watchlist Coverage          │
│ └─ Multi-Format Reports        │
└─────────────────────────────────┘
    ↓
Output: Google Sheets Dashboard + CSV Reports
```

## System Setup & Configuration

### 1. Environment Configuration

Create `.env` file in project root:

```bash
# Search Group - Multiple API keys for rotation
GOOGLE_SEARCH_API_KEY=your_primary_api_key_here
GOOGLE_SEARCH_CSE_ID=your_custom_search_engine_id_here
GOOGLE_SEARCH_API_KEY1=your_second_api_key_here
GOOGLE_SEARCH_API_KEY2=your_third_api_key_here
# ... up to GOOGLE_SEARCH_API_KEY6 (7 keys total)

# Process Group - Google Sheets integration
GOOGLE_SHEET_ID=your_sheet_id_here
GOOGLE_SHEETS_CREDENTIALS={"type":"service_account",...}

# Performance tuning
SEARCH_RATE_LIMIT_PER_SECOND=1.0
MIN_QUALITY_THRESHOLD=4
LOG_LEVEL=INFO
```

### 2. Directory Structure

```bash
# Required directories (auto-created by components)
data/
├── md/                    # Search Group output → Process Group input
├── reports/               # Process Group CSV output
└── cache/                 # Search caching and progress

# Required input file
StockID_TWSE_TPEX.csv              # Must be in project root (代號,名稱 format)
```

### 3. Dependencies Installation

```bash
# Install all dependencies
pip install -r requirements.txt

# Core dependencies include:
# - google-api-python-client (Search Group)
# - pandas, gspread (Process Group)
# - requests, beautifulsoup4 (Both)
```

## Integrated Workflows

### Complete Pipeline Execution

```bash
# 1. Execute Search Group (data collection)
cd search_group
python search_cli.py validate                           # Validate API keys
python search_cli.py search --all --count 2 --min-quality 4  # Collect data

# 2. Execute Process Group (analysis & reporting)
cd ../process_group  
python process_cli.py validate                          # Validate processing setup
python process_cli.py process                           # Full analysis + upload

# 3. Verify results
ls ../data/md/*.md | wc -l                              # Count generated files
ls ../data/reports/*.csv                                # Check reports
```

### Incremental Workflows

```bash
# Search specific companies only
cd search_group
python search_cli.py search --company 2330 --count 3

# Process recent files only  
cd process_group
python process_cli.py process-recent --hours=24

# Generate specific reports without upload
cd process_group
python process_cli.py keyword-summary --no-upload      # Query patterns
python process_cli.py watchlist-summary --no-upload    # Watchlist coverage
```

### Development & Testing

```bash
# Test Search Group independently
cd search_group
python search_cli.py search --company 2330 --count 1   # Test single company
python search_cli.py status                            # Check API key rotation

# Test Process Group independently
cd process_group
python process_cli.py process-single --company 2330 --no-upload  # Test processing
python process_cli.py analyze-quality                  # Test quality analysis
```

## File Format Integration

### Search Group Output (MD Files)

```markdown
---
search_query: 台積電 2330 factset EPS 預估           # Used by Process Group
quality_score: 8                                    # Used by Process Group  
company: 台積電                                     # Used for validation
stock_code: 2330                                    # Used for validation
content_validation: {"is_valid": true, "reason": "..."} # Used by Process Group
version: v3.5.1                                     # Version tracking
---
[Content continues...]
```

### Process Group Output Reports

1. **Portfolio Summary** (14 columns): Investment overview
2. **Detailed Report** (22 columns): Complete analysis with GitHub Raw URLs
3. **Query Pattern Summary** (10 columns): Search effectiveness analysis  
4. **Watchlist Summary** (12 columns): Company coverage status

*See Process Group documentation for detailed format specifications.*

## Error Handling & Recovery

### Cross-Stage Error Scenarios

| Issue | Detection | Resolution |
|-------|-----------|------------|
| No MD files generated | Process Group shows "Found 0 MD files" | Run Search Group first |
| Search quota exhausted | Search Group API errors | Add more API keys to .env |
| Invalid MD format | Process Group parsing errors | Check Search Group version compatibility |
| Missing watchlist | Process Group validation warnings | Ensure `StockID_TWSE_TPEX.csv` in project root |

### System Validation

```bash
# Validate complete system
cd search_group && python search_cli.py validate
cd ../process_group && python process_cli.py validate

# Check file communication
cd search_group && python search_cli.py search --company 2330 --count 1
cd ../process_group && python process_cli.py process-single --company 2330
```

## Monitoring & Status

### Search Group Monitoring

```bash
cd search_group
python search_cli.py status
# Shows: API key rotation status, quota usage, cache statistics
```

### Process Group Monitoring

```bash
cd process_group  
python process_cli.py stats
# Shows: MD file counts, quality distribution, report status
```

### Integration Health Check

```bash
# Check file flow between stages
echo "MD files generated: $(ls data/md/*.md 2>/dev/null | wc -l)"
echo "Reports generated: $(ls data/reports/*.csv 2>/dev/null | wc -l)"

# Verify Google Sheets integration
cd process_group
python sheets_uploader.py --test-connection
```

## Performance Optimization

### Search Group Optimization
- **API Keys**: Use 7 keys for maximum 700 searches/day capacity
- **Quality Threshold**: Set `--min-quality 4` for production efficiency
- **Batch Size**: Use `--count 2` for optimal quality vs. quantity balance

### Process Group Optimization  
- **Memory**: Process Group handles 1000+ files efficiently (<200MB peak)
- **Batch Processing**: Use `process-recent` for incremental updates
- **Upload Strategy**: Use `--no-upload` during development/testing

### End-to-End Performance
- **Throughput**: 300-700 companies/hour depending on API key count
- **Quality Rate**: >70% companies achieve quality score 4+
- **Success Rate**: >95% file processing success with validation

## Version Compatibility

### Component Version Matrix

| Search Group | Process Group | Compatibility | Notes |
|--------------|---------------|---------------|--------|
| v3.5.1 | v3.6.1 | ✅ Full | Current production versions |
| v3.5.0 | v3.6.1 | ✅ Compatible | Process Group handles v3.5.0 MD format |
| v3.5.1 | v3.6.0 | ✅ Compatible | Missing v3.6.1 watchlist features |

### Migration Notes
- MD file format is backward compatible across v3.5.x versions
- Process Group v3.6.1 can handle MD files from Search Group v3.5.0+
- Environment variables are additive (new keys don't break existing setup)

## Troubleshooting Quick Reference

```bash
# Common issues and solutions

# Issue: "All API keys exhausted" 
# Solution: Add more keys to .env and restart Search Group

# Issue: "觀察名單未載入"
# Solution: Ensure StockID_TWSE_TPEX.csv is in project root with correct format

# Issue: "No MD files found"  
# Solution: Run Search Group first to generate MD files

# Issue: Google Sheets upload fails
# Solution: Check GOOGLE_SHEETS_CREDENTIALS in .env

# Issue: Content validation issues
# Solution: Check Search Group v3.5.1 content validation logs
```

## Efficiency Tools Guide

This section documents the efficiency tools created to streamline testing and data management.

### Problem 1: Slow Verification Process
**Issue**: Processing all MD files takes too long when we only need to verify specific stocks (e.g., 2330, 2357).

**Solution**: `verify_stocks.py` - Quick verification tool that processes only specified stocks.

### Problem 2: Out-of-Date Data Complexity
**Issue**: Most MD files can be older than 90 days, slowing down processing and cluttering data.

**Solution**: `scripts/quarantine_files.py` - Automatically identifies and quarantines old files.

### Quick Reference

```bash
# 1. Quarantine old files (>90 days)
python scripts/quarantine_files.py --days 90 --quarantine

# 2. Quick verification for specific stocks
python verify_stocks.py 2330 2357

# 3. Check quarantine report
cat old_files_report.txt
```

### Tool 1: Quick Stock Verification (`verify_stocks.py`)

**Purpose**: Fast verification of specific stocks without processing all MD files.

**Features**:
- **Fast**: Process only specified stocks (e.g., 1 file vs full set)
- **Focused**: Immediate results for target stocks
- **Complete**: Full quality analysis and reporting
- **Save**: Results saved to `data/reports/verification/`

**Usage**:

```bash
# Verify single stock
python verify_stocks.py 2330

# Verify multiple stocks
python verify_stocks.py 2330 2357 2454

# Verify and upload to Google Sheets
python verify_stocks.py 2330 2357 --upload
```

### Tool 2: Old Files Quarantine (`scripts/quarantine_files.py`)

**Purpose**: Automatically identify and move MD files older than a specified threshold to quarantine.

**Usage**:

```bash
# Scan only (no changes)
python scripts/quarantine_files.py

# Scan with custom threshold (60 days)
python scripts/quarantine_files.py --days 60

# Actually quarantine files (with confirmation)
python scripts/quarantine_files.py --days 90 --quarantine

# Auto-confirm quarantine
echo yes | python scripts/quarantine_files.py --days 90 --quarantine
```

**Output Structure**:

```
data/quarantine/old_files/
├── 2025-02/
├── 2025-03/
├── 2025-06/
└── 2025-09/
```

### Best Practices

```bash
# Daily workflow: search → verify → process
cd search_group
python search_cli.py search --company 2330 --count 3
cd ..
python verify_stocks.py 2330 2357
python process_group/process_cli.py process

# Weekly maintenance
python scripts/quarantine_files.py --days 90 --quarantine
python verify_stocks.py 2330 2317 2454 2412 2357
python process_group/process_cli.py process
```

## Improved Search Guide (False Positive Prevention)

This section documents the improvements made to prevent false positive content validation.

### Problem Identified

Stock 2330 (台積電/TSMC) had incorrect data because:
1. MD files contained articles about other stocks where "2330" appeared as a price/target value.
2. Example: Article about stock 5269 (祥碩) with "預估目標價為2330元" was incorrectly tagged as TSMC content.
3. Content validation was too lenient and only checked if the symbol appeared anywhere in text.

### Solutions Implemented

#### 1. Enhanced Content Validation (`search_engine.py`)

**Location**: `search_group/search_engine.py` - `_validate_content()` method

**Key Improvements**:
- **Context-aware symbol matching**: Requires symbol in proper stock contexts (e.g., `2330-TW`, `代號: 2330`).
- **False positive detection**: Rejects when symbol appears as price/target value (e.g., `目標價為2330元`).
- **Stricter requirements**: Requires both symbol and company name; minimum confidence 0.8.

#### 2. False Positive Quarantine (`quarantine_false_positives.py`)

**Location**: `quarantine_false_positives.py` (project root)

**Usage**:

```bash
# Scan and report only
python quarantine_false_positives.py

# Scan specific stock
python quarantine_false_positives.py --stock 2330

# Quarantine with confirmation
python quarantine_false_positives.py --stock 2330 --quarantine

# Scan all stocks and quarantine
python quarantine_false_positives.py --quarantine
```

#### 3. Improved Search Patterns (`improved_search_patterns.py`)

**Location**: `search_group/improved_search_patterns.py`

**Key Features**:
- Tiered pattern system (FactSet direct → consensus → EPS tables → analysis)
- Stock-specific patterns to avoid false positives
- Excludes price-only matches (e.g., `-"2330元"`)

**Usage**:

```python
from improved_search_patterns import get_search_patterns_for_stock

# Get primary patterns for TSMC
patterns = get_search_patterns_for_stock('2330', '台積電', tier='primary')
```

### How to Use These Improvements

```bash
# New searches (validation is automatic)
cd search_group
python search_cli.py search --company 2330 --count 5 --min-quality 7

# Scan existing MD files for false positives
python quarantine_false_positives.py

# Re-process valid files
cd process_group
python process_cli.py process
```

For detailed troubleshooting, implementation guides, and component-specific information, refer to:
- **Search Group Details**: [search_group/instructions.md](search_group/instructions.md)
- **Process Group Details**: [process_group/instructions.md](process_group/instructions.md)

---

**System Integration Guide v3.6.1**

*This document focuses on system-level architecture and integration patterns. For detailed implementation of individual components, consult the component-specific instruction documents.*
