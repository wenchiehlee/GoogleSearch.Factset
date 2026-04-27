#!/usr/bin/env python3
"""
Quarantine MD Files Script
Moves problematic MD files to quarantine directory

DEFAULT BEHAVIOR (no flags):
  - CSV-based detection: Uses raw_factset_detailed_report.csv
  - Checks ONLY: quality_score >= 7.6 AND missing revenue/EPS data (truly inflated)
  - Does NOT check: age, low quality (unless --days or --max-quality added)

Detection Methods:
  1. CSV-based (RECOMMENDED, default): Fast, uses raw_factset_detailed_report.csv
     - Criteria: quality_score >= 7.6 AND missing revenue/EPS data
     - Files with high quality AND actual data are NOT flagged (legitimate)
  2. File-based (--no-csv): Direct MD file parsing
     - Also checks: inconsistent quality metadata

OPTIONAL Filters (must explicitly add):
  - Age filter (--days X): Quarantine files older than X days
  - Quality filter (--max-quality N): Quarantine files with score <= N

Usage:
    python quarantine_files.py                        # CSV: inflated quality ONLY
    python quarantine_files.py --quarantine           # Actually move files
    python quarantine_files.py --no-csv               # File-based: inflated + inconsistent
    python quarantine_files.py --days 90              # ADD age filter (>90 days)
    python quarantine_files.py --max-quality 5        # ADD quality filter (≤5)
    python quarantine_files.py --days 90 --quarantine # Age filter + quarantine
"""

import os
import sys
import re
import shutil
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from urllib.parse import unquote

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


class OldFileQuarantiner:
    """Quarantine MD files older than specified threshold"""

    def __init__(self, days_threshold: int = None, max_quality: float = None):
        self.data_dir = Path("data/md")
        self.quarantine_base_dir = Path("data/quarantine")
        self.quarantine_base_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories for different quarantine reasons
        self.quarantine_dirs = {
            'old': self.quarantine_base_dir / 'old',
            'inflated_quality': self.quarantine_base_dir / 'inflated_quality',
            'inconsistent': self.quarantine_base_dir / 'inconsistent',
            'low_quality': self.quarantine_base_dir / 'low_quality'
        }

        # Create all subdirectories
        for qdir in self.quarantine_dirs.values():
            qdir.mkdir(parents=True, exist_ok=True)

        self.days_threshold = days_threshold
        self.max_quality = max_quality

        # Only calculate cutoff_date if days_threshold is specified
        if self.days_threshold is not None:
            self.cutoff_date = datetime.now() - timedelta(days=days_threshold)
            print(f"[INFO] Age threshold: {days_threshold} days")
            print(f"[INFO] Cutoff date: {self.cutoff_date.strftime('%Y-%m-%d')}")
        else:
            self.cutoff_date = None

        if self.max_quality is not None:
            print(f"[INFO] Quality threshold: {self.max_quality}")

        # Build filter description
        filters = []
        if self.cutoff_date is not None:
            filters.append(f"older than {days_threshold} days")
        if self.max_quality is not None:
            filters.append(f"quality <= {self.max_quality}")

        if filters:
            print(f"[INFO] Quarantine criteria: {' OR '.join(filters)}")
        else:
            print(f"[INFO] No age/quality filters - will check for inflated/inconsistent quality scores only")

        # Always check for inflated/inconsistent scores
        print(f"[INFO] Always checking: inflated quality scores, inconsistent metadata\n")

    def extract_md_date(self, filepath: Path) -> Tuple[datetime, str]:
        """Extract md_date from MD file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read(2000)  # Read first 2000 chars for metadata

            # Look for md_date in YAML frontmatter
            md_date_match = re.search(r'md_date:\s*(\d{4})/(\d{1,2})/(\d{1,2})', content)
            if md_date_match:
                year, month, day = md_date_match.groups()
                date_str = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
                date_obj = datetime(int(year), int(month), int(day))
                return date_obj, date_str

            # Fallback: Look for extracted_date
            extracted_match = re.search(r'extracted_date:\s*(\d{4})-(\d{2})-(\d{2})', content)
            if extracted_match:
                year, month, day = extracted_match.groups()
                date_str = f"{year}/{month}/{day}"
                date_obj = datetime(int(year), int(month), int(day))
                return date_obj, date_str

            # Fallback: Use file modification time
            mtime = filepath.stat().st_mtime
            date_obj = datetime.fromtimestamp(mtime)
            date_str = date_obj.strftime('%Y/%m/%d')
            return date_obj, f"{date_str} (file mtime)"

        except Exception as e:
            print(f"[ERROR] Failed to extract date from {filepath.name}: {e}")
            # Return very old date to be safe
            return datetime(2020, 1, 1), "2020/01/01 (error)"

    def extract_quality_score(self, filepath: Path) -> tuple[float, bool]:
        """
        Extract quality_score or 品質評分 from MD file
        Returns: (quality_score, is_consistent)
        - is_consistent=False if both fields exist but have different values
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read(2000)

            # Extract both field names if they exist
            en_match = re.search(r'quality_score:\s*([0-9]+(?:\.[0-9]+)?)', content)
            zh_match = re.search(r'品質評分:\s*([0-9]+(?:\.[0-9]+)?)', content)

            en_score = float(en_match.group(1)) if en_match else None
            zh_score = float(zh_match.group(1)) if zh_match else None

            # Check for inconsistency
            if en_score is not None and zh_score is not None:
                if abs(en_score - zh_score) > 0.01:  # Allow tiny float differences
                    print(f"[WARNING] Inconsistent quality scores in {filepath.name}: "
                          f"quality_score={en_score}, 品質評分={zh_score}")
                    return (en_score, False)  # Return English version but mark as inconsistent

            # Return whichever score exists
            if en_score is not None:
                return (en_score, True)
            if zh_score is not None:
                return (zh_score, True)

        except Exception as e:
            print(f"[ERROR] Failed to extract quality score from {filepath.name}: {e}")
        return (-1.0, True)

    def extract_stock_info(self, filename: str) -> Tuple[str, str]:
        """Extract stock code and company name from filename"""
        match = re.match(r'(\d{4})_([^_]+)_', filename)
        if match:
            return match.group(1), match.group(2)
        return "Unknown", "Unknown"

    def has_actual_data(self, filepath: Path) -> bool:
        """
        Check if file has actual FactSet data (EPS, analysts, target price)
        Returns True if data found, False if missing

        IMPROVED: Strips HTML to avoid false positives from HTML garbage
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            # Skip YAML frontmatter (only search actual content)
            if content.startswith('---'):
                parts = content.split('---', 2)
                if len(parts) >= 3:
                    content = parts[2]  # Content after second ---

            # REMOVED: Aggressive cutting of HTML which deleted valid content
            # Instead, we strip tags but KEEP the text inside
            
            # Use regex to strip all HTML tags
            content_clean = re.sub(r'<[^>]+>', ' ', content)
            # Normalize whitespace
            content_clean = ' '.join(content_clean.split())

            # More specific EPS patterns (avoid HTML false positives)
            eps_patterns = [
                r'(?:FY|20\d{2})\s*EPS.*?NT?\$?\s*(\d+\.?\d*)',  # EPS with year prefix
                r'EPS.*?[預估值].*?(\d+\.?\d*)',  # EPS with forecast keywords
                r'每股盈餘.*?(\d+\.?\d*)\s*元',  # Chinese EPS with units
            ]

            # More specific analyst patterns
            analyst_patterns = [
                r'(\d+)\s*位分析師',  # X analysts (Chinese)
                r'分析師共\s*(\d+)',  # Total X analysts
                r'(\d+)\s*analysts?\s+covering',  # X analysts covering
            ]

            # More specific target price patterns
            target_patterns = [
                r'目標價.*?NT?\$?\s*(\d+\.?\d*)\s*元',  # Target price with units
                r'target\s+price.*?NT?\$?\s*(\d+\.?\d*)',  # English target price
                r'平均目標價.*?(\d+\.?\d*)',  # Average target price
            ]

            has_eps = any(re.search(pattern, content_clean, re.IGNORECASE) for pattern in eps_patterns)
            has_analysts = any(re.search(pattern, content_clean, re.IGNORECASE) for pattern in analyst_patterns)
            has_target = any(re.search(pattern, content_clean, re.IGNORECASE) for pattern in target_patterns)

            # Check for FactSet content in article (not just metadata)
            # Require it to be near financial terms
            has_factset = bool(re.search(r'FactSet.*?(?:預估|分析|目標價|EPS|盈餘)', content_clean, re.IGNORECASE)) or \
                         bool(re.search(r'(?:預估|分析|目標價|EPS|盈餘).*?FactSet', content_clean, re.IGNORECASE))

            return has_eps or has_analysts or has_target or has_factset

        except Exception as e:
            print(f"[ERROR] Failed to check data in {filepath.name}: {e}")
            return True  # Assume has data if we can't check

    def extract_all_info(self, filepath: Path) -> Dict:
        """
        Optimized: Extract all information from MD file in ONE read
        Returns: dict with date_obj, date_str, quality_score, is_consistent, has_data
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()  # Read file ONCE

            # Extract date (from first 2000 chars)
            header = content[:2000]

            # Look for md_date in YAML frontmatter
            md_date_match = re.search(r'md_date:\s*(\d{4})/(\d{1,2})/(\d{1,2})', header)
            if md_date_match:
                year, month, day = md_date_match.groups()
                date_str = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
                date_obj = datetime(int(year), int(month), int(day))
            else:
                # Fallback: Look for extracted_date
                extracted_match = re.search(r'extracted_date:\s*(\d{4})-(\d{2})-(\d{2})', header)
                if extracted_match:
                    year, month, day = extracted_match.groups()
                    date_str = f"{year}/{month}/{day}"
                    date_obj = datetime(int(year), int(month), int(day))
                else:
                    # Fallback: Use file modification time
                    mtime = filepath.stat().st_mtime
                    date_obj = datetime.fromtimestamp(mtime)
                    date_str = date_obj.strftime('%Y/%m/%d') + " (file mtime)"

            # Extract quality scores
            en_match = re.search(r'quality_score:\s*([0-9]+(?:\.[0-9]+)?)', header)
            zh_match = re.search(r'品質評分:\s*([0-9]+(?:\.[0-9]+)?)', header)

            en_score = float(en_match.group(1)) if en_match else None
            zh_score = float(zh_match.group(1)) if zh_match else None

            # Check for inconsistency
            is_consistent = True
            if en_score is not None and zh_score is not None:
                if abs(en_score - zh_score) > 0.01:
                    is_consistent = False

            # Get final quality score
            quality_score = en_score if en_score is not None else (zh_score if zh_score is not None else -1.0)

            # Check for actual data (IMPROVED: strip HTML first)
            
            # Check for actual data (Use the improved has_actual_data method)
            has_data = self.has_actual_data(filepath)

            return {
                'date_obj': date_obj,
                'date_str': date_str,
                'quality_score': quality_score,
                'is_consistent': is_consistent,
                'has_data': has_data
            }

        except Exception as e:
            print(f"[ERROR] Failed to extract info from {filepath.name}: {e}")
            return {
                'date_obj': datetime(2020, 1, 1),
                'date_str': "2020/01/01 (error)",
                'quality_score': -1.0,
                'is_consistent': True,
                'has_data': True
            }

    def _get_quarantine_reasons(self, date_obj: datetime, quality_score: float, is_consistent: bool, has_data: bool = True) -> List[str]:
        reasons = []
        # Check for data inconsistency first
        if not is_consistent:
            reasons.append("inconsistent_quality")
        # Check for inflated quality score (high score but no actual data)
        if quality_score >= 7.5 and not has_data:
            reasons.append("inflated_quality")
        # Only check age if days_threshold was specified
        if self.cutoff_date is not None and date_obj < self.cutoff_date:
            reasons.append("old")
        # Only check quality if max_quality was specified
        if self.max_quality is not None and quality_score >= 0 and quality_score <= self.max_quality:
            reasons.append("low_quality")
        return reasons

    def scan_from_csv(self, csv_path: str = 'data/reports/raw_factset_detailed_report.csv') -> List[Dict]:
        """
        CSV-based detection (RECOMMENDED): Much faster and more reliable

        Quarantine files with:
        - quality_score >= 7.5 AND missing revenue/EPS data (truly inflated)

        This approach is simpler than parsing MD files and uses already-processed data.
        """
        try:
            import pandas as pd
        except ImportError:
            print("[ERROR] pandas not installed. Install with: pip install pandas")
            print("[INFO] Falling back to file-based scanning...")
            return self.scan_old_files()

        csv_path = Path(csv_path)
        if not csv_path.exists():
            print(f"[WARNING] CSV report not found: {csv_path}")
            print("[INFO] Falling back to file-based scanning...")
            return self.scan_old_files()

        print(f"[INFO] Using CSV-based detection: {csv_path}")
        print(f"[INFO] Criteria: quality_score >= 7.5 AND (missing revenue OR missing EPS)\n")

        # Read CSV
        df = pd.read_csv(csv_path, encoding='utf-8-sig')

        # Define data columns to check
        revenue_cols = ['2025營收平均值', '2026營收平均值', '2027營收平均值', '2028營收平均值']
        eps_cols = ['2025EPS平均值', '2026EPS平均值', '2027EPS平均值', '2028EPS平均值']

        # Find files with high quality scores
        high_quality = df[df['品質評分'] >= 7.5].copy()

        # Check if they actually have data
        # A file has data only if it has BOTH revenue AND EPS (quarantine if either is missing)
        high_quality['has_revenue'] = high_quality[revenue_cols].notna().any(axis=1)
        high_quality['has_eps'] = high_quality[eps_cols].notna().any(axis=1)
        high_quality['has_data'] = high_quality['has_revenue'] & high_quality['has_eps']

        # Only flag files with high quality BUT no actual data (truly inflated)
        inflated = high_quality[~high_quality['has_data']]

        print(f"[INFO] Found {len(high_quality)} files with quality >= 7.5")
        print(f"[INFO] Of these, {len(inflated)} have missing data (truly inflated)")
        print(f"[INFO] Skipping {len(high_quality) - len(inflated)} files with legitimate high quality\n")

        # Also check for low quality files if max_quality is specified
        low_quality_files = pd.DataFrame()
        if self.max_quality is not None:
            low_quality_files = df[df['品質評分'] <= self.max_quality].copy()
            print(f"[INFO] Found {len(low_quality_files)} files with quality <= {self.max_quality} (low quality)\n")

        results = []

        # Process inflated quality files
        for idx, row in inflated.iterrows():
            stock_code = row['代號']
            company_name = row['名稱']
            quality_score = row['品質評分']
            md_date = str(row['MD日期'])
            md_file_url = str(row['MD File']) if pd.notna(row.get('MD File')) else None

            # Extract filename from URL if available
            target_filename = None
            if md_file_url and 'data/md/' in md_file_url:
                # Extract filename from URL: ...data/md/2301_光寶科_factset_83089811.md
                target_filename = md_file_url.split('data/md/')[-1]
                # URL-decode the filename to handle Chinese characters
                target_filename = unquote(target_filename)

            # Find matching MD file
            if target_filename:
                # Try exact match first
                md_file = self.data_dir / target_filename
                if not md_file.exists():
                    # Fallback to glob if exact match not found
                    md_files = list(self.data_dir.glob(f'{stock_code}_*_factset_*.md'))
                    md_file = md_files[0] if md_files else None
            else:
                # No URL in CSV, use glob
                md_files = list(self.data_dir.glob(f'{stock_code}_*_factset_*.md'))
                md_file = md_files[0] if md_files else None

            if not md_file:
                continue  # Skip if file not found

            # Parse date for directory organization
            if pd.notna(md_date) and len(md_date) >= 10:
                try:
                    date_obj = datetime.strptime(md_date, '%Y-%m-%d')
                    date_str = date_obj.strftime('%Y/%m/%d')
                except:
                    date_obj = datetime.now()
                    date_str = date_obj.strftime('%Y/%m/%d')
            else:
                date_obj = datetime.now()
                date_str = date_obj.strftime('%Y/%m/%d')

            # Calculate age in days
            age_days = (datetime.now() - date_obj).days

            results.append({
                'filepath': md_file,
                'filename': md_file.name,
                'stock_code': stock_code,
                'company_name': company_name,
                'md_date': date_str,
                'date_obj': date_obj,
                'age_days': age_days,
                'quality_score': quality_score,
                'has_data': False,  # These are confirmed inflated (high score, no data)
                'reasons': ['inflated_quality']
            })

        # Process low quality files
        for idx, row in low_quality_files.iterrows():
            stock_code = row['代號']
            company_name = row['名稱']
            quality_score = row['品質評分']
            md_date = str(row['MD日期'])
            md_file_url = str(row['MD File']) if pd.notna(row.get('MD File')) else None

            # Extract filename from URL if available
            target_filename = None
            if md_file_url and 'data/md/' in md_file_url:
                target_filename = md_file_url.split('data/md/')[-1]
                # URL-decode the filename to handle Chinese characters
                target_filename = unquote(target_filename)

            # Find matching MD file
            if target_filename:
                md_file = self.data_dir / target_filename
                if not md_file.exists():
                    md_files = list(self.data_dir.glob(f'{stock_code}_*_factset_*.md'))
                    md_file = md_files[0] if md_files else None
            else:
                md_files = list(self.data_dir.glob(f'{stock_code}_*_factset_*.md'))
                md_file = md_files[0] if md_files else None

            if not md_file:
                continue

            # Parse date
            if pd.notna(md_date) and len(md_date) >= 10:
                try:
                    date_obj = datetime.strptime(md_date, '%Y-%m-%d')
                    date_str = date_obj.strftime('%Y/%m/%d')
                except:
                    date_obj = datetime.now()
                    date_str = date_obj.strftime('%Y/%m/%d')
            else:
                date_obj = datetime.now()
                date_str = date_obj.strftime('%Y/%m/%d')

            age_days = (datetime.now() - date_obj).days

            results.append({
                'filepath': md_file,
                'filename': md_file.name,
                'stock_code': stock_code,
                'company_name': company_name,
                'md_date': date_str,
                'date_obj': date_obj,
                'age_days': age_days,
                'quality_score': quality_score,
                'has_data': True,  # Doesn't matter for low quality
                'reasons': ['low_quality']
            })

        print(f"[INFO] CSV scan completed. Found {len(results)} files to quarantine\n")
        return results

    def scan_old_files(self) -> List[Dict]:
        """
        File-based detection: Scans MD files directly (slower but works without CSV)

        For better performance and reliability, use scan_from_csv() instead.
        """
        results = []

        md_files = list(self.data_dir.glob("*.md"))
        total_files = len(md_files)
        print(f"[INFO] Scanning {total_files} MD files...\n")

        # Progress tracking
        progress_interval = max(10, total_files // 10)  # Show progress every 10% or every 10 files
        start_time = datetime.now()

        for idx, filepath in enumerate(md_files, 1):
            # Show progress indicators
            if idx % progress_interval == 0 or idx == total_files:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = idx / elapsed if elapsed > 0 else 0
                eta = (total_files - idx) / rate if rate > 0 else 0
                print(f"[PROGRESS] {idx}/{total_files} files ({idx*100//total_files}%) | "
                      f"{rate:.1f} files/sec | ETA: {eta:.0f}s")

            # Extract all info in ONE read (optimized!)
            info = self.extract_all_info(filepath)
            date_obj = info['date_obj']
            date_str = info['date_str']
            quality_score = info['quality_score']
            is_consistent = info['is_consistent']
            has_data = info['has_data']

            # Check quarantine reasons
            reasons = self._get_quarantine_reasons(date_obj, quality_score, is_consistent, has_data)

            if reasons:
                stock_code, company_name = self.extract_stock_info(filepath.name)
                age_days = (datetime.now() - date_obj).days

                results.append({
                    'filepath': filepath,
                    'filename': filepath.name,
                    'stock_code': stock_code,
                    'company_name': company_name,
                    'md_date': date_str,
                    'date_obj': date_obj,
                    'age_days': age_days,
                    'quality_score': quality_score,
                    'has_data': has_data,
                    'reasons': reasons
                })

        # Final summary
        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\n[INFO] Scan completed in {total_time:.1f}s ({total_files/total_time:.1f} files/sec)")
        print(f"[INFO] Found {len(results)} files matching quarantine criteria\n")

        return results

    def generate_report(self, results: List[Dict]) -> str:
        """Generate detailed report of old files"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("QUARANTINE REPORT")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if self.cutoff_date is not None:
            report_lines.append(f"Age threshold: {self.days_threshold} days (cutoff: {self.cutoff_date.strftime('%Y-%m-%d')})")
        if self.max_quality is not None:
            report_lines.append(f"Quality threshold: <= {self.max_quality}")
        report_lines.append("=" * 80)
        report_lines.append("")

        if not results:
            report_lines.append("No old files found!")
            return '\n'.join(report_lines)

        report_lines.append(f"Total old files found: {len(results)}")
        report_lines.append("")

        # Group by stock code
        by_stock = {}
        for result in results:
            stock = result['stock_code']
            if stock not in by_stock:
                by_stock[stock] = []
            by_stock[stock].append(result)

        # Sort by stock code
        for stock, items in sorted(by_stock.items()):
            report_lines.append(f"\nStock: {stock} ({items[0]['company_name']})")
            report_lines.append(f"  Old files: {len(items)}")
            report_lines.append("")

            # Sort by age (oldest first)
            items.sort(key=lambda x: x['age_days'], reverse=True)

            for item in items[:5]:  # Show first 5 oldest
                report_lines.append(f"  File: {item['filename']}")
                report_lines.append(f"    Date: {item['md_date']}")
                report_lines.append(f"    Age: {item['age_days']} days")
                report_lines.append(f"    Quality: {item['quality_score']}")
                report_lines.append(f"    Reasons: {', '.join(item['reasons'])}")
                report_lines.append("")

            if len(items) > 5:
                report_lines.append(f"  ... and {len(items) - 5} more files")
                report_lines.append("")

        # Summary statistics
        report_lines.append("\n" + "=" * 80)
        report_lines.append("SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"Total stocks affected: {len(by_stock)}")
        report_lines.append(f"Total files to quarantine: {len(results)}")
        report_lines.append(f"Average age: {sum(r['age_days'] for r in results) / len(results):.1f} days")
        if self.max_quality is not None:
            low_quality_count = sum(1 for r in results if "low_quality" in r['reasons'])
            report_lines.append(f"Low quality files: {low_quality_count}")
        inflated_count = sum(1 for r in results if "inflated_quality" in r['reasons'])
        if inflated_count > 0:
            report_lines.append(f"Inflated quality scores (high score, no data): {inflated_count}")
        inconsistent_count = sum(1 for r in results if "inconsistent_quality" in r['reasons'])
        if inconsistent_count > 0:
            report_lines.append(f"Inconsistent quality scores: {inconsistent_count}")
        oldest = max(results, key=lambda x: x['age_days'])
        report_lines.append(f"Oldest file: {oldest['filename']} ({oldest['age_days']} days)")

        return '\n'.join(report_lines)

    def quarantine_files(self, results: List[Dict]) -> int:
        """Move files to quarantine, organized by reason"""
        moved_count = 0

        # Track counts by reason
        reason_counts = {reason: 0 for reason in self.quarantine_dirs.keys()}

        # Deduplicate by filepath to avoid moving the same file multiple times
        seen_files = {}
        for result in results:
            filepath_str = str(result['filepath'])
            if filepath_str not in seen_files:
                seen_files[filepath_str] = result
            # If duplicate, keep the one with the most recent date
            elif result['date_obj'] > seen_files[filepath_str]['date_obj']:
                seen_files[filepath_str] = result

        print(f"[INFO] Deduplicated {len(results)} entries to {len(seen_files)} unique files\n")

        for result in seen_files.values():
            filepath = result['filepath']
            date_obj = result['date_obj']
            reasons = result['reasons']

            # Determine primary quarantine reason (priority order)
            # Priority: inconsistent > inflated_quality > low_quality > old
            primary_reason = None
            if 'inconsistent_quality' in reasons:
                primary_reason = 'inconsistent'
            elif 'inflated_quality' in reasons:
                primary_reason = 'inflated_quality'
            elif 'low_quality' in reasons:
                primary_reason = 'low_quality'
            elif 'old' in reasons:
                primary_reason = 'old'

            if primary_reason:
                # Get the appropriate quarantine directory
                base_dir = self.quarantine_dirs[primary_reason]

                # Create subdirectory by year-month within the reason directory
                month_dir = base_dir / date_obj.strftime('%Y-%m')
                month_dir.mkdir(exist_ok=True)

                dest_path = month_dir / filepath.name

                try:
                    shutil.move(str(filepath), str(dest_path))
                    reason_counts[primary_reason] += 1
                    print(f"[OK] Moved: {filepath.name} -> {primary_reason}/{month_dir.name}/")
                    moved_count += 1
                except Exception as e:
                    print(f"[ERROR] Failed to move {filepath.name}: {e}")

        # Print summary
        print(f"\n[SUMMARY] Files moved by reason:")
        for reason, count in reason_counts.items():
            if count > 0:
                print(f"  {reason}: {count} files")

        return moved_count


def main():
    parser = argparse.ArgumentParser(
        description='Quarantine problematic MD files with CSV-based detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DEFAULT BEHAVIOR (no flags):
  python quarantine_files.py                 # CSV-based: truly inflated quality ONLY

  What it checks:
  - Inflated quality scores (score >= 7.5 BUT missing revenue OR missing EPS)
  - Source: data/reports/raw_factset_detailed_report.csv
  - Files with high quality AND both revenue+EPS are skipped (legitimate)

  What it does NOT check (unless explicitly added):
  - Age-based filtering (no --days flag)
  - Low quality filtering (no --max-quality flag)

Examples:
  python quarantine_files.py                 # Dry-run: Check truly inflated quality
  python quarantine_files.py --quarantine    # Move files: truly inflated quality
  python quarantine_files.py --no-csv        # File-based: inflated + inconsistent
  python quarantine_files.py --days 60       # CSV + ADD age filter (>60 days)
  python quarantine_files.py --max-quality 5 # CSV + ADD quality filter (≤5)
  python quarantine_files.py --days 180 --quarantine  # Age + quality + move

CSV-based detection (DEFAULT):
  - Uses: data/reports/raw_factset_detailed_report.csv
  - Checks: quality_score >= 7.5 AND (missing revenue OR missing EPS)
  - Skips: Files with high quality AND both revenue+EPS (legitimate)
  - Fast, reliable, uses already-processed data
  - Perfect for daily automation (no false positives)
        """
    )

    parser.add_argument('--quarantine', action='store_true',
                       help='Actually move files to quarantine (default: report only)')
    parser.add_argument('--no-csv', action='store_true',
                       help='Use file-based detection instead of CSV (slower)')
    parser.add_argument('--days', type=int, default=None,
                       help='Age threshold in days (omit to ignore age)')
    parser.add_argument('--max-quality', type=float, default=None,
                       help='Quarantine files with quality_score <= max-quality')
    parser.add_argument('--output', type=str, default='old_files_report.txt',
                       help='Output report filename')
    parser.add_argument('--yes', action='store_true',
                       help='Skip confirmation prompt (auto-confirm)')

    args = parser.parse_args()

    print("=" * 80)
    print("QUARANTINE TOOL - FactSet Pipeline")
    print("=" * 80)
    print()

    # Scan for problematic files
    quarantiner = OldFileQuarantiner(days_threshold=args.days, max_quality=args.max_quality)

    # Use CSV-based detection by default (faster, more reliable)
    if args.no_csv:
        print("[INFO] Using file-based detection (--no-csv flag)\n")
        results = quarantiner.scan_old_files()
    else:
        results = quarantiner.scan_from_csv()

    # Generate report
    report = quarantiner.generate_report(results)
    print(report)

    # Save report to file
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\n[OK] Report saved to: {args.output}")

    # Quarantine if requested
    if args.quarantine and results:
        print()
        print("=" * 80)
        
        if args.yes:
            response = 'yes'
        else:
            response = input(f"Quarantine {len(results)} old files? (yes/no): ")

        if response.lower() in ['yes', 'y']:
            moved = quarantiner.quarantine_files(results)
            print(f"\n[OK] Quarantined {moved} files to: {quarantiner.quarantine_base_dir}/")
            print(f"[INFO] Files organized by reason: old/, inflated_quality/, inconsistent/, low_quality/")
        else:
            print("[INFO] Quarantine cancelled.")
    elif results and not args.quarantine:
        print()
        print("=" * 80)
        print("[INFO] Run with --quarantine flag to move these files.")

    print()
    print("[OK] Done!")


if __name__ == "__main__":
    main()
