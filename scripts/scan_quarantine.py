#!/usr/bin/env python3
"""
Scan quarantined MD files to check if any were incorrectly quarantined
"""

import os
import sys
from pathlib import Path

# Add process_group to path
sys.path.insert(0, 'process_group')

from md_parser import MDParser

def scan_quarantine_files():
    """Scan all quarantined MD files and report quality scores"""

    quarantine_dir = Path('data/quarantine')
    parser = MDParser()

    # Find all MD files in quarantine
    md_files = list(quarantine_dir.glob('**/*.md'))

    print(f"Found {len(md_files)} quarantined MD files")
    print("\n" + "="*80)
    print("QUARANTINE SCAN RESULTS")
    print("="*80 + "\n")

    # Track statistics
    should_restore = []
    correctly_quarantined = []

    for md_file in sorted(md_files):
        try:
            # Parse the file
            result = parser.parse_md_file(str(md_file))

            if result:
                quality_score = result.get('quality_score', 0)
                stock_code = result.get('stock_code', 'unknown')
                company_name = result.get('company_name', 'unknown')
                md_date = result.get('md_date', 'unknown')

                # Check if this file should be restored (quality > 7.4)
                if quality_score > 7.4:
                    should_restore.append({
                        'file': md_file,
                        'stock_code': stock_code,
                        'company_name': company_name,
                        'quality_score': quality_score,
                        'md_date': md_date,
                        'reason': f'Quality {quality_score} > 7.4 threshold'
                    })
                    print(f"❌ SHOULD RESTORE: {md_file.name}")
                    print(f"   {stock_code} - {company_name}")
                    print(f"   Quality: {quality_score} (> 7.4)")
                    print(f"   Date: {md_date}")
                    print(f"   Path: {md_file.relative_to('data/quarantine')}")
                    print()
                else:
                    correctly_quarantined.append({
                        'file': md_file,
                        'quality_score': quality_score
                    })

        except Exception as e:
            print(f"⚠️ Error parsing {md_file.name}: {e}")
            continue

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total quarantined files: {len(md_files)}")
    print(f"Correctly quarantined (quality <= 7.4): {len(correctly_quarantined)}")
    print(f"Should restore (quality > 7.4): {len(should_restore)}")

    if should_restore:
        print("\n" + "="*80)
        print("FILES TO RESTORE")
        print("="*80)
        for item in should_restore:
            print(f"\n{item['stock_code']} - {item['company_name']}")
            print(f"  File: {item['file'].name}")
            print(f"  Quality: {item['quality_score']}")
            print(f"  Date: {item['md_date']}")
            print(f"  Reason: {item['reason']}")

    return should_restore

if __name__ == "__main__":
    should_restore = scan_quarantine_files()

    if should_restore:
        print(f"\n⚠️ Found {len(should_restore)} files that should be restored!")
        sys.exit(1)
    else:
        print("\n✅ All quarantined files are correctly quarantined (quality <= 7.4)")
        sys.exit(0)
