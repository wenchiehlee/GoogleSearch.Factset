
import os
import shutil
import re
from pathlib import Path

def has_actual_data(filepath: Path) -> bool:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        if content.startswith('---'):
            parts = content.split('---', 2)
            if len(parts) >= 3:
                content = parts[2]

        content_clean = re.sub(r'<[^>]+>', ' ', content)
        content_clean = ' '.join(content_clean.split())

        eps_patterns = [
            r'(?:FY|20\d{2})\s*EPS.*?NT?\$?\s*(\d+\.?\d*)',
            r'EPS.*?[預估值].*?(\d+\.?\d*)',
            r'每股盈餘.*?(\d+\.?\d*)\s*元',
        ]
        
        rev_patterns = [
            r'(?:FY|20\d{2})\s*Revenue.*?(\d+\.?\d*)',
            r'營收.*?[預估值].*?(\d+\.?\d*)',
            r'營業收入.*?(\d+\.?\d*)\s*[億萬]?',
        ]

        has_eps = any(re.search(p, content_clean, re.IGNORECASE) for p in eps_patterns)
        has_rev = any(re.search(p, content_clean, re.IGNORECASE) for p in rev_patterns)

        if not (has_eps or has_rev):
            if 'table' in content.lower() and '|' in content:
                return True
            return False

        return True
    except Exception:
        return False

def restore_2026_files():
    quarantine_base = Path('data/quarantine')
    target_dir = Path('data/md')
    target_dir.mkdir(parents=True, exist_ok=True)
    
    count = 0
    # Search in all subdirectories of quarantine for 2026 folders
    for root, dirs, files in os.walk(quarantine_base):
        for d in dirs:
            if d.startswith('2026-'):
                folder_path = Path(root) / d
                print(f"Checking folder: {folder_path}")
                for file in folder_path.glob('*.md'):
                    if has_actual_data(file):
                        # print(f"  ✅ Restoring {file.name}")
                        shutil.move(str(file), str(target_dir / file.name))
                        count += 1
    
    print(f"\nDone! Restored {count} valid 2026 files to {target_dir}")

if __name__ == "__main__":
    restore_2026_files()
