import re
import os
import json
from html import unescape

FILE_PATH = r"C:\Users\WJLEE\SynologyDrive\NAS\github.com\GoogleSearch.Factset\data\quarantine\inflated_quality\2025-08\2357_華碩_factset_677cedb1.md"

def clean_content(raw_content):
    """徹底清理 HTML 和 JSON 雜訊"""
    # 1. 移除 YAML header
    content = raw_content
    if content.startswith('---'):
        parts = content.split('---', 2)
        if len(parts) >= 3:
            content = parts[2]

    # 2. 處理 HTML 實體 (如 &amp;)
    content = unescape(content)

    # 3. 移除所有 <script> 和 <style> 標籤及其內容 (這是雜訊大宗)
    content = re.sub(r'<(script|style).*?>.*?</\1>', ' ', content, flags=re.DOTALL | re.IGNORECASE)

    # 4. 移除所有其餘 HTML 標籤
    content = re.sub(r'<[^>]+>', ' ', content)

    # 5. 處理 JSON 轉義字元 (例如 \" 轉為 ")
    content = content.replace('\\"', '"').replace('\\n', '\n')

    # 6. 壓縮空白
    content = ' '.join(content.split())
    return content

def extract_financials(text):
    """嘗試提取關鍵財務數據"""
    results = {}
    
    # 搜尋 EPS 模式
    eps_patterns = [
        # 針對鉅亨網標題: 華碩(2357-TW)EPS預估下修至52.43元
        r'\(?\d{4}-TW\)?\s*EPS\s*(?:預估|下修|調升|至)\s*([0-9]+\.[0-9]+)',
        # 針對鉅亨網內文: 做出2025年EPS預估：中位數由52.79元下修至52.43元
        r'(\d{4})\s*年\s*EPS\s*(?:預估|下修|調升|至|：).*?(?:至|為)\s*([0-9]+\.[0-9]+)',
        # 針對表格形式: 2025年(前值) 2026年 2027年 最高值 56.49(56.64) 58 65.85
        r'(\d{4})\s*年\s*[^|]*\|\s*([0-9]+\.[0-9]+)',
    ]
    
    # Debug: Print sentences containing EPS
    print("\n[Debug] 包含 'EPS' 的文字內容:")
    for sentence in re.findall(r'[^。，,]{5,20}EPS[^。，,]{5,50}', text):
        print(f"  - {sentence}")

    results['eps'] = []
    for pattern in eps_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for m in matches:
            results['eps'].append(f"Year {m[0]}: {m[1]}")

    # 搜尋分析師人數
    analyst_match = re.search(r'(\d+)\s*位分析師', text)
    if analyst_match:
        results['analysts'] = analyst_match.group(1)

    # 搜尋目標價
    target_match = re.search(r'目標價(?:為|:)\s*(\d+)', text)
    if target_match:
        results['target_price'] = target_match.group(1)

    return results

def main():
    if not os.path.exists(FILE_PATH):
        print(f"Error: File not found at {FILE_PATH}")
        return

    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        raw = f.read()

    print("--- 原始檔案大小: ", len(raw), " bytes ---")
    
    cleaned = clean_content(raw)
    print("\n--- 清理後的文字片段 (前 500 字) ---")
    print(cleaned[:500])
    print("\n" + "="*50)

    data = extract_financials(cleaned)
    
    print("\n[提取結果]")
    print(f"分析師人數: {data.get('analysts', '未找到')}")
    print(f"目標價: {data.get('target_price', '未找到')}")
    print("EPS 預估:")
    for eps in data.get('eps', []):
        print(f"  - {eps}")

    if not data.get('eps') and not data.get('analysts'):
        print("\n[警告] 依然無法提取數據，代表目前的 Regex 模式需要針對此格式進行調整。")
    else:
        print("\n[成功] 已成功從混亂的 HTML/JSON 中提取出數據！")

if __name__ == "__main__":
    main()
