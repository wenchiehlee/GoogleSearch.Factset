import csv
import os

# Base directory of the script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FACTSET_PATH = os.path.join(BASE_DIR, "data", "reports", "raw_factset_detailed_report.csv")

# Key companies to focus on
SYMBOLS = ["2330", "2317", "2454", "2382", "2308"]

def main():
    if not os.path.exists(FACTSET_PATH):
        print(f"Error: {FACTSET_PATH} not found. Please run the FactSet pipeline first.")
        return

    latest_consensus = {}

    try:
        with open(FACTSET_PATH, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # print(f"DEBUG: Headers found: {reader.fieldnames}")
            for row in reader:
                symbol = row.get("代號")
                if not symbol or symbol not in SYMBOLS:
                    continue
                
                # MD日期 (Report Date) and 搜尋日期 (Fetch Date)
                report_date = row.get("MD日期", "")
                search_date = row.get("搜尋日期", "")
                
                if symbol not in latest_consensus:
                    latest_consensus[symbol] = row
                else:
                    # Logic: Prioritize the most recent report date, then the most recent fetch
                    current = latest_consensus[symbol]
                    if report_date > current["MD日期"]:
                        latest_consensus[symbol] = row
                    elif report_date == current["MD日期"] and search_date > current["搜尋日期"]:
                        latest_consensus[symbol] = row
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Print Summary Table
    header = f"{'Code':<6} {'Name':<10} {'Analyst':<8} {'Target':<8} {'2026 EPS':<10} {'2027 EPS':<10} {'2028 EPS':<10}"
    print("\n" + "=" * 75)
    print("FactSet Consensus Summary (Latest Reports)")
    print("=" * 75)
    print(header)
    print("-" * 75)
    
    for symbol in SYMBOLS:
        if symbol in latest_consensus:
            r = latest_consensus[symbol]
            name = r.get("名稱", "N/A")
            analysts = r.get("分析師數量", "N/A")
            target = r.get("目標價", "N/A")
            eps26 = r.get("2026EPS平均值", "N/A")
            eps27 = r.get("2027EPS平均值", "N/A")
            eps28 = r.get("2028EPS平均值", "N/A")
            
            print(f"{symbol:<6} {name:<10} {analysts:<8} {target:<8} {eps26:<10} {eps27:<10} {eps28:<10}")
        else:
            print(f"{symbol:<6} {'Not Found':<10}")
    print("=" * 75 + "\n")

if __name__ == "__main__":
    main()
