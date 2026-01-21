
# FactSet Pipeline v3.6.1 - CSV 報告使用指南
生成時間: 2026-01-21 13:25:40
時間戳: 20260121_132540

## 📁 生成的檔案清單

### 主要報告檔案
- portfolio: `portfolio_summary_20260121_132540.csv`
- detailed: `detailed_report_20260121_132540.csv`
- keyword: `factset_query_pattern_summary_latest.csv`
- watchlist: `watchlist_summary_latest.csv`
- validation: `validation_summary_latest.csv`


### 最新版本檔案 (無時間戳)
- `factset_portfolio_summary_latest.csv` - 投資組合摘要
- `factset_detailed_report_latest.csv` - 詳細報告
- `factset_query_pattern_summary_latest.csv` - 查詢模式統計 (如有)
- `watchlist_summary_latest.csv` - 觀察名單統計 (如有)
- `validation_summary_latest.csv` - 驗證摘要

## 🔧 Google Sheets 手動上傳步驟

### 方法 1: 檔案匯入
1. 開啟 Google Sheets
2. 建立新的試算表或開啟現有試算表
3. 點選「檔案」→「匯入」
4. 上傳 CSV 檔案
5. 選擇「建立新工作表」
6. 重複步驟 3-5 for 每個 CSV 檔案

### 方法 2: 複製貼上
1. 用 Excel 或文字編輯器開啟 CSV 檔案
2. 全選並複製內容
3. 在 Google Sheets 中建立新工作表
4. 貼上內容
5. 如果格式有問題，使用「資料」→「分欄」功能

## 📊 各報告檔案說明

### 投資組合摘要 (portfolio_summary_*.csv)
- **用途**: 每家公司的關鍵指標摘要
- **欄位**: 14 個欄位，包含代號、名稱、EPS 預測、品質評分等
- **適用**: 高階主管報告、快速概覽

### 詳細報告 (detailed_report_*.csv)
- **用途**: 所有記錄的完整詳細資訊
- **欄位**: 22 個欄位，包含所有 EPS 數據、驗證狀態、MD 檔案連結
- **適用**: 分析師深度分析、資料驗證

### 查詢模式統計 (query_pattern_summary_*.csv)
- **用途**: 搜尋查詢模式效果分析
- **欄位**: 10 個欄位，包含模式使用次數、品質評分、分類
- **適用**: 系統優化、搜尋策略改進

### 觀察名單統計 (watchlist_summary_*.csv)
- **用途**: 觀察名單公司處理狀態分析
- **欄位**: 12 個欄位，包含處理狀態、品質評分、關鍵字分析
- **適用**: 覆蓋率監控、優先處理規劃

### 驗證摘要 (validation_summary_*.csv)
- **用途**: 整體處理狀態和統計摘要
- **欄位**: 5 個欄位，包含統計項目、數值、說明
- **適用**: 系統健康度監控、處理品質評估

## 💡 使用建議

### Excel 分析
1. 使用 Excel 的樞紐分析表功能進行深度分析
2. 建立圖表視覺化呈現資料
3. 設定條件格式突出關鍵資訊

### 自動化處理
1. 可以寫程式定期處理 `*_latest.csv` 檔案
2. 建立監控儀表板讀取這些 CSV
3. 設定警報系統監控品質評分變化

### 資料整合
1. 使用 SQL 資料庫匯入這些 CSV 進行複雜查詢
2. 結合其他資料源進行交叉分析
3. 建立歷史資料趨勢分析

## ⚠️ 注意事項

### 檔案編碼
- 所有 CSV 檔案使用 UTF-8-BOM 編碼
- Excel 開啟時中文應正常顯示
- 如有亂碼，請確認編碼設定

### 資料格式
- 數值欄位已經格式化，可直接用於計算
- 日期格式為 YYYY-MM-DD
- 股票代號為純數字格式

### 更新頻率
- 時間戳版本檔案保留歷史記錄
- `*_latest.csv` 檔案總是最新版本
- 建議定期備份重要的時間戳版本

---
FactSet Pipeline v3.6.1 - CSV Only Mode
避免 Google Sheets API 限制，提供穩定可靠的輸出方案
