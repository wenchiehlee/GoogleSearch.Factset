#!/usr/bin/env python3
"""
Report Generator - FactSet Pipeline v3.6.1 (Updated for md_date)
Updated to use md_date field from Search Group metadata
Enhanced MD日期 handling with reliable metadata source
"""

import json
import os
import re
import pandas as pd
import urllib.parse
from datetime import datetime
from typing import Dict, Any, List, Optional
import pytz

class ReportGenerator:
    """報告生成器 v3.6.1-updated - 使用 Search Group 的 md_date 欄位"""

    def __init__(self, github_repo_base="https://raw.githubusercontent.com/wenchiehlee/GoogleSearch/refs/heads/main", output_dir="data/reports"):
        self.github_repo_base = github_repo_base
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 設定台北時區
        self.taipei_tz = pytz.timezone('Asia/Taipei')
        
        # 投資組合摘要欄位
        self.portfolio_summary_columns = [
            '代號', '名稱', '股票代號', 'MD最舊日期', 'MD最新日期', 'MD資料筆數',
            '分析師數量', '目標價',
            '2025EPS最高值', '2025EPS最低值', '2025EPS平均值', '2025EPS中位數',
            '2026EPS最高值', '2026EPS最低值', '2026EPS平均值', '2026EPS中位數',
            '2027EPS最高值', '2027EPS最低值', '2027EPS平均值', '2027EPS中位數',
            '2028EPS最高值', '2028EPS最低值', '2028EPS平均值', '2028EPS中位數',
            '2025營收最高值', '2025營收最低值', '2025營收平均值', '2025營收中位數',
            '2026營收最高值', '2026營收最低值', '2026營收平均值', '2026營收中位數',
            '2027營收最高值', '2027營收最低值', '2027營收平均值', '2027營收中位數',
            '2028營收最高值', '2028營收最低值', '2028營收平均值', '2028營收中位數',
            '品質評分', '狀態', 'MD日期', 'MD File', '搜尋日期', '處理日期'
        ]

        # 詳細報告欄位
        self.detailed_report_columns = [
            '代號', '名稱', '股票代號', 'MD日期', '分析師數量', '目標價',
            '2025EPS最高值', '2025EPS最低值', '2025EPS平均值', '2025EPS中位數',
            '2026EPS最高值', '2026EPS最低值', '2026EPS平均值', '2026EPS中位數',
            '2027EPS最高值', '2027EPS最低值', '2027EPS平均值', '2027EPS中位數',
            '2028EPS最高值', '2028EPS最低值', '2028EPS平均值', '2028EPS中位數',
            '2025營收最高值', '2025營收最低值', '2025營收平均值', '2025營收中位數',
            '2026營收最高值', '2026營收最低值', '2026營收平均值', '2026營收中位數',
            '2027營收最高值', '2027營收最低值', '2027營收平均值', '2027營收中位數',
            '2028營收最高值', '2028營收最低值', '2028營收平均值', '2028營收中位數',
            '品質評分', '狀態', '驗證狀態', 'MD File', '搜尋日期', '處理日期'
        ]

        # 關鍵字報告欄位 (10 欄位) - 保留傳統關鍵字分析用
        self.keyword_summary_columns = [
            '關鍵字', '使用次數', '平均品質評分', '最高品質評分', '最低品質評分',
            '相關公司數量', '品質狀態', '分類', '效果評級', '處理日期'
        ]
        
        # 查詢模式報告欄位 (10 欄位) - 新增標準化查詢模式
        self.query_pattern_summary_columns = [
            'Query pattern', '使用次數', '平均品質評分', '最高品質評分', '最低品質評分',
            '相關公司數量', '品質狀態', '分類', '效果評級', '處理日期'
        ]
        
        # 觀察名單報告欄位 (12 欄位) - v3.6.1 新增
        self.watchlist_summary_columns = [
            '公司代號', '公司名稱', 'MD檔案數量', '處理狀態', '平均品質評分', '最高品質評分',
            '搜尋關鍵字數量', '主要關鍵字', '關鍵字平均品質', '最新檔案日期', '驗證狀態', '處理日期'
        ]

    def _get_taipei_time(self) -> str:
        """取得台北時間的字串格式"""
        taipei_time = datetime.now(self.taipei_tz)
        return taipei_time.strftime('%Y-%m-%d %H:%M:%S')

    def _should_include_in_report_v351_updated(self, company_data: Dict[str, Any]) -> bool:
        """UPDATED: 判斷是否應該將此資料包含在報告中 - 使用 md_date 優先邏輯"""
        
        # 檢查 1: 基本資料完整性
        company_code = company_data.get('company_code')
        company_name = company_data.get('company_name')
        
        if not company_code or company_code == 'Unknown':
            return False
        
        if not company_name or company_name == 'Unknown':
            return False
        
        # UPDATED: md_date 檢查 - 現在 md_date 應該由 Search Group 提供
        # 不再強制排除，但會在品質評分中反映
        
        # 檢查 2: 驗證結果檢查
        validation_result = company_data.get('validation_result', {})
        validation_status = validation_result.get('overall_status', 'unknown')
        
        if validation_status == 'error':
            return False
        
        # 檢查 3: content_validation_passed 字段
        validation_passed = company_data.get('content_validation_passed', True)
        if not validation_passed:
            return False
        
        # 檢查 4: 檢查關鍵驗證錯誤
        validation_errors = company_data.get('validation_errors', [])
        if validation_errors:
            critical_error_keywords = [
                '不在觀察名單中',
                '公司名稱不符觀察名單',
                '觀察名單顯示應為',
                '愛派爾.*愛立信',
                '愛立信.*愛派爾',
                '公司代號格式無效'
            ]
            
            for error in validation_errors:
                error_str = str(error)
                if any(re.search(keyword, error_str, re.IGNORECASE) for keyword in critical_error_keywords):
                    return False
        
        # 檢查 5: 品質評分為 0 且有驗證失敗狀態的特殊情況
        quality_score = company_data.get('quality_score', 0)
        quality_status = company_data.get('quality_status', '')
        
        if quality_score == 0 and '驗證失敗' in quality_status:
            return False
        
        return True

    def generate_portfolio_summary(self, processed_companies: List[Dict], filter_invalid=True) -> pd.DataFrame:
        """UPDATED: 生成投資組合摘要 - 使用 md_date 優先邏輯"""
        try:
            # 增強過濾邏輯 - 使用更新的過濾方法
            if filter_invalid:
                original_count = len(processed_companies)
                valid_companies = []
                filtered_reasons = {
                    'validation_failed': 0,
                    'other_issues': 0
                }
                
                # UPDATED: 統計 md_date 情況
                md_date_stats = {
                    'total_files': 0,
                    'with_md_date': 0,
                    'with_content_date_fallback': 0,
                    'no_date_available': 0,
                    'search_group_coverage': 0
                }
                
                for company in processed_companies:
                    md_date_stats['total_files'] += 1
                    
                    # 檢查日期來源
                    md_date = self._get_md_date_with_priority(company)
                    md_date_source = self._get_md_date_source(company)
                    
                    if md_date_source == 'md_date':
                        md_date_stats['with_md_date'] += 1
                        md_date_stats['search_group_coverage'] += 1
                    elif md_date_source == 'content_date':
                        md_date_stats['with_content_date_fallback'] += 1
                    else:
                        md_date_stats['no_date_available'] += 1
                    
                    # 應用過濾邏輯
                    if not self._should_include_in_report_v351_updated(company):
                        # 進一步分類原因
                        validation_result = company.get('validation_result', {})
                        if validation_result.get('overall_status') == 'error':
                            filtered_reasons['validation_failed'] += 1
                        else:
                            filtered_reasons['other_issues'] += 1
                        continue
                    
                    valid_companies.append(company)
                
                # ENHANCED: 詳細統計輸出
                search_group_coverage = (md_date_stats['search_group_coverage'] / md_date_stats['total_files'] * 100) if md_date_stats['total_files'] > 0 else 0
                total_date_coverage = ((md_date_stats['with_md_date'] + md_date_stats['with_content_date_fallback']) / md_date_stats['total_files'] * 100) if md_date_stats['total_files'] > 0 else 0
                
                print(f"📊 投資組合摘要過濾結果:")
                print(f"   原始公司數: {original_count}")
                print(f"   保留公司數: {len(valid_companies)}")
                print(f"   排除原因:")
                print(f"     驗證失敗: {filtered_reasons['validation_failed']}")
                print(f"     其他問題: {filtered_reasons['other_issues']}")
                print(f"")
                print(f"📅 MD日期來源統計:")
                print(f"   Search Group (md_date): {md_date_stats['with_md_date']}")
                print(f"   Process Group (content_date): {md_date_stats['with_content_date_fallback']}")
                print(f"   無日期: {md_date_stats['no_date_available']}")
                print(f"   Search Group 覆蓋率: {search_group_coverage:.1f}%")
                print(f"   總日期覆蓋率: {total_date_coverage:.1f}%")
            else:
                valid_companies = processed_companies
                print(f"📊 投資組合摘要：未啟用過濾，包含所有 {len(valid_companies)} 家公司")
            
            # 按公司分組，取得每家公司的最佳品質資料
            company_summary = {}
            
            for company_data in valid_companies:
                company_code = company_data.get('company_code', 'Unknown')
                
                if company_code not in company_summary:
                    company_summary[company_code] = {
                        'files': [],
                        'best_quality_data': None
                    }
                
                company_summary[company_code]['files'].append(company_data)
                
                # 選擇最佳品質資料
                current_best = company_summary[company_code]['best_quality_data']
                
                if current_best is None:
                    company_summary[company_code]['best_quality_data'] = company_data
                else:
                    current_quality = company_data.get('quality_score', 0)
                    best_quality = current_best.get('quality_score', 0)
                    
                    if current_quality > best_quality:
                        company_summary[company_code]['best_quality_data'] = company_data
            
            # 生成摘要資料
            summary_rows = []
            
            for company_code, company_info in company_summary.items():
                best_data = company_info['best_quality_data']
                all_files = company_info['files']
                
                # UPDATED: 計算日期範圍 - 使用 md_date 優先邏輯
                oldest_date, newest_date = self._calculate_date_range_with_priority(all_files)
                
                # 使用增強的品質狀態顯示
                quality_score = best_data.get('quality_score', 0)
                md_date = self._get_md_date_with_priority(best_data)
                has_date = bool(md_date and md_date.strip())
                quality_status = self._get_quality_status_by_score_enhanced(quality_score, has_date)
                md_file_url = self._format_md_file_url_with_warning(best_data)
                search_datetime = self._get_search_datetime(best_data)
                
                # 使用最佳品質資料生成摘要
                clean_code = self._clean_stock_code_for_display(company_code)
                
                summary_row = {
                    '代號': clean_code,
                    '名稱': best_data.get('company_name', 'Unknown'),
                    '股票代號': f"{clean_code}-TW",
                    'MD最舊日期': oldest_date,
                    'MD最新日期': newest_date,
                    'MD資料筆數': len(all_files),
                    '分析師數量': best_data.get('analyst_count', 0),
                    '目標價': best_data.get('target_price', ''),
                    '2025EPS最高值': self._format_eps_value(best_data.get('eps_2025_high')),
                    '2025EPS最低值': self._format_eps_value(best_data.get('eps_2025_low')),
                    '2025EPS平均值': self._format_eps_value(best_data.get('eps_2025_avg')),
                    '2025EPS中位數': self._format_eps_value(best_data.get('eps_2025_median')),
                    '2026EPS最高值': self._format_eps_value(best_data.get('eps_2026_high')),
                    '2026EPS最低值': self._format_eps_value(best_data.get('eps_2026_low')),
                    '2026EPS平均值': self._format_eps_value(best_data.get('eps_2026_avg')),
                    '2026EPS中位數': self._format_eps_value(best_data.get('eps_2026_median')),
                    '2027EPS最高值': self._format_eps_value(best_data.get('eps_2027_high')),
                    '2027EPS最低值': self._format_eps_value(best_data.get('eps_2027_low')),
                    '2027EPS平均值': self._format_eps_value(best_data.get('eps_2027_avg')),
                    '2027EPS中位數': self._format_eps_value(best_data.get('eps_2027_median')),
                    '2028EPS最高值': self._format_eps_value(best_data.get('eps_2028_high')),
                    '2028EPS最低值': self._format_eps_value(best_data.get('eps_2028_low')),
                    '2028EPS平均值': self._format_eps_value(best_data.get('eps_2028_avg')),
                    '2028EPS中位數': self._format_eps_value(best_data.get('eps_2028_median')),
                    '2025營收最高值': self._format_eps_value(best_data.get('revenue_2025_high')),
                    '2025營收最低值': self._format_eps_value(best_data.get('revenue_2025_low')),
                    '2025營收平均值': self._format_eps_value(best_data.get('revenue_2025_avg')),
                    '2025營收中位數': self._format_eps_value(best_data.get('revenue_2025_median')),
                    '2026營收最高值': self._format_eps_value(best_data.get('revenue_2026_high')),
                    '2026營收最低值': self._format_eps_value(best_data.get('revenue_2026_low')),
                    '2026營收平均值': self._format_eps_value(best_data.get('revenue_2026_avg')),
                    '2026營收中位數': self._format_eps_value(best_data.get('revenue_2026_median')),
                    '2027營收最高值': self._format_eps_value(best_data.get('revenue_2027_high')),
                    '2027營收最低值': self._format_eps_value(best_data.get('revenue_2027_low')),
                    '2027營收平均值': self._format_eps_value(best_data.get('revenue_2027_avg')),
                    '2027營收中位數': self._format_eps_value(best_data.get('revenue_2027_median')),
                    '2028營收最高值': self._format_eps_value(best_data.get('revenue_2028_high')),
                    '2028營收最低值': self._format_eps_value(best_data.get('revenue_2028_low')),
                    '2028營收平均值': self._format_eps_value(best_data.get('revenue_2028_avg')),
                    '2028營收中位數': self._format_eps_value(best_data.get('revenue_2028_median')),
                    '品質評分': quality_score,
                    '狀態': quality_status,
                    'MD日期': md_date,
                    'MD File': md_file_url,
                    '搜尋日期': search_datetime,
                    '處理日期': self._get_taipei_time()
                }
                
                summary_rows.append(summary_row)
            
            # 建立 DataFrame
            df = pd.DataFrame(summary_rows, columns=self.portfolio_summary_columns)
            df = df.sort_values('代號')
            
            print(f"✅ 投資組合摘要已使用最佳品質資料生成")
            
            return df
            
        except Exception as e:
            print(f"❌ 生成投資組合摘要失敗: {e}")
            return pd.DataFrame(columns=self.portfolio_summary_columns)

    def generate_detailed_report(self, processed_companies: List[Dict], filter_invalid=True) -> pd.DataFrame:
        """UPDATED: 生成詳細報告 - 使用 md_date 優先邏輯"""
        try:
            detailed_rows = []
            filtered_count = 0
            date_source_stats = {
                'md_date': 0,
                'content_date': 0,
                'no_date': 0
            }
            
            for company_data in processed_companies:
                # 檢查是否應該過濾此資料
                if filter_invalid and not self._should_include_in_report_v351_updated(company_data):
                    filtered_count += 1
                    continue
                
                # UPDATED: 使用 md_date 優先邏輯取得日期
                md_date = self._get_md_date_with_priority(company_data)
                date_source = self._get_md_date_source(company_data)
                
                # 統計日期來源
                if date_source == 'md_date':
                    date_source_stats['md_date'] += 1
                elif date_source == 'content_date':
                    date_source_stats['content_date'] += 1
                else:
                    date_source_stats['no_date'] += 1
                
                quality_score = company_data.get('quality_score', 0)
                has_date = bool(md_date and md_date.strip())
                
                # 生成增強驗證狀態標記
                validation_status = self._generate_validation_status_marker_v351(company_data)
                
                # 使用增強的品質狀態顯示
                quality_status = self._get_quality_status_by_score_enhanced(quality_score, has_date)
                
                # 處理 MD 檔案連結
                md_file_url = self._format_md_file_url_with_warning(company_data)

                search_datetime = self._get_search_datetime(company_data)
                
                detailed_row = {
                    '代號': company_data.get('company_code', 'Unknown'),
                    '名稱': company_data.get('company_name', 'Unknown'),
                    '股票代號': f"{company_data.get('company_code', 'Unknown')}-TW",
                    'MD日期': md_date,  # UPDATED: 使用優先邏輯取得的日期
                    '分析師數量': company_data.get('analyst_count', 0),
                    '目標價': company_data.get('target_price', ''),
                    '2025EPS最高值': self._format_eps_value(company_data.get('eps_2025_high')),
                    '2025EPS最低值': self._format_eps_value(company_data.get('eps_2025_low')),
                    '2025EPS平均值': self._format_eps_value(company_data.get('eps_2025_avg')),
                    '2025EPS中位數': self._format_eps_value(company_data.get('eps_2025_median')),
                    '2026EPS最高值': self._format_eps_value(company_data.get('eps_2026_high')),
                    '2026EPS最低值': self._format_eps_value(company_data.get('eps_2026_low')),
                    '2026EPS平均值': self._format_eps_value(company_data.get('eps_2026_avg')),
                    '2026EPS中位數': self._format_eps_value(company_data.get('eps_2026_median')),
                    '2027EPS最高值': self._format_eps_value(company_data.get('eps_2027_high')),
                    '2027EPS最低值': self._format_eps_value(company_data.get('eps_2027_low')),
                    '2027EPS平均值': self._format_eps_value(company_data.get('eps_2027_avg')),
                    '2027EPS中位數': self._format_eps_value(company_data.get('eps_2027_median')),
                    '2028EPS最高值': self._format_eps_value(company_data.get('eps_2028_high')),
                    '2028EPS最低值': self._format_eps_value(company_data.get('eps_2028_low')),
                    '2028EPS平均值': self._format_eps_value(company_data.get('eps_2028_avg')),
                    '2028EPS中位數': self._format_eps_value(company_data.get('eps_2028_median')),
                    '2025營收最高值': self._format_eps_value(company_data.get('revenue_2025_high')),
                    '2025營收最低值': self._format_eps_value(company_data.get('revenue_2025_low')),
                    '2025營收平均值': self._format_eps_value(company_data.get('revenue_2025_avg')),
                    '2025營收中位數': self._format_eps_value(company_data.get('revenue_2025_median')),
                    '2026營收最高值': self._format_eps_value(company_data.get('revenue_2026_high')),
                    '2026營收最低值': self._format_eps_value(company_data.get('revenue_2026_low')),
                    '2026營收平均值': self._format_eps_value(company_data.get('revenue_2026_avg')),
                    '2026營收中位數': self._format_eps_value(company_data.get('revenue_2026_median')),
                    '2027營收最高值': self._format_eps_value(company_data.get('revenue_2027_high')),
                    '2027營收最低值': self._format_eps_value(company_data.get('revenue_2027_low')),
                    '2027營收平均值': self._format_eps_value(company_data.get('revenue_2027_avg')),
                    '2027營收中位數': self._format_eps_value(company_data.get('revenue_2027_median')),
                    '2028營收最高值': self._format_eps_value(company_data.get('revenue_2028_high')),
                    '2028營收最低值': self._format_eps_value(company_data.get('revenue_2028_low')),
                    '2028營收平均值': self._format_eps_value(company_data.get('revenue_2028_avg')),
                    '2028營收中位數': self._format_eps_value(company_data.get('revenue_2028_median')),
                    '品質評分': quality_score,
                    '狀態': quality_status,
                    '驗證狀態': validation_status,
                    'MD File': md_file_url,
                    '搜尋日期': search_datetime,
                    '處理日期': self._get_taipei_time()
                }
                
                detailed_rows.append(detailed_row)
            
            # 建立 DataFrame
            df = pd.DataFrame(detailed_rows, columns=self.detailed_report_columns)
            df = df.sort_values(['代號', 'MD日期'], ascending=[True, False])
            
            # ENHANCED: 詳細統計輸出
            total_files = len(detailed_rows)
            
            print(f"📊 詳細報告統計:")
            print(f"   包含檔案數: {total_files}")
            print(f"   過濾檔案數: {filtered_count}")
            print(f"📅 MD日期來源分布:")
            print(f"   Search Group (md_date): {date_source_stats['md_date']}")
            print(f"   Process Group (content_date): {date_source_stats['content_date']}")
            print(f"   無日期: {date_source_stats['no_date']}")
            
            search_group_coverage = (date_source_stats['md_date'] / total_files * 100) if total_files > 0 else 0
            print(f"   Search Group 覆蓋率: {search_group_coverage:.1f}%")
            
            return df
            
        except Exception as e:
            print(f"❌ 生成詳細報告失敗: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame(columns=self.detailed_report_columns)

    # UPDATED: 新增 md_date 優先邏輯的方法
    def _get_md_date_with_priority(self, company_data: Dict[str, Any]) -> str:
        """UPDATED: 使用優先順序取得 MD 日期: md_date > content_date > empty"""
        
        # Priority 1: md_date from Search Group metadata
        yaml_data = company_data.get('yaml_data', {})
        md_date = yaml_data.get('md_date', '')
        
        if md_date and md_date.strip():
            # Validate and format md_date
            formatted_date = self._format_date_for_display(md_date)
            if formatted_date:
                return formatted_date
        
        # Priority 2: content_date from Process Group extraction
        content_date = company_data.get('content_date', '')
        if content_date and content_date.strip():
            formatted_date = self._format_date_for_display(content_date)
            if formatted_date:
                return formatted_date
        
        # Priority 3: No date available
        return ""

    def _get_md_date_source(self, company_data: Dict[str, Any]) -> str:
        """UPDATED: 取得 MD 日期的來源"""
        
        # Check md_date first
        yaml_data = company_data.get('yaml_data', {})
        md_date = yaml_data.get('md_date', '')
        
        if md_date and md_date.strip():
            return 'md_date'
        
        # Check content_date
        content_date = company_data.get('content_date', '')
        if content_date and content_date.strip():
            return 'content_date'
        
        return 'no_date'

    def _format_date_for_display(self, date_str: str) -> str:
        """格式化日期為顯示格式 YYYY-MM-DD"""
        if not date_str or not date_str.strip():
            return ""
        
        try:
            date_str = date_str.strip()
            
            # Handle YYYY/MM/DD format
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    year, month, day = parts
                    if (year.isdigit() and month.isdigit() and day.isdigit() and
                        1900 <= int(year) <= 2100 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31):
                        return f"{year}-{int(month):02d}-{int(day):02d}"
            
            # Handle YYYY-MM-DD format (already correct)
            elif '-' in date_str and len(date_str) >= 8:
                parts = date_str.split('-')
                if len(parts) >= 3:
                    year, month, day = parts[0], parts[1], parts[2]
                    if (year.isdigit() and month.isdigit() and day.isdigit() and
                        1900 <= int(year) <= 2100 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31):
                        return f"{year}-{int(month):02d}-{int(day):02d}"
            
            # Handle datetime objects
            elif isinstance(date_str, datetime):
                return date_str.strftime('%Y-%m-%d')
                
        except Exception as e:
            print(f"⚠️ 日期格式化失敗: {date_str} - {e}")
        
        return ""

    def _calculate_date_range_with_priority(self, all_files: List[Dict]) -> tuple:
        """UPDATED: 計算日期範圍 - 使用 md_date 優先邏輯"""
        valid_dates = []
        
        for file_data in all_files:
            md_date = self._get_md_date_with_priority(file_data)
            if md_date:
                valid_dates.append(md_date)
        
        if valid_dates:
            valid_dates.sort()
            return valid_dates[0], valid_dates[-1]
        
        return "", ""

    def generate_statistics_report(self, processed_companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """UPDATED: 生成統計報告 - 包含 md_date 統計"""
        total_companies = len(processed_companies)
        
        if total_companies == 0:
            return {
                'total_companies': 0,
                'companies_with_data': 0,
                'success_rate': 0
            }
        
        # 基本統計
        companies_with_data = len([c for c in processed_companies if c.get('quality_score', 0) > 0])
        success_rate = (companies_with_data / total_companies) * 100
        
        # 品質分析
        quality_scores = [c.get('quality_score', 0) for c in processed_companies]
        
        # ENHANCED: MD日期來源詳細分析
        md_date_analysis = {
            'total_files': total_companies,
            'md_date_from_search_group': 0,
            'content_date_from_process_group': 0,
            'no_date_available': 0,
            'search_group_coverage_rate': 0,
            'total_date_coverage_rate': 0
        }
        
        for company in processed_companies:
            date_source = self._get_md_date_source(company)
            
            if date_source == 'md_date':
                md_date_analysis['md_date_from_search_group'] += 1
            elif date_source == 'content_date':
                md_date_analysis['content_date_from_process_group'] += 1
            else:
                md_date_analysis['no_date_available'] += 1
        
        # 計算覆蓋率
        md_date_analysis['search_group_coverage_rate'] = (md_date_analysis['md_date_from_search_group'] / total_companies * 100)
        md_date_analysis['total_date_coverage_rate'] = ((md_date_analysis['md_date_from_search_group'] + md_date_analysis['content_date_from_process_group']) / total_companies * 100)
        
        # 驗證統計
        validation_passed = 0
        validation_failed = 0
        validation_disabled = 0
        
        for company in processed_companies:
            validation_result = company.get('validation_result', {})
            validation_method = validation_result.get('validation_method', 'unknown')
            
            if validation_method == 'disabled':
                validation_disabled += 1
            elif self._should_include_in_report_v351_updated(company):
                validation_passed += 1
            else:
                validation_failed += 1
        
        validation_success_rate = (validation_passed / total_companies) * 100
        
        # 過濾統計
        companies_included_in_report = len([c for c in processed_companies if self._should_include_in_report_v351_updated(c)])
        inclusion_rate = (companies_included_in_report / total_companies) * 100
        
        # 觀察名單相關統計
        watchlist_companies = len([c for c in processed_companies 
                                 if c.get('company_code', '') and 
                                 self._is_watchlist_company(c.get('company_code', ''))])
        
        statistics = {
            'version': '3.6.1-updated-for-md-date',
            'report_type': 'comprehensive_with_md_date_priority',
            'timestamp': datetime.now().isoformat(),
            
            # 基本統計
            'total_companies': total_companies,
            'companies_with_data': companies_with_data,
            'success_rate': round(success_rate, 1),
            
            # ENHANCED: MD日期來源統計
            'md_date_source_analysis': {
                'total_files_processed': md_date_analysis['total_files'],
                'md_date_from_search_group': md_date_analysis['md_date_from_search_group'],
                'content_date_from_process_group': md_date_analysis['content_date_from_process_group'],
                'no_date_available': md_date_analysis['no_date_available'],
                'search_group_coverage_rate': round(md_date_analysis['search_group_coverage_rate'], 1),
                'total_date_coverage_rate': round(md_date_analysis['total_date_coverage_rate'], 1),
                'priority_system': 'md_date (Search Group) > content_date (Process Group) > empty',
                'improvement_note': f"Search Group 提供了 {md_date_analysis['md_date_from_search_group']} 個檔案的 md_date，Process Group 提供了 {md_date_analysis['content_date_from_process_group']} 個 fallback 日期"
            },
            
            # 驗證統計
            'validation_statistics': {
                'validation_passed': validation_passed,
                'validation_failed': validation_failed,
                'validation_disabled': validation_disabled,
                'validation_success_rate': round(validation_success_rate, 1),
                'companies_included_in_report': companies_included_in_report,
                'inclusion_rate': round(inclusion_rate, 1),
                'filtered_out': total_companies - companies_included_in_report
            },
            
            # 品質分析
            'quality_analysis': {
                'average_quality_score': round(sum(quality_scores) / len(quality_scores), 1) if quality_scores else 0,
                'highest_quality_score': max(quality_scores) if quality_scores else 0,
                'lowest_quality_score': min(quality_scores) if quality_scores else 0,
                'files_with_quality_1_or_less': len([s for s in quality_scores if s <= 1]),
                'quality_distribution': {
                    'excellent_9_10': len([s for s in quality_scores if s >= 9]),
                    'good_7_8': len([s for s in quality_scores if 7 <= s < 9]),
                    'fair_5_6': len([s for s in quality_scores if 5 <= s < 7]),
                    'poor_3_4': len([s for s in quality_scores if 3 <= s < 5]),
                    'very_poor_1_2': len([s for s in quality_scores if 1 <= s < 3]),
                    'missing_date_or_error_0_1': len([s for s in quality_scores if s <= 1])
                }
            },
            
            # 觀察名單相關統計
            'watchlist_statistics': {
                'watchlist_companies_in_data': watchlist_companies,
                'watchlist_coverage_in_data': round((watchlist_companies / total_companies) * 100, 1) if total_companies > 0 else 0
            }
        }
        
        return statistics

    # 保持其他方法不變，但使用更新的日期邏輯
    def _get_quality_status_by_score_enhanced(self, score: float, has_date: bool = True) -> str:
        """增強的品質狀態指標 - 考慮日期可用性"""
        if not has_date:
            return "🔴 缺少日期"  # Special status for missing date
        elif score >= 9:
            return "🟢 優秀"
        elif score >= 7:
            return "🟡 良好"
        elif score >= 5:
            return "🟠 普通"
        else:
            return "🔴 不足"

    # 保留所有其他現有方法 (generate_keyword_summary, generate_watchlist_summary, 等等)
    # ... (其他方法保持不變)

    # 輔助方法
    def _is_watchlist_company(self, company_code: str) -> bool:
        """檢查是否為觀察名單公司（簡化檢查）"""
        if company_code and company_code.isdigit() and len(company_code) == 4:
            return True
        return False

    def _clean_stock_code_for_display(self, code):
        """清理股票代號，確保顯示為純數字（無引號）"""
        if pd.isna(code) or code is None or code == '':
            return ''
        
        code_str = str(code).strip()
        
        if code_str.startswith("'"):
            code_str = code_str[1:]
        
        if code_str.isdigit():
            return code_str
        
        return code_str

    def _format_md_file_url_with_warning(self, company_data: Dict[str, Any]) -> str:
        """格式化 MD 檔案連結"""
        filename = company_data.get('filename', '')
        
        if not filename:
            return ""
        
        if not filename.endswith('.md'):
            filename += '.md'
        
        encoded_filename = urllib.parse.quote(filename, safe='')
        raw_url = f"{self.github_repo_base}/data/md/{encoded_filename}"
        
        return raw_url

    def _get_search_datetime(self, company_data: Dict[str, Any]) -> str:
        """取得搜尋時間 (YAML extracted_date)，格式化為 YYYY-MM-DD HH:MM:SS"""
        yaml_data = company_data.get('yaml_data', {})
        raw_value = yaml_data.get('extracted_date') or company_data.get('extracted_date') or ''

        if isinstance(raw_value, datetime):
            return raw_value.strftime('%Y-%m-%d %H:%M:%S')

        raw_str = str(raw_value).strip()
        if not raw_str:
            return ""

        try:
            sanitized = raw_str.replace('T', ' ').replace('Z', '')
            if '.' in sanitized:
                sanitized = sanitized.split('.')[0]
            parsed = datetime.fromisoformat(sanitized)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(self.taipei_tz)
            return parsed.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return raw_str

    def _format_eps_value(self, eps_value) -> str:
        """格式化 EPS 數值"""
        if eps_value is None or eps_value == '':
            return ''
        try:
            eps = float(eps_value)
            return f"{eps:.2f}"
        except (ValueError, TypeError):
            return str(eps_value)

    def _generate_validation_status_marker_v351(self, company_data: Dict[str, Any]) -> str:
        """生成驗證狀態標記"""
        validation_result = company_data.get('validation_result', {})
        validation_status = validation_result.get('overall_status', 'unknown')
        validation_method = validation_result.get('validation_method', 'unknown')
        validation_passed = company_data.get('content_validation_passed', True)
        validation_errors = company_data.get('validation_errors', [])
        validation_warnings = company_data.get('validation_warnings', [])
        validation_enabled = company_data.get('validation_enabled', False)
        
        # 多層驗證狀態判斷
        if validation_status == 'error' or not validation_passed:
            if validation_errors:
                main_error = str(validation_errors[0])
                if "不在觀察名單" in main_error:
                    return "🚫 不在觀察名單"
                elif "公司名稱不符觀察名單" in main_error:
                    return "📝 名稱不符"
                elif "愛派爾" in main_error or "愛立信" in main_error:
                    return "📄 名稱混亂"
                else:
                    return "❌ 驗證失敗"
            else:
                return "❌ 驗證失敗"
        
        elif validation_method == 'disabled' or not validation_enabled:
            return "⚠️ 驗證停用"
        
        elif validation_warnings:
            return "⚠️ 有警告"
        
        else:
            return "✅ 通過"

    # 為了完整性，這裡需要包含其他必要的方法
    # 實際實作中應該包含所有原有的方法，這裡只展示關鍵修改部分
    
    def generate_keyword_summary(self, keyword_analysis: Dict[str, Any]) -> pd.DataFrame:
        """支援查詢模式分析的關鍵字統計報告生成（保持不變）"""
        if not keyword_analysis or keyword_analysis.get('error'):
            return None

        pattern_stats = keyword_analysis.get('pattern_stats', {})
        if not pattern_stats:
            return None

        rows = []
        updated_at = datetime.now(self.taipei_tz).strftime('%Y-%m-%d %H:%M:%S')

        for pattern, stats in sorted(
            pattern_stats.items(),
            key=lambda item: item[1].get('usage_count', 0),
            reverse=True
        ):
            avg_quality = stats.get('avg_quality_score', 0)
            rows.append({
                'Query pattern': pattern,
                '使用次數': stats.get('usage_count', 0),
                '平均品質評分': avg_quality,
                '最高品質評分': stats.get('max_quality_score', 0),
                '最低品質評分': stats.get('min_quality_score', 0),
                '相關公司數量': stats.get('company_count', 0),
                '品質狀態': self._get_quality_status_by_score_enhanced(avg_quality, True),
                '分類': stats.get('category', '其他'),
                '效果評級': self._format_effectiveness_rating(stats.get('effectiveness_score', 0)),
                '處理日期': updated_at
            })

        return pd.DataFrame(rows, columns=self.query_pattern_summary_columns)

    def generate_watchlist_summary(self, watchlist_analysis: Dict[str, Any]) -> pd.DataFrame:
        """生成觀察名單統計報告 (v3.6.1 新增)"""
        if not watchlist_analysis or watchlist_analysis.get('error'):
            return None

        company_status = watchlist_analysis.get('company_processing_status', {})
        keyword_effectiveness = watchlist_analysis.get('keyword_effectiveness_analysis', {})

        status_labels = {
            'processed': '✅ 已處理',
            'multiple_files': '📄 多個檔案',
            'not_found': '❌ 未找到',
            'validation_failed': '❌ 驗證失敗',
            'low_quality': '⚠️ 品質過低'
        }

        rows = []
        updated_at = datetime.now(self.taipei_tz).strftime('%Y-%m-%d %H:%M:%S')

        for company_code, status_info in company_status.items():
            company_name = status_info.get('company_name', '')
            file_count = status_info.get('file_count', 0)
            status = status_labels.get(status_info.get('status', ''), '❓ 未知')

            average_quality = status_info.get('average_quality_score', 0.0)
            max_quality = status_info.get('max_quality_score', 0.0)

            keyword_info = keyword_effectiveness.get(company_code, {})
            keyword_count = keyword_info.get('keyword_diversity', 0) or 0
            main_keywords = keyword_info.get('best_keywords', []) or []
            keyword_avg_quality = keyword_info.get('avg_effectiveness', 0.0) or 0.0

            latest_date = status_info.get('latest_file_date', '')

            validation_status = '✅ 驗證通過'
            if status_info.get('status') == 'validation_failed' or status_info.get('validation_errors'):
                validation_status = '❌ 驗證失敗'

            rows.append({
                '公司代號': self._clean_stock_code_for_display(company_code),
                '公司名稱': company_name,
                'MD檔案數量': file_count,
                '處理狀態': status,
                '平均品質評分': average_quality,
                '最高品質評分': max_quality,
                '搜尋關鍵字數量': keyword_count,
                '主要關鍵字': ', '.join(main_keywords),
                '關鍵字平均品質': keyword_avg_quality,
                '最新檔案日期': latest_date,
                '驗證狀態': validation_status,
                '處理日期': updated_at
            })

        df = pd.DataFrame(rows, columns=self.watchlist_summary_columns)
        return df

    def save_keyword_summary(self, keyword_df: pd.DataFrame) -> str:
        """儲存查詢模式統計報告為最新版本"""
        if keyword_df is None:
            return ""
        os.makedirs(self.output_dir, exist_ok=True)
        query_latest = os.path.join(self.output_dir, "factset_query_pattern_summary_latest.csv")
        keyword_df.to_csv(query_latest, index=False, encoding='utf-8-sig')
        return query_latest

    def save_watchlist_summary(self, watchlist_df: pd.DataFrame) -> str:
        """儲存觀察名單統計報告為最新版本"""
        if watchlist_df is None:
            return ""
        os.makedirs(self.output_dir, exist_ok=True)
        watchlist_latest = os.path.join(self.output_dir, "watchlist_summary_latest.csv")
        watchlist_df.to_csv(watchlist_latest, index=False, encoding='utf-8-sig')
        return watchlist_latest

    def _format_effectiveness_rating(self, score: float) -> str:
        """格式化查詢模式效果評級"""
        try:
            score_val = float(score)
        except (TypeError, ValueError):
            score_val = 0.0

        if score_val >= 8:
            return '優秀 ⭐⭐⭐'
        if score_val >= 6:
            return '良好 ⭐⭐'
        if score_val >= 4:
            return '普通 ⭐'
        return '不足'

    def save_all_reports(self, portfolio_df: pd.DataFrame, detailed_df: pd.DataFrame, 
                        keyword_df: pd.DataFrame = None, watchlist_df: pd.DataFrame = None) -> Dict[str, str]:
        """儲存所有報告為 CSV"""
        saved_files: Dict[str, str] = {}
        os.makedirs(self.output_dir, exist_ok=True)

        def _write_csv(df: pd.DataFrame, path: str) -> None:
            df.to_csv(path, index=False, encoding='utf-8-sig')

        # Portfolio Summary
        if portfolio_df is not None:
            portfolio_latest = os.path.join(self.output_dir, "factset_portfolio_summary_latest.csv")
            _write_csv(portfolio_df, portfolio_latest)
            saved_files['factset_portfolio_summary_latest'] = portfolio_latest

        # Detailed Report
        if detailed_df is not None:
            detailed_latest = os.path.join(self.output_dir, "raw_factset_detailed_report.csv")
            _write_csv(detailed_df, detailed_latest)
            saved_files['factset_detailed_report_latest'] = detailed_latest

        # Query Pattern Summary
        if keyword_df is not None:
            query_latest = os.path.join(self.output_dir, "factset_query_pattern_summary_latest.csv")
            _write_csv(keyword_df, query_latest)
            saved_files['factset_query_pattern_summary_latest'] = query_latest

        # Watchlist Summary
        if watchlist_df is not None:
            watchlist_latest = os.path.join(self.output_dir, "watchlist_summary_latest.csv")
            _write_csv(watchlist_df, watchlist_latest)
            saved_files['watchlist_summary_latest'] = watchlist_latest

        # Legacy processed outputs (for local consumers)
        processed_dir = "data/processed"
        os.makedirs(processed_dir, exist_ok=True)
        if portfolio_df is not None:
            processed_portfolio = os.path.join(processed_dir, "portfolio_summary.csv")
            _write_csv(portfolio_df, processed_portfolio)
            saved_files['processed_portfolio_summary'] = processed_portfolio
        if detailed_df is not None:
            processed_detailed = os.path.join(processed_dir, "detailed_data.csv")
            _write_csv(detailed_df, processed_detailed)
            saved_files['processed_detailed_data'] = processed_detailed

        return saved_files

    def save_statistics_report(self, statistics: Dict[str, Any]) -> str:
        """儲存統計報告為 JSON（僅保留最新檔）"""
        os.makedirs(self.output_dir, exist_ok=True)
        stats_latest = os.path.join(self.output_dir, "statistics_latest.json")
        with open(stats_latest, "w", encoding="utf-8") as f:
            json.dump(statistics, f, ensure_ascii=True, indent=2)
        return stats_latest


# 測試功能
if __name__ == "__main__":
    generator = ReportGenerator()
    
    print("=== ReportGenerator v3.6.1-updated 測試 (md_date 優先邏輯) ===")
    
    # 測試模擬資料
    test_companies = [
        {
            'company_code': '2330',
            'company_name': '台積電',
            'content_date': '2025/09/10',  # Process Group 提取的日期
            'yaml_data': {
                'md_date': '2025/07/31',  # Search Group 提供的日期
                'extracted_date': '2025-09-10T11:53:26.757739'
            },
            'quality_score': 9,
            'analyst_count': 42,
            'validation_result': {'overall_status': 'valid'},
            'content_validation_passed': True,
            'validation_errors': [],
            'filename': '2330_台積電_factset_abc123.md'
        },
        {
            'company_code': '2317',
            'company_name': '鴻海',
            'content_date': '',  # Process Group 未能提取日期
            'yaml_data': {
                'md_date': '2025/05/20',  # Search Group 提供了日期
                'extracted_date': '2025-09-10T11:53:24.593213'
            },
            'quality_score': 8,
            'analyst_count': 22,
            'validation_result': {'overall_status': 'valid'},
            'content_validation_passed': True,
            'validation_errors': [],
            'filename': '2317_鴻海_factset_def456.md'
        }
    ]
    
    print("測試 1: md_date 優先邏輯")
    for company in test_companies:
        md_date = generator._get_md_date_with_priority(company)
        date_source = generator._get_md_date_source(company)
        
        print(f"   {company['company_code']}: MD日期='{md_date}', 來源={date_source}")
    
    print("測試 2: 日期範圍計算")
    oldest, newest = generator._calculate_date_range_with_priority(test_companies)
    print(f"   日期範圍: {oldest} 到 {newest}")
    
    print("測試 3: 統計報告")
    stats = generator.generate_statistics_report(test_companies)
    md_stats = stats.get('md_date_source_analysis', {})
    
    print(f"   Search Group 覆蓋率: {md_stats.get('search_group_coverage_rate', 0)}%")
    print(f"   總日期覆蓋率: {md_stats.get('total_date_coverage_rate', 0)}%")
    
    print(f"\n🎉 ReportGenerator v3.6.1-updated 測試完成!")
    print(f"✅ md_date 優先邏輯已實作")
    print(f"✅ Search Group 日期優先於 Process Group 提取")
    print(f"✅ 完整統計 md_date 來源分布")
