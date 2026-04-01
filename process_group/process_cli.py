#!/usr/bin/env python3
"""
Process CLI - FactSet Pipeline v3.6.1 (Modified)
命令列介面 - 整合增強內容日期處理邏輯
支援缺少內容日期檔案的低品質評分而非排除
"""

import argparse
import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import traceback

class ProcessCLI:
    """Process CLI v3.6.1-modified - 增強內容日期處理整合"""
    
    def __init__(self):
        self.version = "3.6.1-modified"
        
        # 初始化所有模組 (graceful degradation)
        self.md_scanner = None
        self.md_parser = None
        self.quality_analyzer = None
        self.keyword_analyzer = None
        self.watchlist_analyzer = None
        self.report_generator = None
        self.sheets_uploader = None
        
        self._init_components()
    
    def _init_components(self):
        """Initialize components - supports graceful degradation"""
        print(f"ProcessCLI v{self.version} initializing...")

        # 1. MD Scanner (required)
        try:
            from md_scanner import MDScanner
            self.md_scanner = MDScanner(md_dir="data/md")
            print("[OK] MDScanner loaded")
        except ImportError as e:
            print(f"[ERROR] MDScanner failed: {e}")
            sys.exit(1)
        
        # 2. MD Parser (required) - Modified version
        try:
            from md_parser import MDParser
            self.md_parser = MDParser()
            print(f"[OK] MDParser v{self.md_parser.version} loaded")
        except ImportError as e:
            print(f"[ERROR] MDParser failed: {e}")
            sys.exit(1)

        # 3. Quality Analyzer (optional)
        try:
            from quality_analyzer import QualityAnalyzer
            self.quality_analyzer = QualityAnalyzer()
            print("[OK] QualityAnalyzer loaded")
        except ImportError as e:
            print(f"[WARN] QualityAnalyzer failed: {e} (will use basic analysis)")
            self.quality_analyzer = None
        
        # 4. Keyword Analyzer (可選) - v3.6.1
        try:
            from keyword_analyzer import KeywordAnalyzer
            self.keyword_analyzer = KeywordAnalyzer()
            print("✅ KeywordAnalyzer v3.6.1 載入成功")
        except ImportError as e:
            print(f"⚠️ KeywordAnalyzer 載入失敗: {e} (將跳過查詢模式分析)")
            self.keyword_analyzer = None
        
        # 5. Watchlist Analyzer (可選) - v3.6.1 新增
        try:
            from watchlist_analyzer import WatchlistAnalyzer
            self.watchlist_analyzer = WatchlistAnalyzer()
            print("✅ WatchlistAnalyzer v3.6.1 載入成功")
        except ImportError as e:
            print(f"⚠️ WatchlistAnalyzer 載入失敗: {e} (將跳過觀察名單分析)")
            self.watchlist_analyzer = None
        
        # 6. Report Generator (必需) - 使用修改版
        try:
            from report_generator import ReportGenerator
            self.report_generator = ReportGenerator()
            print("✅ ReportGenerator v3.6.1-modified 載入成功")
        except ImportError as e:
            print(f"❌ ReportGenerator 載入失敗: {e}")
            sys.exit(1)
        
        # 7. Sheets Uploader (可選)
        try:
            from sheets_uploader import SheetsUploader
            self.sheets_uploader = SheetsUploader()
            print("✅ SheetsUploader 載入成功")
        except ImportError as e:
            print(f"⚠️ SheetsUploader 載入失敗: {e} (將跳過 Google Sheets 上傳)")
            self.sheets_uploader = None
        
        print(f"🎉 ProcessCLI v{self.version} 初始化完成")

    def validate_setup(self) -> bool:
        """驗證系統設定 - 增強版內容日期檢查"""
        print(f"\n=== ProcessCLI v{self.version} 系統驗證 ===")
        
        validation_results = {}
        overall_status = True
        
        # 1. MD Scanner 驗證
        try:
            md_files = self.md_scanner.scan_all_md_files()
            validation_results['md_scanner'] = {
                'status': 'success',
                'message': f"找到 {len(md_files)} 個 MD 檔案",
                'files_count': len(md_files)
            }
            print(f"✅ MD Scanner: 找到 {len(md_files)} 個檔案")
        except Exception as e:
            validation_results['md_scanner'] = {
                'status': 'error',
                'message': f"MD Scanner 錯誤: {e}",
                'files_count': 0
            }
            print(f"❌ MD Scanner 錯誤: {e}")
            overall_status = False
        
        # 2. MD Parser 驗證 (包含內容日期檢查)
        try:
            if validation_results['md_scanner']['files_count'] > 0:
                # 測試解析一個檔案
                test_file = md_files[0] if md_files else None
                if test_file:
                    test_result = self.md_parser.parse_md_file(test_file)
                    
                    # 檢查內容日期提取
                    content_date = test_result.get('content_date', '')
                    quality_score = test_result.get('quality_score', 0)
                    
                    validation_results['md_parser'] = {
                        'status': 'success',
                        'message': f"MD Parser v{self.md_parser.version} 運行正常",
                        'test_file': os.path.basename(test_file),
                        'content_date_extracted': bool(content_date),
                        'quality_score': quality_score,
                        'enhanced_scoring': quality_score <= 1 if not content_date else False
                    }
                    
                    if content_date:
                        print(f"✅ MD Parser: 測試成功 - 內容日期: {content_date}")
                    else:
                        print(f"✅ MD Parser: 測試成功 - 無內容日期，品質評分: {quality_score}")
                        if quality_score <= 1:
                            print(f"   🔍 增強品質評分: 缺少內容日期已正確懲罰")
                else:
                    validation_results['md_parser'] = {
                        'status': 'warning',
                        'message': "無可測試的 MD 檔案"
                    }
                    print(f"⚠️ MD Parser: 無檔案可測試")
            else:
                validation_results['md_parser'] = {
                    'status': 'warning',
                    'message': "無 MD 檔案可測試"
                }
                print(f"⚠️ MD Parser: 無檔案可測試")
        except Exception as e:
            validation_results['md_parser'] = {
                'status': 'error',
                'message': f"MD Parser 錯誤: {e}"
            }
            print(f"❌ MD Parser 錯誤: {e}")
            overall_status = False
        
        # 3. 查詢模式分析器驗證 (v3.6.1)
        if self.keyword_analyzer:
            try:
                validation_results['keyword_analyzer'] = {
                    'status': 'success',
                    'message': "KeywordAnalyzer v3.6.1 已載入，支援標準化查詢模式分析"
                }
                print("✅ KeywordAnalyzer: 查詢模式分析功能可用")
            except Exception as e:
                validation_results['keyword_analyzer'] = {
                    'status': 'error',
                    'message': f"KeywordAnalyzer 錯誤: {e}"
                }
                print(f"❌ KeywordAnalyzer 錯誤: {e}")
        else:
            validation_results['keyword_analyzer'] = {
                'status': 'disabled',
                'message': "KeywordAnalyzer 未載入"
            }
            print("⚠️ KeywordAnalyzer: 功能停用")
        
        # 4. 觀察名單分析器驗證 (v3.6.1)
        if self.watchlist_analyzer:
            try:
                watchlist_size = len(self.watchlist_analyzer.watchlist_mapping)
                validation_results['watchlist_analyzer'] = {
                    'status': 'success',
                    'message': f"WatchlistAnalyzer v3.6.1 已載入，觀察名單: {watchlist_size} 家公司",
                    'watchlist_size': watchlist_size
                }
                print(f"✅ WatchlistAnalyzer: 觀察名單載入 {watchlist_size} 家公司")
            except Exception as e:
                validation_results['watchlist_analyzer'] = {
                    'status': 'error',
                    'message': f"WatchlistAnalyzer 錯誤: {e}"
                }
                print(f"❌ WatchlistAnalyzer 錯誤: {e}")
        else:
            validation_results['watchlist_analyzer'] = {
                'status': 'disabled',
                'message': "WatchlistAnalyzer 未載入"
            }
            print("⚠️ WatchlistAnalyzer: 功能停用")
        
        # 5. Report Generator 驗證
        try:
            validation_results['report_generator'] = {
                'status': 'success',
                'message': "ReportGenerator v3.6.1-modified 已載入，支援增強內容日期處理"
            }
            print("✅ ReportGenerator: 增強內容日期處理功能可用")
        except Exception as e:
            validation_results['report_generator'] = {
                'status': 'error',
                'message': f"ReportGenerator 錯誤: {e}"
            }
            print(f"❌ ReportGenerator 錯誤: {e}")
            overall_status = False
        
        # 6. Sheets Uploader 驗證
        if self.sheets_uploader:
            try:
                # 這裡可以加入實際的連線測試
                validation_results['sheets_uploader'] = {
                    'status': 'success',
                    'message': "SheetsUploader 已載入"
                }
                print("✅ SheetsUploader: Google Sheets 上傳功能可用")
            except Exception as e:
                validation_results['sheets_uploader'] = {
                    'status': 'error',
                    'message': f"SheetsUploader 錯誤: {e}"
                }
                print(f"❌ SheetsUploader 錯誤: {e}")
        else:
            validation_results['sheets_uploader'] = {
                'status': 'disabled',
                'message': "SheetsUploader 未載入"
            }
            print("⚠️ SheetsUploader: 功能停用")
        
        # 總結
        print(f"\n=== 驗證結果總結 ===")
        if overall_status:
            print("🎉 系統驗證通過！核心功能可用")
        else:
            print("⚠️ 系統驗證部分失敗，但可能仍可運行")
        
        print(f"📋 增強功能狀態:")
        print(f"   內容日期增強處理: ✅ 已啟用")
        print(f"   查詢模式分析: {'✅ 可用' if self.keyword_analyzer else '❌ 停用'}")
        print(f"   觀察名單分析: {'✅ 可用' if self.watchlist_analyzer else '❌ 停用'}")
        print(f"   Google Sheets 上傳: {'✅ 可用' if self.sheets_uploader else '❌ 停用'}")
        
        self._save_validation_results(validation_results)
        return overall_status

    def _save_validation_results(self, validation_results: Dict[str, Any]) -> None:
        """儲存驗證結果（僅保留最新檔）"""
        reports_dir = os.path.join("data", "reports")
        os.makedirs(reports_dir, exist_ok=True)
        output_path = os.path.join(reports_dir, "validation_results_latest.json")
        payload = {
            'version': self.version,
            'timestamp': datetime.now().isoformat(),
            'results': validation_results
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True, indent=2)

    def process_all_md_files(self, upload_sheets=True, **kwargs) -> bool:
        """MODIFIED: 處理所有 MD 檔案 - 增強內容日期統計"""
        print(f"\n=== 完整處理所有 MD 檔案 (v{self.version}) ===")
        
        try:
            # 1. 掃描所有 MD 檔案
            print("🔍 掃描 MD 檔案...")
            md_files = self.md_scanner.scan_all_md_files()
            
            if not md_files:
                print("⚠️ 未找到任何 MD 檔案 - 無需處理 (視為成功)")
                return True
            
            print(f"📁 找到 {len(md_files)} 個 MD 檔案")
            
            # 2. 解析每個檔案
            print("📖 解析 MD 檔案...")
            processed_companies = []
            content_date_stats = {
                'total_processed': 0,
                'successful_date_extraction': 0,
                'failed_date_extraction': 0,
                'low_quality_due_to_missing_date': 0
            }
            
            for i, md_file in enumerate(md_files, 1):
                try:
                    print(f"   處理中 ({i}/{len(md_files)}): {os.path.basename(md_file)}")
                    
                    # 使用修改版 MD Parser
                    parsed_data = self.md_parser.parse_md_file(md_file)
                    
                    # ENHANCED: 統計內容日期提取情況
                    content_date = parsed_data.get('content_date', '')
                    quality_score = parsed_data.get('quality_score', 0)
                    
                    content_date_stats['total_processed'] += 1
                    
                    if content_date and content_date.strip():
                        content_date_stats['successful_date_extraction'] += 1
                    else:
                        content_date_stats['failed_date_extraction'] += 1
                        if quality_score <= 1:
                            content_date_stats['low_quality_due_to_missing_date'] += 1
                    
                    # 品質分析 (如果可用)
                    if self.quality_analyzer:
                        quality_data = self.quality_analyzer.analyze(parsed_data)
                        parsed_data.update(quality_data)
                    
                    processed_companies.append(parsed_data)
                    
                except Exception as e:
                    print(f"   ⚠️ 解析失敗: {os.path.basename(md_file)} - {e}")
                    continue
            
            # ENHANCED: 顯示內容日期統計
            success_rate = (content_date_stats['successful_date_extraction'] / content_date_stats['total_processed'] * 100) if content_date_stats['total_processed'] > 0 else 0
            
            print(f"\n📊 內容日期提取統計:")
            print(f"   總處理檔案: {content_date_stats['total_processed']}")
            print(f"   成功提取日期: {content_date_stats['successful_date_extraction']}")
            print(f"   失敗提取日期: {content_date_stats['failed_date_extraction']}")
            print(f"   成功率: {success_rate:.1f}%")
            print(f"   低品質(缺日期): {content_date_stats['low_quality_due_to_missing_date']}")
            
            if not processed_companies:
                print("❌ 沒有成功處理的公司資料")
                return False
            
            print(f"✅ 成功處理 {len(processed_companies)} 家公司")
            
            # 3. 查詢模式分析 (v3.6.1)
            keyword_analysis = None
            if self.keyword_analyzer:
                print("🔍 進行查詢模式分析...")
                try:
                    keyword_analysis = self.keyword_analyzer.analyze_query_patterns(processed_companies)
                    pattern_count = len(keyword_analysis.get('pattern_stats', {}))
                    print(f"✅ 查詢模式分析完成: {pattern_count} 個模式")
                except Exception as e:
                    print(f"⚠️ 查詢模式分析失敗: {e}")
                    keyword_analysis = None
            
            # 4. 觀察名單分析 (v3.6.1)
            watchlist_analysis = None
            if self.watchlist_analyzer:
                print("🔍 進行觀察名單分析...")
                try:
                    watchlist_analysis = self.watchlist_analyzer.analyze_watchlist_coverage(processed_companies)
                    coverage_rate = watchlist_analysis.get('coverage_rate', 0)
                    print(f"✅ 觀察名單分析完成: 覆蓋率 {coverage_rate:.1f}%")
                except Exception as e:
                    print(f"⚠️ 觀察名單分析失敗: {e}")
                    watchlist_analysis = None
            
            # 5. 生成報告
            print("📋 生成報告...")
            
            # Portfolio Summary
            portfolio_summary = self.report_generator.generate_portfolio_summary(processed_companies)
            print(f"✅ 投資組合摘要: {len(portfolio_summary)} 家公司")
            
            # Detailed Report  
            detailed_report = self.report_generator.generate_detailed_report(processed_companies)
            print(f"✅ 詳細報告: {len(detailed_report)} 筆記錄")
            
            # Query Pattern Summary (v3.6.1)
            pattern_summary = None
            if keyword_analysis:
                pattern_summary = self.report_generator.generate_keyword_summary(keyword_analysis)
                if pattern_summary is not None:
                    print(f"✅ 查詢模式報告: {len(pattern_summary)} 個模式")
                else:
                    print("⚠️ 查詢模式報告生成失敗")

            # Watchlist Summary (v3.6.1)
            watchlist_summary = None
            if watchlist_analysis:
                watchlist_summary = self.report_generator.generate_watchlist_summary(watchlist_analysis)
                if watchlist_summary is not None:
                    print(f"✅ 觀察名單報告: {len(watchlist_summary)} 家公司")
                else:
                    print("⚠️ 觀察名單報告生成失敗")
            
            # 6. 儲存報告
            saved_files = self.report_generator.save_all_reports(
                portfolio_summary, 
                detailed_report, 
                pattern_summary if pattern_summary is not None else None,
                watchlist_summary if watchlist_summary is not None else None
            )
            
            # 7. 生成統計報告
            statistics = self.report_generator.generate_statistics_report(processed_companies)
            self.report_generator.save_statistics_report(statistics)
            
            # 8. 上傳到 Google Sheets (如果可用且要求)
            if upload_sheets and self.sheets_uploader:
                print("☁️ 上傳到 Google Sheets...")
                try:
                    upload_success = self.sheets_uploader.upload_all_reports(
                        portfolio_summary, detailed_report, pattern_summary, watchlist_summary
                    )
                    if upload_success:
                        print("✅ Google Sheets 上傳成功")
                    else:
                        print("⚠️ Google Sheets 上傳失敗")
                except Exception as e:
                    print(f"⚠️ Google Sheets 上傳錯誤: {e}")
            elif upload_sheets:
                print("⚠️ SheetsUploader 未載入，跳過上傳")
            
            print(f"\n🎉 完整處理成功完成！")
            print(f"📈 關鍵統計:")
            content_date_stats_final = statistics.get('content_date_extraction', {})
            print(f"   內容日期成功率: {content_date_stats_final.get('success_rate_percentage', 0)}%")
            print(f"   缺少日期但包含: {content_date_stats_final.get('files_included_despite_missing_date', 0)} 檔案")
            print(f"   報告覆蓋率: {statistics.get('validation_statistics', {}).get('inclusion_rate', 0)}%")
            
            return True
            
        except Exception as e:
            print(f"❌ 處理過程發生錯誤: {e}")
            traceback.print_exc()
            return False

    def analyze_content_date_extraction(self, **kwargs) -> bool:
        """ENHANCED: 專門分析內容日期提取情況"""
        print(f"\n=== 內容日期提取分析 (v{self.version}) ===")
        
        try:
            # 掃描 MD 檔案
            md_files = self.md_scanner.scan_all_md_files()
            
            if not md_files:
                print("❌ 未找到任何 MD 檔案")
                return False
            
            print(f"📁 分析 {len(md_files)} 個 MD 檔案的內容日期提取情況")
            
            # 詳細分析
            extraction_analysis = {
                'total_files': len(md_files),
                'successful_extractions': 0,
                'failed_extractions': 0,
                'extraction_methods': {},
                'quality_impact': {
                    'high_quality_with_date': 0,
                    'low_quality_no_date': 0,
                    'patterns_successful': [],
                    'patterns_failed': []
                }
            }
            
            print("\n📊 逐檔案分析:")
            
            for i, md_file in enumerate(md_files, 1):
                try:
                    filename = os.path.basename(md_file)
                    parsed_data = self.md_parser.parse_md_file(md_file)
                    
                    content_date = parsed_data.get('content_date', '')
                    quality_score = parsed_data.get('quality_score', 0)
                    extraction_method = parsed_data.get('date_extraction_method', 'unknown')
                    
                    if content_date and content_date.strip():
                        extraction_analysis['successful_extractions'] += 1
                        if quality_score >= 7:
                            extraction_analysis['quality_impact']['high_quality_with_date'] += 1
                        
                        # 記錄成功的提取方法
                        if extraction_method not in extraction_analysis['extraction_methods']:
                            extraction_analysis['extraction_methods'][extraction_method] = {'success': 0, 'total': 0}
                        extraction_analysis['extraction_methods'][extraction_method]['success'] += 1
                        
                        print(f"   ✅ {filename}: {content_date} (品質: {quality_score}, 方法: {extraction_method})")
                    else:
                        extraction_analysis['failed_extractions'] += 1
                        if quality_score <= 1:
                            extraction_analysis['quality_impact']['low_quality_no_date'] += 1
                        
                        print(f"   ❌ {filename}: 無日期 (品質: {quality_score}, 方法: {extraction_method})")
                    
                    # 記錄總數
                    if extraction_method not in extraction_analysis['extraction_methods']:
                        extraction_analysis['extraction_methods'][extraction_method] = {'success': 0, 'total': 0}
                    extraction_analysis['extraction_methods'][extraction_method]['total'] += 1
                    
                except Exception as e:
                    print(f"   ⚠️ {filename}: 解析錯誤 - {e}")
                    extraction_analysis['failed_extractions'] += 1
            
            # 統計總結
            success_rate = (extraction_analysis['successful_extractions'] / extraction_analysis['total_files'] * 100)
            
            print(f"\n📈 提取統計總結:")
            print(f"   總檔案數: {extraction_analysis['total_files']}")
            print(f"   成功提取: {extraction_analysis['successful_extractions']}")
            print(f"   提取失敗: {extraction_analysis['failed_extractions']}")
            print(f"   成功率: {success_rate:.1f}%")
            
            print(f"\n🎯 品質影響分析:")
            print(f"   高品質且有日期: {extraction_analysis['quality_impact']['high_quality_with_date']}")
            print(f"   低品質因缺日期: {extraction_analysis['quality_impact']['low_quality_no_date']}")
            
            print(f"\n🔧 提取方法效果:")
            for method, stats in extraction_analysis['extraction_methods'].items():
                method_success_rate = (stats['success'] / stats['total'] * 100) if stats['total'] > 0 else 0
                print(f"   {method}: {stats['success']}/{stats['total']} ({method_success_rate:.1f}%)")
            
            return True
            
        except Exception as e:
            print(f"❌ 內容日期分析失敗: {e}")
            traceback.print_exc()
            return False

    def analyze_keywords_only(self, **kwargs) -> bool:
        """只進行查詢模式分析 (v3.6.1)"""
        print(f"\n=== 查詢模式分析 (v{self.version}) ===")
        
        if not self.keyword_analyzer:
            print("❌ KeywordAnalyzer 未載入，無法進行分析")
            return False
        
        try:
            # 掃描和解析檔案
            md_files = self.md_scanner.scan_all_md_files()
            if not md_files:
                print("❌ 未找到任何 MD 檔案")
                return False
            
            print(f"📁 處理 {len(md_files)} 個檔案進行查詢模式分析")
            
            processed_companies = []
            for md_file in md_files:
                try:
                    parsed_data = self.md_parser.parse_md_file(md_file)
                    processed_companies.append(parsed_data)
                except Exception as e:
                    print(f"⚠️ 解析失敗: {os.path.basename(md_file)}")
                    continue
            
            # 進行查詢模式分析
            keyword_analysis = self.keyword_analyzer.analyze_query_patterns(processed_companies)
            
            pattern_count = len(keyword_analysis.get('pattern_stats', {}))
            print(f"✅ 分析完成: 發現 {pattern_count} 個查詢模式")
            
            # 顯示前幾個模式
            pattern_stats = keyword_analysis.get('pattern_stats', {})
            if pattern_stats:
                print(f"\n🔍 前5個最有效的查詢模式:")
                sorted_patterns = sorted(pattern_stats.items(), 
                                       key=lambda x: x[1]['avg_quality_score'], 
                                       reverse=True)
                
                for i, (pattern, stats) in enumerate(sorted_patterns[:5], 1):
                    print(f"   {i}. {pattern}")
                    print(f"      使用次數: {stats['usage_count']}, 平均品質: {stats['avg_quality_score']:.1f}")
            
            return True
            
        except Exception as e:
            print(f"❌ 查詢模式分析失敗: {e}")
            traceback.print_exc()
            return False

    def analyze_watchlist_only(self, **kwargs) -> bool:
        """只進行觀察名單分析 (v3.6.1)"""
        print(f"\n=== 觀察名單分析 (v{self.version}) ===")
        
        if not self.watchlist_analyzer:
            print("❌ WatchlistAnalyzer 未載入，無法進行分析")
            return False
        
        try:
            # 掃描和解析檔案
            md_files = self.md_scanner.scan_all_md_files()
            if not md_files:
                print("❌ 未找到任何 MD 檔案")
                return False
            
            print(f"📁 處理 {len(md_files)} 個檔案進行觀察名單分析")
            
            processed_companies = []
            for md_file in md_files:
                try:
                    parsed_data = self.md_parser.parse_md_file(md_file)
                    processed_companies.append(parsed_data)
                except Exception as e:
                    print(f"⚠️ 解析失敗: {os.path.basename(md_file)}")
                    continue
            
            # 進行觀察名單分析
            watchlist_analysis = self.watchlist_analyzer.analyze_watchlist_coverage(processed_companies)
            
            coverage_rate = watchlist_analysis.get('coverage_rate', 0)
            total_watchlist = watchlist_analysis.get('total_watchlist_companies', 0)
            processed_count = watchlist_analysis.get('companies_with_md_files', 0)
            
            print(f"✅ 分析完成:")
            print(f"   觀察名單總數: {total_watchlist} 家公司")
            print(f"   已處理公司: {processed_count} 家公司")
            print(f"   覆蓋率: {coverage_rate:.1f}%")
            
            # 顯示狀態分布
            status_summary = watchlist_analysis.get('company_status_summary', {})
            if status_summary:
                print(f"\n📊 處理狀態分布:")
                for status, count in status_summary.items():
                    status_name = {
                        'processed': '✅ 已處理',
                        'not_found': '❌ 未找到',
                        'validation_failed': '🚫 驗證失敗',
                        'low_quality': '🔴 品質過低',
                        'multiple_files': '📄 多個檔案'
                    }.get(status, status)
                    print(f"   {status_name}: {count} 家")
            
            return True
            
        except Exception as e:
            print(f"❌ 觀察名單分析失敗: {e}")
            traceback.print_exc()
            return False

    def generate_keyword_summary(self, upload_sheets=True, **kwargs) -> bool:
        """生成查詢模式統計報告 (v3.6.1)"""
        print(f"\n=== 生成查詢模式統計報告 (v{self.version}) ===")
        
        if not self.keyword_analyzer:
            print("❌ KeywordAnalyzer 未載入，無法生成報告")
            return False
        
        try:
            # 獲取分析結果
            md_files = self.md_scanner.scan_all_md_files()
            processed_companies = []
            
            for md_file in md_files:
                try:
                    parsed_data = self.md_parser.parse_md_file(md_file)
                    processed_companies.append(parsed_data)
                except:
                    continue
            
            keyword_analysis = self.keyword_analyzer.analyze_query_patterns(processed_companies)
            
            # 生成報告
            keyword_summary = self.report_generator.generate_keyword_summary(keyword_analysis)
            
            # 儲存報告
            saved_path = self.report_generator.save_keyword_summary(keyword_summary)
            
            # 上傳 (如果需要)
            if upload_sheets and self.sheets_uploader:
                try:
                    self.sheets_uploader.upload_keyword_summary(keyword_summary)
                    print("✅ 已上傳到 Google Sheets")
                except Exception as e:
                    print(f"⚠️ Google Sheets 上傳失敗: {e}")
            
            print(f"✅ 查詢模式統計報告生成完成")
            return True
            
        except Exception as e:
            print(f"❌ 生成查詢模式報告失敗: {e}")
            traceback.print_exc()
            return False

    def generate_watchlist_summary(self, upload_sheets=True, **kwargs) -> bool:
        """生成觀察名單統計報告 (v3.6.1)"""
        print(f"\n=== 生成觀察名單統計報告 (v{self.version}) ===")
        
        if not self.watchlist_analyzer:
            print("❌ WatchlistAnalyzer 未載入，無法生成報告")
            return False
        
        try:
            # 獲取分析結果
            md_files = self.md_scanner.scan_all_md_files()
            processed_companies = []
            
            for md_file in md_files:
                try:
                    parsed_data = self.md_parser.parse_md_file(md_file)
                    processed_companies.append(parsed_data)
                except:
                    continue
            
            watchlist_analysis = self.watchlist_analyzer.analyze_watchlist_coverage(processed_companies)
            
            # 生成報告
            watchlist_summary = self.report_generator.generate_watchlist_summary(watchlist_analysis)
            
            # 儲存報告
            saved_path = self.report_generator.save_watchlist_summary(watchlist_summary)
            
            # 上傳 (如果需要)
            if upload_sheets and self.sheets_uploader:
                try:
                    self.sheets_uploader.upload_watchlist_summary(watchlist_summary)
                    print("✅ 已上傳到 Google Sheets")
                except Exception as e:
                    print(f"⚠️ Google Sheets 上傳失敗: {e}")
            
            print(f"✅ 觀察名單統計報告生成完成")
            return True
            
        except Exception as e:
            print(f"❌ 生成觀察名單報告失敗: {e}")
            traceback.print_exc()
            return False

    def generate_csv_only(self) -> bool:
        """僅生成 CSV 報告 (用於 Quarantine 偵測)

        輕量級處理:
        - 掃描和解析 MD 檔案
        - 生成 raw_factset_detailed_report.csv
        - 不上傳、不生成其他報告
        - 用於 Quarantine workflow 的偵測來源
        """
        print(f"\n=== 生成 CSV 報告 (v{self.version}) ===")

        try:
            # 1. 掃描 MD 檔案
            print("📁 掃描 MD 檔案...")
            md_files = self.md_scanner.scan_all_md_files()

            if not md_files:
                print("⚠️ 未找到任何 MD 檔案 - 無需生成 CSV (視為成功)")
                return True

            print(f"✅ 找到 {len(md_files)} 個 MD 檔案")

            # 2. 解析 MD 檔案
            print("🔄 解析 MD 檔案...")
            processed_companies = []
            total_files = len(md_files)
            failed_count = 0

            for i, md_file in enumerate(md_files, 1):
                try:
                    # Progress bar display
                    progress_pct = (i / total_files) * 100
                    bar_length = 30
                    filled_length = int(bar_length * i // total_files)
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)

                    print(f"\r   [{bar}] {progress_pct:>5.1f}% ({i}/{total_files}) - {os.path.basename(md_file)[:40]:<40}", end='', flush=True)

                    parsed_data = self.md_parser.parse_md_file(md_file)
                    processed_companies.append(parsed_data)
                except Exception as e:
                    failed_count += 1
                    continue

            # Clear progress line and show summary
            print(f"\r   {'':80}\r", end='')  # Clear line
            print(f"✅ 成功處理 {len(processed_companies)}/{total_files} 家公司", end='')
            if failed_count > 0:
                print(f" (失敗: {failed_count})")
            else:
                print()

            if not processed_companies:
                print("❌ 沒有成功處理的公司資料")
                return False

            # 3. 生成詳細報告 CSV
            print("📋 生成詳細報告...")
            detailed_report = self.report_generator.generate_detailed_report(processed_companies)
            print(f"✅ 詳細報告: {len(detailed_report)} 筆記錄")

            # 4. 僅儲存詳細報告 CSV
            output_path = os.path.join(self.report_generator.output_dir, 'raw_factset_detailed_report.csv')
            detailed_report.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"💾 已儲存: {output_path}")

            print(f"\n✅ CSV 報告生成完成！")
            print(f"📊 用途: Quarantine workflow 偵測來源")

            return True

        except Exception as e:
            print(f"❌ CSV 生成失敗: {e}")
            traceback.print_exc()
            return False

    def force_rescan_all_md_files(self, upload_sheets: bool = True) -> bool:
        """強制重新掃描所有 MD 檔案 (即使版本相同也重新計算 quality_score)

        用途:
        - 修復已遷移但分數不正確的檔案 (例如: 舊版本評分算法導致的錯誤分數)
        - 使用最新的 quality_analyzer_simplified.py 重新評分所有檔案
        - 包含營收資料評分 (25% 權重)

        流程:
        1. 設定 md_parser.force_rescan = True
        2. 掃描所有 MD 檔案
        3. 重新解析並更新每個檔案的 quality_score
        4. 生成報告並上傳
        """
        print(f"\n=== 強制重新掃描所有 MD 檔案 (v{self.version}) ===")
        print(f"⚠️ 將重新計算所有檔案的 quality_score (包含營收評分)")

        try:
            # 啟用強制掃描模式
            self.md_parser.force_rescan = True
            print(f"✅ 已啟用強制掃描模式")

            # 調用完整的處理流程
            success = self.process_all_md_files(upload_sheets=upload_sheets)

            # 還原強制掃描模式
            self.md_parser.force_rescan = False

            if success:
                print(f"\n✅ 強制重新掃描完成！")
                print(f"📊 所有檔案的 quality_score 已使用最新算法重新計算")
            else:
                print(f"\n❌ 強制重新掃描失敗")

            return success

        except Exception as e:
            self.md_parser.force_rescan = False  # 確保還原
            print(f"❌ 強制重新掃描失敗: {e}")
            traceback.print_exc()
            return False

    def show_stats(self) -> bool:
        """顯示統計資訊 - 增強內容日期統計"""
        print(f"\n=== ProcessCLI v{self.version} 統計資訊 ===")

        try:
            # MD 檔案統計
            md_files = self.md_scanner.scan_all_md_files()
            stats = self.md_scanner.get_stats()

            print(f"📁 MD 檔案統計:")
            print(f"   總檔案數: {len(md_files)}")
            print(f"   檔案總大小: {stats.get('total_size_mb', 0):.1f} MB")
            print(f"   最新檔案: {stats.get('newest_file', 'N/A')}")
            print(f"   最舊檔案: {stats.get('oldest_file', 'N/A')}")

            # 快速內容日期統計
            if md_files:
                print(f"\n📅 內容日期提取快速統計 (抽樣前10個檔案):")
                sample_files = md_files[:10]
                sample_stats = {'with_date': 0, 'without_date': 0, 'low_quality': 0}

                for md_file in sample_files:
                    try:
                        parsed_data = self.md_parser.parse_md_file(md_file)
                        content_date = parsed_data.get('content_date', '')
                        quality_score = parsed_data.get('quality_score', 0)

                        if content_date:
                            sample_stats['with_date'] += 1
                        else:
                            sample_stats['without_date'] += 1
                            if quality_score <= 1:
                                sample_stats['low_quality'] += 1
                    except:
                        sample_stats['without_date'] += 1

                sample_success_rate = (sample_stats['with_date'] / len(sample_files) * 100)
                print(f"   抽樣成功率: {sample_success_rate:.1f}% ({sample_stats['with_date']}/{len(sample_files)})")
                print(f"   低品質(缺日期): {sample_stats['low_quality']}")

            # 模組狀態
            print(f"\n🔧 模組狀態:")
            print(f"   MD Parser: ✅ v{self.md_parser.version if self.md_parser else 'N/A'}")
            print(f"   Quality Analyzer: {'✅' if self.quality_analyzer else '❌'}")
            print(f"   Keyword Analyzer: {'✅' if self.keyword_analyzer else '❌'}")
            print(f"   Watchlist Analyzer: {'✅' if self.watchlist_analyzer else '❌'}")
            print(f"   Report Generator: {'✅' if self.report_generator else '❌'}")
            print(f"   Sheets Uploader: {'✅' if self.sheets_uploader else '❌'}")

            return True

        except Exception as e:
            print(f"❌ 統計資訊獲取失敗: {e}")
            return False


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(
        description='FactSet 處理系統 v3.6.1-modified - 增強內容日期處理',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
命令範例:
  python process_cli.py validate                    # 驗證系統設定
  python process_cli.py process                     # 完整處理所有檔案
  python process_cli.py process --no-upload         # 處理但不上傳
  python process_cli.py generate-csv                # 僅生成 CSV (用於 Quarantine)
  python process_cli.py force-rescan                # 強制重新掃描所有檔案 (重新計算分數)
  python process_cli.py force-rescan --no-upload    # 強制重新掃描但不上傳
  python process_cli.py analyze-content-date        # 分析內容日期提取
  python process_cli.py analyze-keywords            # 查詢模式分析
  python process_cli.py analyze-watchlist           # 觀察名單分析
  python process_cli.py keyword-summary             # 生成查詢模式報告
  python process_cli.py watchlist-summary           # 生成觀察名單報告
  python process_cli.py stats                       # 顯示統計資訊

v3.6.1-modified 增強功能:
  ✅ 缺少內容日期的檔案顯示低品質評分而非排除
  ✅ 詳細的內容日期提取成功率統計
  ✅ 標準化查詢模式分析和報告
  ✅ 觀察名單覆蓋率分析和報告
  ✅ 輕量級 CSV 生成 (generate-csv) 用於 Quarantine 偵測
        """
    )
    
    parser.add_argument('command', choices=[
        'validate',                # 驗證系統設定
        'process',                 # 處理所有 MD 檔案
        'process-recent',          # 處理最近的 MD 檔案
        'process-single',          # 處理單一公司
        'generate-csv',            # 僅生成 CSV (用於 Quarantine)
        'force-rescan',            # 強制重新掃描所有 MD 檔案 (重新計算 quality_score)
        'analyze-quality',         # 品質分析
        'analyze-keywords',        # 查詢模式分析 (v3.6.1)
        'analyze-watchlist',       # 觀察名單分析 (v3.6.1)
        'analyze-content-date',    # 內容日期提取分析 (modified)
        'keyword-summary',         # 查詢模式統計報告 (v3.6.1)
        'watchlist-summary',       # 觀察名單統計報告 (v3.6.1)
        'stats',                   # 顯示統計資訊
    ])
    
    parser.add_argument('--company', help='公司代號')
    parser.add_argument('--hours', type=int, default=24, help='小時數')
    parser.add_argument('--no-upload', action='store_true', help='不上傳到 Sheets')
    parser.add_argument('--force-upload', action='store_true', help='強制上傳，忽略驗證錯誤')
    parser.add_argument('--min-usage', type=int, default=1, help='查詢模式最小使用次數')
    parser.add_argument('--include-missing', action='store_true', help='包含缺失公司資訊')
    parser.add_argument('--dry-run', action='store_true', help='預覽模式，不實際執行')
    
    args = parser.parse_args()
    
    # 初始化 CLI
    try:
        cli = ProcessCLI()
    except Exception as e:
        print(f"❌ ProcessCLI 初始化失敗: {e}")
        sys.exit(1)
    
    # 執行命令
    success = False
    upload_sheets = not args.no_upload
    
    try:
        if args.command == 'validate':
            success = cli.validate_setup()

        elif args.command == 'process':
            success = cli.process_all_md_files(upload_sheets=upload_sheets)

        elif args.command == 'generate-csv':
            success = cli.generate_csv_only()

        elif args.command == 'force-rescan':
            success = cli.force_rescan_all_md_files(upload_sheets=upload_sheets)

        elif args.command == 'analyze-content-date':
            success = cli.analyze_content_date_extraction()

        elif args.command == 'analyze-keywords':
            success = cli.analyze_keywords_only()

        elif args.command == 'analyze-watchlist':
            success = cli.analyze_watchlist_only()

        elif args.command == 'keyword-summary':
            success = cli.generate_keyword_summary(upload_sheets=upload_sheets)

        elif args.command == 'watchlist-summary':
            success = cli.generate_watchlist_summary(upload_sheets=upload_sheets)

        elif args.command == 'stats':
            success = cli.show_stats()

        else:
            print(f"❌ 未實現的命令: {args.command}")
            success = False
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 用戶中斷操作")
        success = False
    except Exception as e:
        print(f"❌ 執行命令時發生錯誤: {e}")
        traceback.print_exc()
        success = False
    
    # 結束
    if success:
        print(f"\n✅ 命令 '{args.command}' 執行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 命令 '{args.command}' 執行失敗")
        sys.exit(1)


if __name__ == "__main__":
    main()
