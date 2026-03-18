from typing import Any, Dict
import json

class ReportFormatter:
    """
    Formats the raw dictionary results from validators and reconcilers into readable reports.
    """
    
    @staticmethod
    def format_validation_report(results: Dict[str, Any]) -> str:
        """
        Generates a readable summary text report for Data Quality Validation.
        """
        dataset = results.get("dataset", "Unknown")
        status = results.get("status", "Unknown")
        rule_results = results.get("rule_results", [])
        
        report_lines = []
        report_lines.append(f"========================================")
        report_lines.append(f"DATA QUALITY VALIDATION REPORT")
        report_lines.append(f"========================================")
        report_lines.append(f"Dataset      : {dataset}")
        report_lines.append(f"Overall Status: {status.upper()}")
        report_lines.append(f"Total Rules  : {len(rule_results)}")
        
        passed_rules = sum(1 for r in rule_results if r.get('passed', False))
        report_lines.append(f"Rules Passed : {passed_rules}")
        report_lines.append(f"Rules Failed : {len(rule_results) - passed_rules}")
        report_lines.append(f"----------------------------------------")
        report_lines.append(f"DETAILED RULE RESULTS")
        report_lines.append(f"----------------------------------------")
        
        for rule in rule_results:
            rule_name = rule.get("rule_name")
            rule_type = rule.get("rule_type")
            column = rule.get("column")
            passed = rule.get("passed", False)
            passed_str = "PASS" if passed else "FAIL"
            
            report_lines.append(f"Rule: {rule_name} | Type: {rule_type} | Column: {column} | Status: {passed_str}")
            res = rule.get("result", {})
            if "error" in res:
                 report_lines.append(f"  Error: {res['error']}")
            else:
                 # Print relevant metrics based on type
                 if rule_type == 'completeness':
                     report_lines.append(f"  Completeness: {res.get('completeness_pct', 0):.2f}%")
                 elif rule_type == 'uniqueness':
                     report_lines.append(f"  Uniqueness: {res.get('uniqueness_pct', 0):.2f}%")
                 elif rule_type == 'range':
                     report_lines.append(f"  Out of Range: {res.get('out_of_range_pct', 0):.2f}%")
                 elif rule_type == 'regex':
                     report_lines.append(f"  Mismatches: {res.get('mismatch_pct', 0):.2f}%")
        
        report_lines.append(f"========================================")
        return "\n".join(report_lines)

    @staticmethod
    def format_reconciliation_report(results: Dict[str, Any]) -> str:
        """
        Generates a readable summary text report for Source vs Target Reconciliation.
        """
        status = results.get("status", "Unknown")
        recon_results = results.get("reconciliation_results", {})
        
        report_lines = []
        report_lines.append(f"========================================")
        report_lines.append(f"DATA RECONCILIATION REPORT")
        report_lines.append(f"========================================")
        report_lines.append(f"Overall Status: {status.upper()}")
        report_lines.append(f"----------------------------------------")
        
        if 'row_count' in recon_results:
            rc = recon_results['row_count']
            report_lines.append(f"ROW COUNT COMPARISON")
            report_lines.append(f"  Source Count : {rc.get('source_count')}")
            report_lines.append(f"  Target Count : {rc.get('target_count')}")
            report_lines.append(f"  Difference   : {rc.get('difference')}")
            report_lines.append(f"  Match        : {'YES' if rc.get('match') else 'NO'}")
            report_lines.append(f"----------------------------------------")

        if 'aggregates' in recon_results:
            report_lines.append(f"AGGREGATES COMPARISON")
            aggs = recon_results['aggregates']
            for col, res in aggs.items():
                report_lines.append(f"  Column: {col}")
                report_lines.append(f"    Source Sum: {res['source']['sum']} | Target Sum: {res['target']['sum']} | Diff: {res['differences']['sum']}")
                report_lines.append(f"    Source Avg: {res['source']['avg']} | Target Avg: {res['target']['avg']} | Diff: {res['differences']['avg']}")
            report_lines.append(f"----------------------------------------")

        if 'record_comparison' in recon_results:
             report_lines.append(f"RECORD LEVEL COMPARISON")
             rcomp = recon_results['record_comparison']
             if rcomp.get('status') == 'success':
                 res = rcomp['results']
                 report_lines.append(f"  Common Records : {res.get('common_records_count')}")
                 report_lines.append(f"  Source Only    : {res.get('source_only_records_count')}")
                 report_lines.append(f"  Target Only    : {res.get('target_only_records_count')}")
                 
                 mismatches = res.get('mismatches', {})
                 if mismatches:
                     report_lines.append(f"  Mismatched Columns:")
                     for col, mdata in mismatches.items():
                         report_lines.append(f"    {col}: {mdata.get('mismatch_count')} mismatches ({mdata.get('mismatch_pct'):.2f}%)")
             else:
                  report_lines.append(f"  Error: {rcomp.get('error')}")

        report_lines.append(f"========================================")
        return "\n".join(report_lines)
