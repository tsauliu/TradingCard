#!/usr/bin/env python3
"""
Failure analysis and recovery recommendations for the robust price logger
Provides detailed failure reporting, pattern analysis, and automated recovery suggestions
"""
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter
from dataclasses import dataclass
import logging

@dataclass
class FailureRecord:
    """Represents a single failure incident"""
    date: str
    timestamp: str
    failure_type: str  # 'download', 'extract', 'process', 'upload'
    error_message: str
    error_code: Optional[str] = None
    retry_count: int = 0
    recovery_action: Optional[str] = None
    context: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'date': self.date,
            'timestamp': self.timestamp,
            'failure_type': self.failure_type,
            'error_message': self.error_message,
            'error_code': self.error_code,
            'retry_count': self.retry_count,
            'recovery_action': self.recovery_action,
            'context': self.context or {}
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FailureRecord':
        return cls(
            date=data['date'],
            timestamp=data['timestamp'],
            failure_type=data['failure_type'],
            error_message=data['error_message'],
            error_code=data.get('error_code'),
            retry_count=data.get('retry_count', 0),
            recovery_action=data.get('recovery_action'),
            context=data.get('context', {})
        )

@dataclass
class FailurePattern:
    """Represents a pattern of failures"""
    pattern_type: str
    frequency: int
    dates_affected: List[str]
    common_error: str
    recovery_recommendation: str
    severity: str  # 'low', 'medium', 'high', 'critical'

class FailureAnalyzer:
    """Analyzes failures and provides recovery recommendations"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.failures: List[FailureRecord] = []
        
    def record_failure(self, date: str, failure_type: str, error: Exception, 
                      retry_count: int = 0, context: Dict[str, Any] = None) -> FailureRecord:
        """Record a failure incident"""
        failure = FailureRecord(
            date=date,
            timestamp=datetime.now().isoformat(),
            failure_type=failure_type,
            error_message=str(error),
            error_code=getattr(error, 'errno', None),
            retry_count=retry_count,
            context=context or {}
        )
        
        self.failures.append(failure)
        self._save_failure_to_disk(failure)
        
        return failure
    
    def _save_failure_to_disk(self, failure: FailureRecord):
        """Save failure record to disk for persistence"""
        failure_file = self.config.get_failure_log_filename(failure.date)
        
        # Load existing failures for this date
        existing_failures = []
        if os.path.exists(failure_file):
            try:
                with open(failure_file, 'r') as f:
                    existing_failures = json.load(f)
            except Exception as e:
                self.logger.warning(f"Could not load existing failures: {e}")
        
        # Add new failure
        existing_failures.append(failure.to_dict())
        
        # Save back to disk
        try:
            with open(failure_file, 'w') as f:
                json.dump(existing_failures, f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save failure record: {e}")
    
    def load_all_failures(self) -> List[FailureRecord]:
        """Load all failure records from disk"""
        all_failures = []
        failures_dir = self.config.directories.failures_path
        
        if not os.path.exists(failures_dir):
            return all_failures
        
        for filename in os.listdir(failures_dir):
            if filename.startswith('failures_') and filename.endswith('.json'):
                filepath = os.path.join(failures_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        failures_data = json.load(f)
                    
                    for failure_dict in failures_data:
                        failure = FailureRecord.from_dict(failure_dict)
                        all_failures.append(failure)
                        
                except Exception as e:
                    self.logger.warning(f"Could not load failures from {filepath}: {e}")
        
        self.failures = all_failures
        return all_failures
    
    def analyze_patterns(self) -> List[FailurePattern]:
        """Analyze failure patterns and identify common issues"""
        if not self.failures:
            self.load_all_failures()
        
        patterns = []
        
        # Group failures by type
        failures_by_type = defaultdict(list)
        for failure in self.failures:
            failures_by_type[failure.failure_type].append(failure)
        
        # Analyze each failure type
        for failure_type, type_failures in failures_by_type.items():
            pattern = self._analyze_failure_type(failure_type, type_failures)
            if pattern:
                patterns.append(pattern)
        
        # Analyze temporal patterns
        temporal_pattern = self._analyze_temporal_patterns()
        if temporal_pattern:
            patterns.append(temporal_pattern)
        
        # Analyze error message patterns
        error_patterns = self._analyze_error_patterns()
        patterns.extend(error_patterns)
        
        return patterns
    
    def _analyze_failure_type(self, failure_type: str, failures: List[FailureRecord]) -> Optional[FailurePattern]:
        """Analyze failures of a specific type"""
        if len(failures) < 2:
            return None
        
        dates_affected = list(set(f.date for f in failures))
        most_common_error = Counter(f.error_message for f in failures).most_common(1)[0][0]
        
        # Determine severity based on frequency and type
        severity = self._determine_severity(failure_type, len(failures), len(dates_affected))
        
        # Generate recovery recommendation
        recommendation = self._get_recovery_recommendation(failure_type, most_common_error)
        
        return FailurePattern(
            pattern_type=f"{failure_type}_failures",
            frequency=len(failures),
            dates_affected=dates_affected,
            common_error=most_common_error,
            recovery_recommendation=recommendation,
            severity=severity
        )
    
    def _analyze_temporal_patterns(self) -> Optional[FailurePattern]:
        """Analyze temporal patterns in failures"""
        if not self.failures:
            return None
        
        # Group failures by date
        failures_by_date = defaultdict(list)
        for failure in self.failures:
            failures_by_date[failure.date].append(failure)
        
        # Find dates with multiple failures
        problematic_dates = [date for date, failures in failures_by_date.items() if len(failures) > 2]
        
        if len(problematic_dates) < 2:
            return None
        
        return FailurePattern(
            pattern_type="temporal_clustering",
            frequency=len(problematic_dates),
            dates_affected=problematic_dates,
            common_error="Multiple failures on same dates",
            recovery_recommendation="Check for systematic issues on these dates, consider manual intervention",
            severity="medium" if len(problematic_dates) < 5 else "high"
        )
    
    def _analyze_error_patterns(self) -> List[FailurePattern]:
        """Analyze patterns in error messages"""
        patterns = []
        
        # Group by similar error messages
        error_groups = defaultdict(list)
        for failure in self.failures:
            # Extract key error indicators
            error_key = self._extract_error_key(failure.error_message)
            error_groups[error_key].append(failure)
        
        for error_key, failures in error_groups.items():
            if len(failures) >= 3:  # At least 3 similar errors
                dates_affected = list(set(f.date for f in failures))
                severity = "high" if len(failures) > 10 else "medium"
                
                pattern = FailurePattern(
                    pattern_type="error_pattern",
                    frequency=len(failures),
                    dates_affected=dates_affected,
                    common_error=error_key,
                    recovery_recommendation=self._get_error_specific_recommendation(error_key),
                    severity=severity
                )
                patterns.append(pattern)
        
        return patterns
    
    def _extract_error_key(self, error_message: str) -> str:
        """Extract key identifying information from error message"""
        # Common error patterns
        if "Connection" in error_message or "timeout" in error_message.lower():
            return "connection_error"
        elif "404" in error_message or "Not Found" in error_message:
            return "file_not_found"
        elif "Permission" in error_message or "Forbidden" in error_message:
            return "permission_error"
        elif "Memory" in error_message or "MemoryError" in error_message:
            return "memory_error"
        elif "7z" in error_message or "extract" in error_message.lower():
            return "extraction_error"
        elif "BigQuery" in error_message or "GCP" in error_message:
            return "bigquery_error"
        else:
            # Use first few words as key
            words = error_message.split()[:3]
            return "_".join(words).lower()
    
    def _determine_severity(self, failure_type: str, frequency: int, dates_affected: int) -> str:
        """Determine severity of failure pattern"""
        if failure_type in ["download", "extract"] and frequency > 20:
            return "critical"
        elif failure_type == "upload" and frequency > 10:
            return "high"
        elif dates_affected > 10:
            return "high"
        elif frequency > 5:
            return "medium"
        else:
            return "low"
    
    def _get_recovery_recommendation(self, failure_type: str, error_message: str) -> str:
        """Get recovery recommendation based on failure type and error"""
        recommendations = {
            "download": "Check network connectivity and TCGcsv.com availability. Consider increasing retry delays.",
            "extract": "Verify 7z installation and file integrity. Check disk space.",
            "process": "Check data format consistency. Verify memory availability.",
            "upload": "Verify BigQuery credentials and quotas. Check network connectivity to GCP."
        }
        
        base_rec = recommendations.get(failure_type, "Manual investigation required")
        
        # Add specific recommendations based on error message
        if "timeout" in error_message.lower():
            base_rec += " Increase timeout values."
        elif "memory" in error_message.lower():
            base_rec += " Increase memory limits or reduce batch size."
        elif "permission" in error_message.lower():
            base_rec += " Check file permissions and credentials."
        
        return base_rec
    
    def _get_error_specific_recommendation(self, error_key: str) -> str:
        """Get specific recommendation based on error pattern"""
        recommendations = {
            "connection_error": "Check network stability and implement connection pooling",
            "file_not_found": "Verify date ranges and data availability on source",
            "permission_error": "Review authentication and authorization settings",
            "memory_error": "Reduce batch size and implement memory optimization",
            "extraction_error": "Verify archive integrity and 7z installation",
            "bigquery_error": "Check BigQuery quotas and authentication"
        }
        
        return recommendations.get(error_key, "Manual investigation and debugging required")
    
    def generate_recovery_report(self) -> Dict[str, Any]:
        """Generate comprehensive recovery report"""
        patterns = self.analyze_patterns()
        
        # Statistics
        total_failures = len(self.failures)
        unique_dates = len(set(f.date for f in self.failures))
        failure_types = Counter(f.failure_type for f in self.failures)
        
        # Prioritized recommendations
        prioritized_patterns = sorted(patterns, key=lambda p: (
            {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}[p.severity],
            p.frequency
        ), reverse=True)
        
        # Recovery strategy
        recovery_strategy = self._generate_recovery_strategy(prioritized_patterns)
        
        report = {
            "summary": {
                "total_failures": total_failures,
                "unique_dates_affected": unique_dates,
                "failure_types": dict(failure_types),
                "report_generated": datetime.now().isoformat()
            },
            "patterns": [
                {
                    "type": p.pattern_type,
                    "severity": p.severity,
                    "frequency": p.frequency,
                    "dates_affected": len(p.dates_affected),
                    "common_error": p.common_error,
                    "recommendation": p.recovery_recommendation
                }
                for p in prioritized_patterns
            ],
            "recovery_strategy": recovery_strategy,
            "failed_dates": sorted(list(set(f.date for f in self.failures))),
            "next_actions": self._get_immediate_actions(prioritized_patterns)
        }
        
        return report
    
    def _generate_recovery_strategy(self, patterns: List[FailurePattern]) -> Dict[str, Any]:
        """Generate overall recovery strategy"""
        strategy = {
            "phase_1_immediate": [],
            "phase_2_systematic": [],
            "phase_3_prevention": []
        }
        
        critical_patterns = [p for p in patterns if p.severity == "critical"]
        if critical_patterns:
            strategy["phase_1_immediate"] = [
                "Stop current operations and investigate critical failures",
                "Fix infrastructure issues (network, permissions, resources)",
                "Test with small date range before full restart"
            ]
        
        high_patterns = [p for p in patterns if p.severity in ["high", "medium"]]
        if high_patterns:
            strategy["phase_2_systematic"] = [
                "Implement targeted fixes for high-frequency error patterns",
                "Increase retry policies and timeouts for affected operations",
                "Process failed dates in smaller batches"
            ]
        
        strategy["phase_3_prevention"] = [
            "Implement enhanced monitoring and alerting",
            "Add data quality validation checkpoints",
            "Set up automated recovery procedures"
        ]
        
        return strategy
    
    def _get_immediate_actions(self, patterns: List[FailurePattern]) -> List[str]:
        """Get immediate action items"""
        actions = []
        
        # Get top 3 most severe patterns
        top_patterns = patterns[:3]
        
        for pattern in top_patterns:
            if pattern.severity in ["critical", "high"]:
                actions.append(f"Address {pattern.pattern_type}: {pattern.recovery_recommendation}")
        
        # Add general actions
        if any(p.severity == "critical" for p in patterns):
            actions.append("URGENT: Stop all operations and fix critical issues first")
        
        actions.append("Review and update retry policies based on failure patterns")
        actions.append("Implement incremental recovery starting with most recent dates")
        
        return actions[:5]  # Limit to top 5 actions
    
    def save_recovery_report(self, filename: str = None) -> str:
        """Save recovery report to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"recovery_report_{timestamp}.json"
        
        report_path = os.path.join(self.config.directories.logs_path, filename)
        
        try:
            report = self.generate_recovery_report()
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            self.logger.info(f"Recovery report saved to: {report_path}")
            return report_path
        except Exception as e:
            self.logger.error(f"Failed to save recovery report: {e}")
            raise
    
    def get_failed_dates(self) -> List[str]:
        """Get list of all dates that had failures"""
        if not self.failures:
            self.load_all_failures()
        
        return sorted(list(set(failure.date for failure in self.failures)))
    
    def validate_recovery(self, date: str) -> Dict[str, Any]:
        """Validate if recovery was successful for a specific date"""
        processed_csv = self.config.get_processed_csv_path(date)
        
        validation_result = {
            "date": date,
            "csv_exists": os.path.exists(processed_csv),
            "csv_size": 0,
            "record_count": 0,
            "validation_passed": False,
            "issues": []
        }
        
        if validation_result["csv_exists"]:
            try:
                # Get file size
                validation_result["csv_size"] = os.path.getsize(processed_csv)
                
                # Count records (approximate)
                with open(processed_csv, 'r') as f:
                    line_count = sum(1 for _ in f) - 1  # Subtract header
                validation_result["record_count"] = line_count
                
                # Basic validation
                if line_count < self.config.validation.min_records_per_date:
                    validation_result["issues"].append(f"Low record count: {line_count}")
                elif line_count > self.config.validation.max_records_per_date:
                    validation_result["issues"].append(f"High record count: {line_count}")
                else:
                    validation_result["validation_passed"] = True
                    
            except Exception as e:
                validation_result["issues"].append(f"Validation error: {str(e)}")
        else:
            validation_result["issues"].append("CSV file not found")
        
        return validation_result