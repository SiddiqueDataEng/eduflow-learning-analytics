"""
Privacy Utilities for EduFlow Learning Analytics

This module provides privacy and compliance utilities for educational data,
including FERPA compliance, data anonymization, encryption, and audit logging.
"""

import os
import hashlib
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import json
import re
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

class PrivacyUtils:
    """Privacy and compliance utilities for educational data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Get encryption key from environment
        self.encryption_key = os.getenv('ENCRYPTION_KEY')
        if not self.encryption_key:
            self.logger.warning("No encryption key provided, generating temporary key")
            self.encryption_key = Fernet.generate_key()
        
        # Initialize Fernet cipher
        if isinstance(self.encryption_key, str):
            self.encryption_key = self.encryption_key.encode()
        
        self.cipher = Fernet(self.encryption_key)
        
        # FERPA compliance settings
        self.ferpa_enabled = os.getenv('FERPA_COMPLIANCE_MODE', 'true').lower() == 'true'
        self.audit_enabled = os.getenv('ENABLE_AUDIT_LOGGING', 'true').lower() == 'true'
        self.anonymization_enabled = os.getenv('ENABLE_DATA_ANONYMIZATION', 'true').lower() == 'true'
        
        # PII field patterns
        self.pii_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b',
            'student_id': r'\b\d{6,10}\b'  # Assuming student IDs are 6-10 digits
        }
        
        # Fields that contain PII and should be anonymized
        self.pii_fields = [
            'email', 'login_id', 'name', 'sortable_name', 'short_name',
            'phone', 'address', 'ssn', 'student_number', 'sis_user_id'
        ]
        
        # Fields that should be encrypted
        self.sensitive_fields = [
            'email', 'phone', 'address', 'ssn', 'student_number'
        ]
    
    def anonymize_student_data(self, students: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Anonymize student data for FERPA compliance.
        
        Args:
            students: List of student records
            
        Returns:
            List of anonymized student records
        """
        if not self.anonymization_enabled:
            self.logger.info("Data anonymization is disabled")
            return students
        
        try:
            anonymized_students = []
            
            for student in students:
                anonymized_student = self._anonymize_record(student.copy())
                
                # Generate consistent anonymous ID
                if 'student_id' in student:
                    anonymized_student['anonymous_id'] = self._generate_anonymous_id(
                        str(student['student_id'])
                    )
                
                # Remove or hash PII fields
                for field in self.pii_fields:
                    if field in anonymized_student:
                        if field in self.sensitive_fields:
                            # Encrypt sensitive fields
                            anonymized_student[f'{field}_encrypted'] = self._encrypt_field(
                                str(anonymized_student[field])
                            )
                            del anonymized_student[field]
                        else:
                            # Hash other PII fields
                            anonymized_student[f'{field}_hash'] = self._hash_field(
                                str(anonymized_student[field])
                            )
                            del anonymized_student[field]
                
                # Add anonymization metadata
                anonymized_student['anonymized_at'] = datetime.utcnow().isoformat()
                anonymized_student['privacy_level'] = 'ferpa_compliant'
                
                anonymized_students.append(anonymized_student)
            
            self.logger.info(f"Anonymized {len(anonymized_students)} student records")
            
            # Log audit event
            if self.audit_enabled:
                self._log_privacy_event(
                    'student_data_anonymization',
                    f"Anonymized {len(students)} student records"
                )
            
            return anonymized_students
            
        except Exception as e:
            self.logger.error(f"Student data anonymization failed: {str(e)}")
            raise
    
    def anonymize_attendance_data(self, attendance_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Anonymize attendance data."""
        if not self.anonymization_enabled:
            return attendance_records
        
        try:
            anonymized_records = []
            
            for record in attendance_records:
                anonymized_record = self._anonymize_record(record.copy())
                
                # Replace student ID with anonymous ID
                if 'student_id' in record:
                    anonymized_record['anonymous_id'] = self._generate_anonymous_id(
                        str(record['student_id'])
                    )
                    del anonymized_record['student_id']
                
                # Remove any PII that might be in attendance records
                for field in self.pii_fields:
                    if field in anonymized_record:
                        del anonymized_record[field]
                
                anonymized_record['anonymized_at'] = datetime.utcnow().isoformat()
                anonymized_records.append(anonymized_record)
            
            self.logger.info(f"Anonymized {len(anonymized_records)} attendance records")
            return anonymized_records
            
        except Exception as e:
            self.logger.error(f"Attendance data anonymization failed: {str(e)}")
            raise
    
    def anonymize_library_data(self, library_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Anonymize library usage data."""
        if not self.anonymization_enabled:
            return library_records
        
        try:
            anonymized_records = []
            
            for record in library_records:
                anonymized_record = self._anonymize_record(record.copy())
                
                # Replace student ID with anonymous ID
                if 'student_id' in record:
                    anonymized_record['anonymous_id'] = self._generate_anonymous_id(
                        str(record['student_id'])
                    )
                    del anonymized_record['student_id']
                
                # Remove PII from library records
                for field in self.pii_fields:
                    if field in anonymized_record:
                        del anonymized_record[field]
                
                anonymized_record['anonymized_at'] = datetime.utcnow().isoformat()
                anonymized_records.append(anonymized_record)
            
            self.logger.info(f"Anonymized {len(anonymized_records)} library records")
            return anonymized_records
            
        except Exception as e:
            self.logger.error(f"Library data anonymization failed: {str(e)}")
            raise
    
    def _anonymize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Anonymize a single record by removing or masking PII."""
        try:
            # Scan for PII patterns in string fields
            for key, value in record.items():
                if isinstance(value, str):
                    # Check for email patterns
                    if re.search(self.pii_patterns['email'], value):
                        record[key] = self._mask_email(value)
                    
                    # Check for phone patterns
                    elif re.search(self.pii_patterns['phone'], value):
                        record[key] = self._mask_phone(value)
                    
                    # Check for SSN patterns
                    elif re.search(self.pii_patterns['ssn'], value):
                        record[key] = self._mask_ssn(value)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Record anonymization failed: {str(e)}")
            return record
    
    def _generate_anonymous_id(self, original_id: str) -> str:
        """Generate a consistent anonymous ID from original ID."""
        try:
            # Use SHA-256 hash for consistent anonymous IDs
            hash_object = hashlib.sha256(original_id.encode())
            return f"anon_{hash_object.hexdigest()[:16]}"
        except Exception as e:
            self.logger.error(f"Anonymous ID generation failed: {str(e)}")
            return f"anon_{hashlib.md5(original_id.encode()).hexdigest()[:16]}"
    
    def _hash_field(self, value: str) -> str:
        """Hash a field value for anonymization."""
        try:
            return hashlib.sha256(value.encode()).hexdigest()
        except Exception as e:
            self.logger.error(f"Field hashing failed: {str(e)}")
            return hashlib.md5(value.encode()).hexdigest()
    
    def _encrypt_field(self, value: str) -> str:
        """Encrypt a sensitive field."""
        try:
            encrypted_value = self.cipher.encrypt(value.encode())
            return base64.b64encode(encrypted_value).decode()
        except Exception as e:
            self.logger.error(f"Field encryption failed: {str(e)}")
            return self._hash_field(value)  # Fallback to hashing
    
    def _decrypt_field(self, encrypted_value: str) -> str:
        """Decrypt a sensitive field."""
        try:
            encrypted_bytes = base64.b64decode(encrypted_value.encode())
            decrypted_value = self.cipher.decrypt(encrypted_bytes)
            return decrypted_value.decode()
        except Exception as e:
            self.logger.error(f"Field decryption failed: {str(e)}")
            raise
    
    def _mask_email(self, email: str) -> str:
        """Mask email address for privacy."""
        try:
            if '@' in email:
                local, domain = email.split('@', 1)
                if len(local) > 2:
                    masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
                else:
                    masked_local = '*' * len(local)
                return f"{masked_local}@{domain}"
            return email
        except Exception:
            return "***@***.***"
    
    def _mask_phone(self, phone: str) -> str:
        """Mask phone number for privacy."""
        try:
            # Remove non-digits
            digits = re.sub(r'\D', '', phone)
            if len(digits) >= 10:
                return f"***-***-{digits[-4:]}"
            return "***-***-****"
        except Exception:
            return "***-***-****"
    
    def _mask_ssn(self, ssn: str) -> str:
        """Mask SSN for privacy."""
        return "***-**-****"
    
    def validate_ferpa_compliance(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate that data meets FERPA compliance requirements.
        
        Args:
            data: Data to validate
            
        Returns:
            Validation results
        """
        try:
            validation_results = {
                'compliant': True,
                'issues': [],
                'recommendations': []
            }
            
            if not self.ferpa_enabled:
                validation_results['recommendations'].append(
                    "FERPA compliance mode is disabled"
                )
            
            # Check for PII in data
            pii_found = []
            
            def check_for_pii(obj, path=""):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        current_path = f"{path}.{key}" if path else key
                        
                        # Check if field name suggests PII
                        if key.lower() in self.pii_fields:
                            pii_found.append(current_path)
                        
                        # Check field value for PII patterns
                        if isinstance(value, str):
                            for pii_type, pattern in self.pii_patterns.items():
                                if re.search(pattern, value):
                                    pii_found.append(f"{current_path} ({pii_type})")
                        
                        # Recursively check nested objects
                        check_for_pii(value, current_path)
                        
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        check_for_pii(item, f"{path}[{i}]")
            
            check_for_pii(data)
            
            if pii_found:
                validation_results['compliant'] = False
                validation_results['issues'].extend([
                    f"PII found in field: {field}" for field in pii_found
                ])
                validation_results['recommendations'].append(
                    "Apply data anonymization before storing or sharing data"
                )
            
            # Check for required anonymization metadata
            if isinstance(data, dict):
                if 'anonymized_at' not in data and pii_found:
                    validation_results['issues'].append(
                        "Data contains PII but lacks anonymization metadata"
                    )
                
                if 'privacy_level' not in data:
                    validation_results['recommendations'].append(
                        "Add privacy_level metadata to indicate compliance status"
                    )
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"FERPA compliance validation failed: {str(e)}")
            return {
                'compliant': False,
                'issues': [f"Validation error: {str(e)}"],
                'recommendations': ["Review data structure and try again"]
            }
    
    def _log_privacy_event(self, event_type: str, details: str):
        """Log privacy-related events for audit purposes."""
        if not self.audit_enabled:
            return
        
        try:
            # This would typically write to a secure audit log
            # For now, we'll use the standard logger
            audit_entry = {
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': event_type,
                'details': details,
                'compliance_framework': 'FERPA',
                'source': 'privacy_utils'
            }
            
            self.logger.info(f"Privacy audit: {json.dumps(audit_entry)}")
            
        except Exception as e:
            self.logger.error(f"Privacy event logging failed: {str(e)}")
    
    def generate_privacy_report(self, data_collections: List[str]) -> Dict[str, Any]:
        """Generate a privacy compliance report."""
        try:
            report = {
                'generated_at': datetime.utcnow().isoformat(),
                'ferpa_compliance_enabled': self.ferpa_enabled,
                'anonymization_enabled': self.anonymization_enabled,
                'audit_logging_enabled': self.audit_enabled,
                'collections_analyzed': data_collections,
                'privacy_measures': {
                    'data_anonymization': self.anonymization_enabled,
                    'field_encryption': True,
                    'pii_detection': True,
                    'audit_logging': self.audit_enabled
                },
                'recommendations': []
            }
            
            if not self.ferpa_enabled:
                report['recommendations'].append(
                    "Enable FERPA compliance mode for educational data"
                )
            
            if not self.anonymization_enabled:
                report['recommendations'].append(
                    "Enable data anonymization for PII protection"
                )
            
            if not self.audit_enabled:
                report['recommendations'].append(
                    "Enable audit logging for compliance tracking"
                )
            
            return report
            
        except Exception as e:
            self.logger.error(f"Privacy report generation failed: {str(e)}")
            raise
    
    def cleanup_expired_data(self, retention_days: int = 2555) -> int:
        """
        Clean up data that has exceeded retention period.
        
        Args:
            retention_days: Number of days to retain data (default: 7 years for FERPA)
            
        Returns:
            Number of records cleaned up
        """
        try:
            # This would typically interface with the database
            # For now, we'll just log the action
            self.logger.info(f"Data cleanup initiated with {retention_days} day retention")
            
            if self.audit_enabled:
                self._log_privacy_event(
                    'data_cleanup',
                    f"Initiated cleanup with {retention_days} day retention policy"
                )
            
            return 0  # Placeholder
            
        except Exception as e:
            self.logger.error(f"Data cleanup failed: {str(e)}")
            raise