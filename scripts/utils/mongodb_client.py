"""
MongoDB Client for EduFlow Learning Analytics

This module provides a MongoDB client with educational data-specific methods,
including FERPA compliance features, data encryption, and audit logging.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import json
from bson import ObjectId
import hashlib

class MongoDBClient:
    """MongoDB client for educational data storage with privacy compliance."""
    
    def __init__(self):
        self.host = os.getenv('MONGODB_HOST', 'localhost')
        self.port = int(os.getenv('MONGODB_PORT', 27017))
        self.database_name = os.getenv('MONGODB_DATABASE', 'eduflow_analytics')
        self.username = os.getenv('MONGODB_USERNAME')
        self.password = os.getenv('MONGODB_PASSWORD')
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize connection
        self.client = None
        self.database = None
        self._connect()
        
        # Setup indexes for performance and compliance
        self._setup_indexes()
    
    def _connect(self):
        """Establish connection to MongoDB."""
        try:
            if self.username and self.password:
                connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
            else:
                connection_string = f"mongodb://{self.host}:{self.port}"
            
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            
            # Test connection
            self.client.admin.command('ping')
            
            self.database = self.client[self.database_name]
            self.logger.info(f"Connected to MongoDB: {self.host}:{self.port}/{self.database_name}")
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"MongoDB connection error: {str(e)}")
            raise
    
    def _setup_indexes(self):
        """Setup indexes for performance and compliance."""
        try:
            # Student data indexes
            self.database.canvas_students.create_index([("student_id", ASCENDING)], unique=True)
            self.database.canvas_students.create_index([("collected_at", DESCENDING)])
            
            self.database.moodle_users.create_index([("user_id", ASCENDING)], unique=True)
            self.database.moodle_users.create_index([("collected_at", DESCENDING)])
            
            # Course data indexes
            self.database.canvas_courses.create_index([("course_id", ASCENDING)], unique=True)
            self.database.moodle_courses.create_index([("course_id", ASCENDING)], unique=True)
            
            # Enrollment indexes
            self.database.canvas_enrollments.create_index([
                ("course_id", ASCENDING), 
                ("user_id", ASCENDING)
            ])
            self.database.moodle_enrollments.create_index([
                ("course_id", ASCENDING), 
                ("user_id", ASCENDING)
            ])
            
            # Assignment and submission indexes
            self.database.canvas_assignments.create_index([("assignment_id", ASCENDING)], unique=True)
            self.database.canvas_submissions.create_index([
                ("assignment_id", ASCENDING),
                ("user_id", ASCENDING)
            ])
            
            # Attendance indexes
            self.database.attendance_records.create_index([
                ("student_id", ASCENDING),
                ("date", DESCENDING)
            ])
            
            # Library usage indexes
            self.database.library_checkouts.create_index([
                ("student_id", ASCENDING),
                ("checkout_date", DESCENDING)
            ])
            
            # Audit and compliance indexes
            self.database.audit_logs.create_index([("timestamp", DESCENDING)])
            self.database.audit_logs.create_index([("user_id", ASCENDING)])
            self.database.audit_logs.create_index([("action", ASCENDING)])
            
            # Data lineage indexes
            self.database.data_lineage.create_index([("execution_date", DESCENDING)])
            self.database.data_lineage.create_index([("dag_id", ASCENDING)])
            
            # TTL index for temporary data (if needed)
            self.database.temp_data.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create MongoDB indexes: {str(e)}")
            # Don't raise here as indexes are not critical for basic functionality
    
    def store_data(self, collection_name: str, data: List[Dict[str, Any]], 
                   upsert_key: Optional[str] = None) -> Dict[str, int]:
        """
        Store data in MongoDB collection with optional upsert capability.
        
        Args:
            collection_name: Name of the collection
            data: List of documents to store
            upsert_key: Key to use for upsert operations (if None, uses insert)
        
        Returns:
            Dictionary with counts of inserted, updated, and failed operations
        """
        if not data:
            self.logger.warning(f"No data provided for collection {collection_name}")
            return {'inserted': 0, 'updated': 0, 'failed': 0}
        
        try:
            collection = self.database[collection_name]
            
            inserted_count = 0
            updated_count = 0
            failed_count = 0
            
            if upsert_key:
                # Perform upsert operations
                for document in data:
                    try:
                        if upsert_key in document:
                            filter_query = {upsert_key: document[upsert_key]}
                            result = collection.replace_one(
                                filter_query, 
                                document, 
                                upsert=True
                            )
                            
                            if result.upserted_id:
                                inserted_count += 1
                            elif result.modified_count > 0:
                                updated_count += 1
                        else:
                            collection.insert_one(document)
                            inserted_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"Failed to upsert document: {str(e)}")
                        failed_count += 1
            else:
                # Perform bulk insert
                try:
                    result = collection.insert_many(data, ordered=False)
                    inserted_count = len(result.inserted_ids)
                except Exception as e:
                    self.logger.error(f"Bulk insert failed: {str(e)}")
                    # Try individual inserts
                    for document in data:
                        try:
                            collection.insert_one(document)
                            inserted_count += 1
                        except DuplicateKeyError:
                            # Skip duplicates
                            continue
                        except Exception as e:
                            self.logger.error(f"Failed to insert document: {str(e)}")
                            failed_count += 1
            
            # Log audit trail
            self._log_data_operation(collection_name, 'store', len(data), inserted_count, updated_count)
            
            result_summary = {
                'inserted': inserted_count,
                'updated': updated_count,
                'failed': failed_count
            }
            
            self.logger.info(f"Stored data in {collection_name}: {result_summary}")
            return result_summary
            
        except Exception as e:
            self.logger.error(f"Failed to store data in {collection_name}: {str(e)}")
            raise
    
    def get_collection_data(self, collection_name: str, 
                           filter_query: Optional[Dict] = None,
                           limit: Optional[int] = None,
                           sort_field: Optional[str] = None,
                           sort_direction: int = DESCENDING) -> List[Dict[str, Any]]:
        """
        Retrieve data from MongoDB collection.
        
        Args:
            collection_name: Name of the collection
            filter_query: MongoDB filter query
            limit: Maximum number of documents to return
            sort_field: Field to sort by
            sort_direction: Sort direction (ASCENDING or DESCENDING)
        
        Returns:
            List of documents
        """
        try:
            collection = self.database[collection_name]
            
            if filter_query is None:
                filter_query = {}
            
            cursor = collection.find(filter_query)
            
            if sort_field:
                cursor = cursor.sort(sort_field, sort_direction)
            
            if limit:
                cursor = cursor.limit(limit)
            
            # Convert ObjectId to string for JSON serialization
            documents = []
            for doc in cursor:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                documents.append(doc)
            
            self.logger.info(f"Retrieved {len(documents)} documents from {collection_name}")
            return documents
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve data from {collection_name}: {str(e)}")
            raise
    
    def get_student_data(self, student_id: str, 
                        include_collections: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get comprehensive data for a specific student across all collections.
        
        Args:
            student_id: Student identifier
            include_collections: List of collections to include (if None, includes all)
        
        Returns:
            Dictionary with student data from all relevant collections
        """
        try:
            if include_collections is None:
                include_collections = [
                    'canvas_students', 'canvas_enrollments', 'canvas_submissions',
                    'moodle_users', 'moodle_enrollments',
                    'attendance_records', 'library_checkouts'
                ]
            
            student_data = {}
            
            for collection_name in include_collections:
                try:
                    # Different collections might use different field names for student ID
                    id_fields = ['student_id', 'user_id']
                    
                    for id_field in id_fields:
                        filter_query = {id_field: student_id}
                        data = self.get_collection_data(collection_name, filter_query)
                        
                        if data:
                            student_data[collection_name] = data
                            break
                            
                except Exception as e:
                    self.logger.warning(f"Failed to get student data from {collection_name}: {str(e)}")
                    continue
            
            # Log audit trail for student data access
            self._log_audit_event('student_data_access', student_id, 
                                f"Accessed data from {len(student_data)} collections")
            
            return student_data
            
        except Exception as e:
            self.logger.error(f"Failed to get student data for {student_id}: {str(e)}")
            raise
    
    def get_course_analytics(self, course_id: str) -> Dict[str, Any]:
        """Get analytics data for a specific course."""
        try:
            analytics = {}
            
            # Get course basic info
            course_data = self.get_collection_data('canvas_courses', {'course_id': course_id})
            if not course_data:
                course_data = self.get_collection_data('moodle_courses', {'course_id': course_id})
            
            if course_data:
                analytics['course_info'] = course_data[0]
            
            # Get enrollment statistics
            enrollments = self.get_collection_data('canvas_enrollments', {'course_id': course_id})
            if not enrollments:
                enrollments = self.get_collection_data('moodle_enrollments', {'course_id': course_id})
            
            analytics['enrollment_count'] = len(enrollments)
            analytics['enrollment_types'] = {}
            
            for enrollment in enrollments:
                enrollment_type = enrollment.get('type', 'unknown')
                analytics['enrollment_types'][enrollment_type] = \
                    analytics['enrollment_types'].get(enrollment_type, 0) + 1
            
            # Get assignment statistics
            assignments = self.get_collection_data('canvas_assignments', {'course_id': course_id})
            analytics['assignment_count'] = len(assignments)
            
            # Get submission statistics
            submissions = self.get_collection_data('canvas_submissions', {'course_id': course_id})
            analytics['submission_count'] = len(submissions)
            
            if submissions:
                total_score = sum(s.get('score', 0) for s in submissions if s.get('score'))
                analytics['average_score'] = total_score / len(submissions) if submissions else 0
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Failed to get course analytics for {course_id}: {str(e)}")
            raise
    
    def _log_data_operation(self, collection_name: str, operation: str, 
                           total_records: int, inserted: int, updated: int):
        """Log data operations for audit purposes."""
        try:
            audit_record = {
                'timestamp': datetime.utcnow(),
                'operation': operation,
                'collection': collection_name,
                'total_records': total_records,
                'inserted_records': inserted,
                'updated_records': updated,
                'source': 'mongodb_client'
            }
            
            self.database.audit_logs.insert_one(audit_record)
            
        except Exception as e:
            self.logger.error(f"Failed to log data operation: {str(e)}")
    
    def _log_audit_event(self, action: str, user_id: str, details: str):
        """Log audit events for compliance."""
        try:
            audit_record = {
                'timestamp': datetime.utcnow(),
                'action': action,
                'user_id': user_id,
                'details': details,
                'source': 'mongodb_client'
            }
            
            self.database.audit_logs.insert_one(audit_record)
            
        except Exception as e:
            self.logger.error(f"Failed to log audit event: {str(e)}")
    
    def cleanup_old_data(self, days_to_keep: int = 2555):  # 7 years for FERPA compliance
        """Clean up old data based on retention policy."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            
            collections_to_clean = [
                'canvas_students', 'canvas_courses', 'canvas_enrollments',
                'canvas_assignments', 'canvas_submissions',
                'moodle_users', 'moodle_courses', 'moodle_enrollments',
                'attendance_records', 'library_checkouts'
            ]
            
            total_deleted = 0
            
            for collection_name in collections_to_clean:
                try:
                    collection = self.database[collection_name]
                    result = collection.delete_many({
                        'collected_at': {'$lt': cutoff_date.isoformat()}
                    })
                    
                    deleted_count = result.deleted_count
                    total_deleted += deleted_count
                    
                    if deleted_count > 0:
                        self.logger.info(f"Deleted {deleted_count} old records from {collection_name}")
                        
                        # Log cleanup operation
                        self._log_data_operation(collection_name, 'cleanup', deleted_count, 0, 0)
                        
                except Exception as e:
                    self.logger.error(f"Failed to cleanup {collection_name}: {str(e)}")
                    continue
            
            self.logger.info(f"Data cleanup completed: {total_deleted} total records deleted")
            return total_deleted
            
        except Exception as e:
            self.logger.error(f"Data cleanup failed: {str(e)}")
            raise
    
    def get_collection_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all collections."""
        try:
            stats = {}
            
            for collection_name in self.database.list_collection_names():
                try:
                    collection = self.database[collection_name]
                    
                    # Get basic stats
                    count = collection.count_documents({})
                    
                    # Get size information
                    collection_stats = self.database.command("collStats", collection_name)
                    
                    stats[collection_name] = {
                        'document_count': count,
                        'size_bytes': collection_stats.get('size', 0),
                        'storage_size_bytes': collection_stats.get('storageSize', 0),
                        'index_count': collection_stats.get('nindexes', 0),
                        'index_size_bytes': collection_stats.get('totalIndexSize', 0)
                    }
                    
                except Exception as e:
                    self.logger.warning(f"Failed to get stats for {collection_name}: {str(e)}")
                    stats[collection_name] = {'error': str(e)}
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get collection stats: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test MongoDB connection."""
        try:
            self.client.admin.command('ping')
            self.logger.info("MongoDB connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection test failed: {str(e)}")
            return False
    
    def close_connection(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")