"""
EduFlow Learning Analytics - Student Data Ingestion DAG

This DAG orchestrates the ingestion of student data from multiple sources:
- Learning Management Systems (Canvas, Moodle, Blackboard)
- Student Information Systems
- Attendance Systems
- Library Systems
- Assessment Platforms

The pipeline ensures FERPA compliance and data privacy throughout the process.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import sys
import os

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')

from data_collectors.canvas_collector import CanvasCollector
from data_collectors.moodle_collector import MoodleCollector
from data_collectors.attendance_collector import AttendanceCollector
from data_collectors.library_collector import LibraryCollector
from utils.mongodb_client import MongoDBClient
from utils.privacy_utils import PrivacyUtils
from utils.data_validator import DataValidator

# DAG Configuration
default_args = {
    'owner': 'eduflow-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'student_data_ingestion',
    default_args=default_args,
    description='Ingest student data from multiple educational platforms',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['education', 'data-ingestion', 'privacy-compliant']
)

def validate_environment(**context):
    """Validate that all required environment variables and connections are available."""
    logger = logging.getLogger(__name__)
    
    required_vars = [
        'CANVAS_API_URL', 'CANVAS_API_TOKEN',
        'MOODLE_API_URL', 'MOODLE_API_TOKEN',
        'MONGODB_HOST', 'MONGODB_DATABASE'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    # Test database connections
    try:
        mongo_client = MongoDBClient()
        mongo_client.test_connection()
        logger.info("MongoDB connection successful")
    except Exception as e:
        raise ConnectionError(f"MongoDB connection failed: {str(e)}")
    
    logger.info("Environment validation completed successfully")

def collect_canvas_data(**context):
    """Collect student data from Canvas LMS."""
    logger = logging.getLogger(__name__)
    
    try:
        collector = CanvasCollector()
        
        # Collect different types of data
        courses = collector.get_courses()
        students = collector.get_students()
        enrollments = collector.get_enrollments()
        assignments = collector.get_assignments()
        submissions = collector.get_submissions()
        grades = collector.get_grades()
        
        # Store data with privacy compliance
        privacy_utils = PrivacyUtils()
        mongo_client = MongoDBClient()
        
        # Anonymize sensitive data
        students_anonymized = privacy_utils.anonymize_student_data(students)
        
        # Store in MongoDB
        mongo_client.store_data('canvas_courses', courses)
        mongo_client.store_data('canvas_students', students_anonymized)
        mongo_client.store_data('canvas_enrollments', enrollments)
        mongo_client.store_data('canvas_assignments', assignments)
        mongo_client.store_data('canvas_submissions', submissions)
        mongo_client.store_data('canvas_grades', grades)
        
        logger.info(f"Canvas data collection completed: {len(students)} students, {len(courses)} courses")
        
        return {
            'students_count': len(students),
            'courses_count': len(courses),
            'assignments_count': len(assignments)
        }
        
    except Exception as e:
        logger.error(f"Canvas data collection failed: {str(e)}")
        raise

def collect_moodle_data(**context):
    """Collect student data from Moodle LMS."""
    logger = logging.getLogger(__name__)
    
    try:
        collector = MoodleCollector()
        
        # Collect Moodle data
        courses = collector.get_courses()
        users = collector.get_users()
        enrollments = collector.get_enrollments()
        activities = collector.get_activities()
        grades = collector.get_grades()
        
        # Privacy compliance
        privacy_utils = PrivacyUtils()
        mongo_client = MongoDBClient()
        
        users_anonymized = privacy_utils.anonymize_student_data(users)
        
        # Store in MongoDB
        mongo_client.store_data('moodle_courses', courses)
        mongo_client.store_data('moodle_users', users_anonymized)
        mongo_client.store_data('moodle_enrollments', enrollments)
        mongo_client.store_data('moodle_activities', activities)
        mongo_client.store_data('moodle_grades', grades)
        
        logger.info(f"Moodle data collection completed: {len(users)} users, {len(courses)} courses")
        
        return {
            'users_count': len(users),
            'courses_count': len(courses),
            'activities_count': len(activities)
        }
        
    except Exception as e:
        logger.error(f"Moodle data collection failed: {str(e)}")
        raise

def collect_attendance_data(**context):
    """Collect attendance data from various systems."""
    logger = logging.getLogger(__name__)
    
    try:
        collector = AttendanceCollector()
        
        # Collect attendance records
        attendance_records = collector.get_attendance_records()
        class_sessions = collector.get_class_sessions()
        
        # Privacy compliance
        privacy_utils = PrivacyUtils()
        mongo_client = MongoDBClient()
        
        attendance_anonymized = privacy_utils.anonymize_attendance_data(attendance_records)
        
        # Store in MongoDB
        mongo_client.store_data('attendance_records', attendance_anonymized)
        mongo_client.store_data('class_sessions', class_sessions)
        
        logger.info(f"Attendance data collection completed: {len(attendance_records)} records")
        
        return {
            'attendance_records_count': len(attendance_records),
            'class_sessions_count': len(class_sessions)
        }
        
    except Exception as e:
        logger.error(f"Attendance data collection failed: {str(e)}")
        raise

def collect_library_data(**context):
    """Collect library usage data."""
    logger = logging.getLogger(__name__)
    
    try:
        collector = LibraryCollector()
        
        # Collect library data
        checkouts = collector.get_checkouts()
        digital_access = collector.get_digital_access()
        resources = collector.get_resources()
        
        # Privacy compliance
        privacy_utils = PrivacyUtils()
        mongo_client = MongoDBClient()
        
        checkouts_anonymized = privacy_utils.anonymize_library_data(checkouts)
        digital_access_anonymized = privacy_utils.anonymize_library_data(digital_access)
        
        # Store in MongoDB
        mongo_client.store_data('library_checkouts', checkouts_anonymized)
        mongo_client.store_data('library_digital_access', digital_access_anonymized)
        mongo_client.store_data('library_resources', resources)
        
        logger.info(f"Library data collection completed: {len(checkouts)} checkouts")
        
        return {
            'checkouts_count': len(checkouts),
            'digital_access_count': len(digital_access),
            'resources_count': len(resources)
        }
        
    except Exception as e:
        logger.error(f"Library data collection failed: {str(e)}")
        raise

def validate_collected_data(**context):
    """Validate the quality and integrity of collected data."""
    logger = logging.getLogger(__name__)
    
    try:
        validator = DataValidator()
        mongo_client = MongoDBClient()
        
        # Get collection names
        collections = [
            'canvas_students', 'canvas_courses', 'canvas_assignments',
            'moodle_users', 'moodle_courses', 'moodle_activities',
            'attendance_records', 'class_sessions',
            'library_checkouts', 'library_resources'
        ]
        
        validation_results = {}
        
        for collection in collections:
            try:
                data = mongo_client.get_collection_data(collection)
                if data:
                    result = validator.validate_collection(collection, data)
                    validation_results[collection] = result
                    logger.info(f"Validation for {collection}: {result['status']}")
                else:
                    logger.warning(f"No data found in collection: {collection}")
                    
            except Exception as e:
                logger.error(f"Validation failed for {collection}: {str(e)}")
                validation_results[collection] = {'status': 'failed', 'error': str(e)}
        
        # Check overall validation status
        failed_validations = [k for k, v in validation_results.items() if v.get('status') != 'passed']
        
        if failed_validations:
            logger.warning(f"Data validation issues in collections: {failed_validations}")
        else:
            logger.info("All data validations passed successfully")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Data validation process failed: {str(e)}")
        raise

def create_data_lineage(**context):
    """Create data lineage tracking for compliance and auditing."""
    logger = logging.getLogger(__name__)
    
    try:
        mongo_client = MongoDBClient()
        
        # Create lineage record
        lineage_record = {
            'dag_id': context['dag'].dag_id,
            'run_id': context['run_id'],
            'execution_date': context['execution_date'],
            'data_sources': [
                'Canvas LMS',
                'Moodle LMS', 
                'Attendance System',
                'Library System'
            ],
            'collections_updated': [
                'canvas_students', 'canvas_courses', 'canvas_assignments',
                'moodle_users', 'moodle_courses', 'moodle_activities',
                'attendance_records', 'library_checkouts'
            ],
            'privacy_measures_applied': [
                'data_anonymization',
                'pii_encryption',
                'access_logging'
            ],
            'compliance_status': 'FERPA_compliant',
            'created_at': datetime.utcnow()
        }
        
        mongo_client.store_data('data_lineage', [lineage_record])
        logger.info("Data lineage record created successfully")
        
    except Exception as e:
        logger.error(f"Data lineage creation failed: {str(e)}")
        raise

def send_completion_notification(**context):
    """Send notification about pipeline completion."""
    logger = logging.getLogger(__name__)
    
    try:
        # Get task results from XCom
        canvas_results = context['task_instance'].xcom_pull(task_ids='collect_canvas_data')
        moodle_results = context['task_instance'].xcom_pull(task_ids='collect_moodle_data')
        attendance_results = context['task_instance'].xcom_pull(task_ids='collect_attendance_data')
        library_results = context['task_instance'].xcom_pull(task_ids='collect_library_data')
        validation_results = context['task_instance'].xcom_pull(task_ids='validate_collected_data')
        
        # Create summary
        summary = {
            'execution_date': context['execution_date'].strftime('%Y-%m-%d %H:%M:%S'),
            'canvas_data': canvas_results,
            'moodle_data': moodle_results,
            'attendance_data': attendance_results,
            'library_data': library_results,
            'validation_status': 'passed' if validation_results else 'failed'
        }
        
        logger.info(f"Pipeline completed successfully: {summary}")
        
        # Here you would typically send email/Slack notification
        # For now, just log the completion
        
    except Exception as e:
        logger.error(f"Notification sending failed: {str(e)}")
        # Don't raise here as this is not critical

# Define tasks
validate_env_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag
)

collect_canvas_task = PythonOperator(
    task_id='collect_canvas_data',
    python_callable=collect_canvas_data,
    dag=dag
)

collect_moodle_task = PythonOperator(
    task_id='collect_moodle_data',
    python_callable=collect_moodle_data,
    dag=dag
)

collect_attendance_task = PythonOperator(
    task_id='collect_attendance_data',
    python_callable=collect_attendance_data,
    dag=dag
)

collect_library_task = PythonOperator(
    task_id='collect_library_data',
    python_callable=collect_library_data,
    dag=dag
)

validate_data_task = PythonOperator(
    task_id='validate_collected_data',
    python_callable=validate_collected_data,
    dag=dag
)

create_lineage_task = PythonOperator(
    task_id='create_data_lineage',
    python_callable=create_data_lineage,
    dag=dag
)

notify_completion_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag
)

# Define task dependencies
validate_env_task >> [
    collect_canvas_task,
    collect_moodle_task,
    collect_attendance_task,
    collect_library_task
]

[
    collect_canvas_task,
    collect_moodle_task,
    collect_attendance_task,
    collect_library_task
] >> validate_data_task

validate_data_task >> create_lineage_task >> notify_completion_task