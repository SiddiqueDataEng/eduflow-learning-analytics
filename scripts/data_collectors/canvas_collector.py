"""
Canvas LMS Data Collector

This module handles data collection from Canvas Learning Management System.
It provides methods to extract student data, course information, assignments,
submissions, and grades while maintaining FERPA compliance.
"""

import os
import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import time
import json
from urllib.parse import urljoin

class CanvasCollector:
    """Canvas LMS API client for educational data collection."""
    
    def __init__(self):
        self.api_url = os.getenv('CANVAS_API_URL')
        self.api_token = os.getenv('CANVAS_API_TOKEN')
        self.logger = logging.getLogger(__name__)
        
        if not self.api_url or not self.api_token:
            raise ValueError("Canvas API URL and token must be provided")
        
        self.headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        # Rate limiting
        self.rate_limit_delay = 0.1  # 100ms between requests
        self.max_retries = 3
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated request to Canvas API with rate limiting and error handling."""
        url = urljoin(self.api_url, endpoint)
        
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.rate_limit_delay)
                response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    wait_time = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 401:
                    raise Exception("Canvas API authentication failed")
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return {}
    
    def _paginate_request(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Handle paginated Canvas API responses."""
        all_data = []
        page = 1
        per_page = 100
        
        if params is None:
            params = {}
        
        params.update({'page': page, 'per_page': per_page})
        
        while True:
            try:
                data = self._make_request(endpoint, params)
                
                if not data or len(data) == 0:
                    break
                
                all_data.extend(data)
                
                # Check if we got a full page (indicating more data might be available)
                if len(data) < per_page:
                    break
                
                page += 1
                params['page'] = page
                
            except Exception as e:
                self.logger.error(f"Pagination failed at page {page}: {str(e)}")
                break
        
        return all_data
    
    def get_courses(self, enrollment_state: str = 'active') -> List[Dict[str, Any]]:
        """Get all courses from Canvas."""
        self.logger.info("Collecting Canvas courses")
        
        try:
            params = {
                'enrollment_state': enrollment_state,
                'include': ['term', 'course_progress', 'storage_quota_used_mb', 'total_students']
            }
            
            courses = self._paginate_request('courses', params)
            
            # Enrich course data
            enriched_courses = []
            for course in courses:
                enriched_course = {
                    'course_id': course.get('id'),
                    'name': course.get('name'),
                    'course_code': course.get('course_code'),
                    'sis_course_id': course.get('sis_course_id'),
                    'enrollment_term_id': course.get('enrollment_term_id'),
                    'start_at': course.get('start_at'),
                    'end_at': course.get('end_at'),
                    'created_at': course.get('created_at'),
                    'updated_at': course.get('updated_at'),
                    'total_students': course.get('total_students', 0),
                    'storage_quota_mb': course.get('storage_quota_mb', 0),
                    'storage_quota_used_mb': course.get('storage_quota_used_mb', 0),
                    'workflow_state': course.get('workflow_state'),
                    'collected_at': datetime.utcnow().isoformat()
                }
                enriched_courses.append(enriched_course)
            
            self.logger.info(f"Collected {len(enriched_courses)} courses from Canvas")
            return enriched_courses
            
        except Exception as e:
            self.logger.error(f"Failed to collect Canvas courses: {str(e)}")
            raise
    
    def get_students(self) -> List[Dict[str, Any]]:
        """Get all students from Canvas courses."""
        self.logger.info("Collecting Canvas students")
        
        try:
            courses = self.get_courses()
            all_students = {}  # Use dict to avoid duplicates
            
            for course in courses:
                course_id = course['course_id']
                
                try:
                    # Get enrollments for this course
                    enrollments = self._paginate_request(
                        f'courses/{course_id}/enrollments',
                        {'type': ['StudentEnrollment'], 'include': ['user']}
                    )
                    
                    for enrollment in enrollments:
                        user = enrollment.get('user', {})
                        user_id = user.get('id')
                        
                        if user_id and user_id not in all_students:
                            student_data = {
                                'student_id': user_id,
                                'sis_user_id': user.get('sis_user_id'),
                                'name': user.get('name'),
                                'sortable_name': user.get('sortable_name'),
                                'short_name': user.get('short_name'),
                                'email': user.get('email'),  # Will be anonymized later
                                'login_id': user.get('login_id'),  # Will be anonymized later
                                'created_at': user.get('created_at'),
                                'last_login': user.get('last_login'),
                                'time_zone': user.get('time_zone'),
                                'locale': user.get('locale'),
                                'collected_at': datetime.utcnow().isoformat()
                            }
                            all_students[user_id] = student_data
                            
                except Exception as e:
                    self.logger.warning(f"Failed to get students for course {course_id}: {str(e)}")
                    continue
            
            students_list = list(all_students.values())
            self.logger.info(f"Collected {len(students_list)} unique students from Canvas")
            return students_list
            
        except Exception as e:
            self.logger.error(f"Failed to collect Canvas students: {str(e)}")
            raise
    
    def get_enrollments(self) -> List[Dict[str, Any]]:
        """Get enrollment data from Canvas."""
        self.logger.info("Collecting Canvas enrollments")
        
        try:
            courses = self.get_courses()
            all_enrollments = []
            
            for course in courses:
                course_id = course['course_id']
                
                try:
                    enrollments = self._paginate_request(
                        f'courses/{course_id}/enrollments',
                        {'include': ['user', 'grades']}
                    )
                    
                    for enrollment in enrollments:
                        enrollment_data = {
                            'enrollment_id': enrollment.get('id'),
                            'course_id': course_id,
                            'user_id': enrollment.get('user_id'),
                            'type': enrollment.get('type'),
                            'role': enrollment.get('role'),
                            'enrollment_state': enrollment.get('enrollment_state'),
                            'created_at': enrollment.get('created_at'),
                            'updated_at': enrollment.get('updated_at'),
                            'start_at': enrollment.get('start_at'),
                            'end_at': enrollment.get('end_at'),
                            'current_score': enrollment.get('grades', {}).get('current_score'),
                            'final_score': enrollment.get('grades', {}).get('final_score'),
                            'current_grade': enrollment.get('grades', {}).get('current_grade'),
                            'final_grade': enrollment.get('grades', {}).get('final_grade'),
                            'collected_at': datetime.utcnow().isoformat()
                        }
                        all_enrollments.append(enrollment_data)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to get enrollments for course {course_id}: {str(e)}")
                    continue
            
            self.logger.info(f"Collected {len(all_enrollments)} enrollments from Canvas")
            return all_enrollments
            
        except Exception as e:
            self.logger.error(f"Failed to collect Canvas enrollments: {str(e)}")
            raise
    
    def get_assignments(self) -> List[Dict[str, Any]]:
        """Get assignment data from Canvas."""
        self.logger.info("Collecting Canvas assignments")
        
        try:
            courses = self.get_courses()
            all_assignments = []
            
            for course in courses:
                course_id = course['course_id']
                
                try:
                    assignments = self._paginate_request(
                        f'courses/{course_id}/assignments',
                        {'include': ['submission']}
                    )
                    
                    for assignment in assignments:
                        assignment_data = {
                            'assignment_id': assignment.get('id'),
                            'course_id': course_id,
                            'name': assignment.get('name'),
                            'description': assignment.get('description'),
                            'points_possible': assignment.get('points_possible'),
                            'grading_type': assignment.get('grading_type'),
                            'submission_types': assignment.get('submission_types'),
                            'due_at': assignment.get('due_at'),
                            'unlock_at': assignment.get('unlock_at'),
                            'lock_at': assignment.get('lock_at'),
                            'created_at': assignment.get('created_at'),
                            'updated_at': assignment.get('updated_at'),
                            'published': assignment.get('published'),
                            'workflow_state': assignment.get('workflow_state'),
                            'collected_at': datetime.utcnow().isoformat()
                        }
                        all_assignments.append(assignment_data)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to get assignments for course {course_id}: {str(e)}")
                    continue
            
            self.logger.info(f"Collected {len(all_assignments)} assignments from Canvas")
            return all_assignments
            
        except Exception as e:
            self.logger.error(f"Failed to collect Canvas assignments: {str(e)}")
            raise
    
    def get_submissions(self) -> List[Dict[str, Any]]:
        """Get submission data from Canvas."""
        self.logger.info("Collecting Canvas submissions")
        
        try:
            assignments = self.get_assignments()
            all_submissions = []
            
            for assignment in assignments:
                course_id = assignment['course_id']
                assignment_id = assignment['assignment_id']
                
                try:
                    submissions = self._paginate_request(
                        f'courses/{course_id}/assignments/{assignment_id}/submissions',
                        {'include': ['user']}
                    )
                    
                    for submission in submissions:
                        submission_data = {
                            'submission_id': submission.get('id'),
                            'assignment_id': assignment_id,
                            'course_id': course_id,
                            'user_id': submission.get('user_id'),
                            'submitted_at': submission.get('submitted_at'),
                            'score': submission.get('score'),
                            'grade': submission.get('grade'),
                            'attempt': submission.get('attempt'),
                            'workflow_state': submission.get('workflow_state'),
                            'submission_type': submission.get('submission_type'),
                            'late': submission.get('late'),
                            'missing': submission.get('missing'),
                            'excused': submission.get('excused'),
                            'seconds_late': submission.get('seconds_late'),
                            'graded_at': submission.get('graded_at'),
                            'collected_at': datetime.utcnow().isoformat()
                        }
                        all_submissions.append(submission_data)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to get submissions for assignment {assignment_id}: {str(e)}")
                    continue
            
            self.logger.info(f"Collected {len(all_submissions)} submissions from Canvas")
            return all_submissions
            
        except Exception as e:
            self.logger.error(f"Failed to collect Canvas submissions: {str(e)}")
            raise
    
    def get_grades(self) -> List[Dict[str, Any]]:
        """Get grade data from Canvas."""
        self.logger.info("Collecting Canvas grades")
        
        try:
            enrollments = self.get_enrollments()
            all_grades = []
            
            for enrollment in enrollments:
                if enrollment['type'] == 'StudentEnrollment':
                    grade_data = {
                        'enrollment_id': enrollment['enrollment_id'],
                        'course_id': enrollment['course_id'],
                        'user_id': enrollment['user_id'],
                        'current_score': enrollment['current_score'],
                        'final_score': enrollment['final_score'],
                        'current_grade': enrollment['current_grade'],
                        'final_grade': enrollment['final_grade'],
                        'collected_at': datetime.utcnow().isoformat()
                    }
                    all_grades.append(grade_data)
            
            self.logger.info(f"Collected {len(all_grades)} grade records from Canvas")
            return all_grades
            
        except Exception as e:
            self.logger.error(f"Failed to collect Canvas grades: {str(e)}")
            raise
    
    def get_course_analytics(self, course_id: int) -> Dict[str, Any]:
        """Get analytics data for a specific course."""
        try:
            analytics = self._make_request(f'courses/{course_id}/analytics/current')
            return analytics
        except Exception as e:
            self.logger.warning(f"Failed to get analytics for course {course_id}: {str(e)}")
            return {}
    
    def get_user_activity(self, user_id: int) -> List[Dict[str, Any]]:
        """Get user activity data."""
        try:
            activity = self._paginate_request(f'users/{user_id}/page_views')
            return activity
        except Exception as e:
            self.logger.warning(f"Failed to get activity for user {user_id}: {str(e)}")
            return []
    
    def test_connection(self) -> bool:
        """Test Canvas API connection."""
        try:
            response = self._make_request('accounts/self')
            if response:
                self.logger.info("Canvas API connection successful")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Canvas API connection failed: {str(e)}")
            return False