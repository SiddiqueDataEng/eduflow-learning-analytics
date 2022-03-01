"""
Student Dashboard for EduFlow Learning Analytics

This Streamlit dashboard provides students with insights into their academic
performance, learning progress, and personalized recommendations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os

# Add utils to path
sys.path.append('/app/utils')
from mongodb_client import MongoDBClient

def main():
    st.set_page_config(
        page_title="EduFlow Student Dashboard",
        page_icon="🎓",
        layout="wide"
    )
    
    st.title("🎓 Student Learning Analytics Dashboard")
    st.markdown("Track your academic progress and get personalized insights")
    
    # Initialize MongoDB client
    try:
        mongo_client = MongoDBClient()
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return
    
    # Student selection (in production, this would be based on authentication)
    st.sidebar.header("Student Selection")
    
    # Get list of students (anonymized)
    try:
        students = mongo_client.get_collection_data('canvas_students', limit=100)
        if not students:
            st.warning("No student data available")
            return
        
        student_options = {
            student.get('anonymous_id', student.get('student_id', 'Unknown')): 
            student.get('name_hash', student.get('name', 'Unknown Student'))[:20]
            for student in students
        }
        
        selected_student_id = st.sidebar.selectbox(
            "Select Student",
            options=list(student_options.keys()),
            format_func=lambda x: student_options[x]
        )
        
    except Exception as e:
        st.error(f"Failed to load student data: {str(e)}")
        return
    
    if not selected_student_id:
        st.info("Please select a student to view dashboard")
        return
    
    # Load student data
    student_data = load_student_data(mongo_client, selected_student_id)
    
    if not student_data:
        st.warning("No data available for selected student")
        return
    
    # Dashboard sections
    display_overview(student_data)
    display_course_performance(student_data)
    display_engagement_metrics(student_data)
    display_recommendations(student_data)

def load_student_data(mongo_client, student_id):
    """Load comprehensive student data."""
    try:
        return mongo_client.get_student_data(student_id)
    except Exception as e:
        st.error(f"Failed to load student data: {str(e)}")
        return None
def display_overview(student_data):
    """Display student overview metrics."""
    st.header("📊 Academic Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate metrics
    enrollments = student_data.get('canvas_enrollments', [])
    submissions = student_data.get('canvas_submissions', [])
    attendance = student_data.get('attendance_records', [])
    
    active_courses = len([e for e in enrollments if e.get('enrollment_state') == 'active'])
    total_submissions = len(submissions)
    avg_score = sum(s.get('score', 0) for s in submissions if s.get('score')) / max(len(submissions), 1)
    attendance_rate = len([a for a in attendance if a.get('status') == 'present']) / max(len(attendance), 1) * 100
    
    with col1:
        st.metric("Active Courses", active_courses)
    
    with col2:
        st.metric("Total Submissions", total_submissions)
    
    with col3:
        st.metric("Average Score", f"{avg_score:.1f}%")
    
    with col4:
        st.metric("Attendance Rate", f"{attendance_rate:.1f}%")

def display_course_performance(student_data):
    """Display course performance analytics."""
    st.header("📚 Course Performance")
    
    enrollments = student_data.get('canvas_enrollments', [])
    
    if not enrollments:
        st.info("No course enrollment data available")
        return
    
    # Create performance DataFrame
    performance_data = []
    for enrollment in enrollments:
        if enrollment.get('type') == 'StudentEnrollment':
            performance_data.append({
                'Course': enrollment.get('course_id', 'Unknown'),
                'Current Score': enrollment.get('current_score', 0),
                'Final Score': enrollment.get('final_score', 0),
                'Grade': enrollment.get('current_grade', 'N/A')
            })
    
    if performance_data:
        df = pd.DataFrame(performance_data)
        
        # Performance chart
        fig = px.bar(df, x='Course', y='Current Score', 
                    title='Current Scores by Course',
                    color='Current Score',
                    color_continuous_scale='viridis')
        st.plotly_chart(fig, use_container_width=True)
        
        # Performance table
        st.subheader("Detailed Performance")
        st.dataframe(df, use_container_width=True)

def display_engagement_metrics(student_data):
    """Display student engagement analytics."""
    st.header("🎯 Engagement Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Attendance trend
        attendance = student_data.get('attendance_records', [])
        if attendance:
            attendance_df = pd.DataFrame(attendance)
            if 'date' in attendance_df.columns:
                attendance_df['date'] = pd.to_datetime(attendance_df['date'])
                attendance_trend = attendance_df.groupby('date').size().reset_index(name='count')
                
                fig = px.line(attendance_trend, x='date', y='count',
                            title='Attendance Trend')
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No attendance data available")
    
    with col2:
        # Library usage
        library_data = student_data.get('library_checkouts', [])
        if library_data:
            library_df = pd.DataFrame(library_data)
            
            # Monthly library usage
            if 'checkout_date' in library_df.columns:
                library_df['checkout_date'] = pd.to_datetime(library_df['checkout_date'])
                library_df['month'] = library_df['checkout_date'].dt.to_period('M')
                monthly_usage = library_df.groupby('month').size().reset_index(name='checkouts')
                
                fig = px.bar(monthly_usage, x='month', y='checkouts',
                           title='Monthly Library Usage')
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No library usage data available")

def display_recommendations(student_data):
    """Display personalized recommendations."""
    st.header("💡 Personalized Recommendations")
    
    recommendations = generate_recommendations(student_data)
    
    for i, rec in enumerate(recommendations, 1):
        with st.expander(f"Recommendation {i}: {rec['title']}"):
            st.write(rec['description'])
            if rec.get('action'):
                st.info(f"Action: {rec['action']}")

def generate_recommendations(student_data):
    """Generate personalized recommendations based on student data."""
    recommendations = []
    
    # Analyze performance
    enrollments = student_data.get('canvas_enrollments', [])
    submissions = student_data.get('canvas_submissions', [])
    attendance = student_data.get('attendance_records', [])
    
    # Low performance recommendation
    low_scores = [e for e in enrollments if e.get('current_score', 100) < 70]
    if low_scores:
        recommendations.append({
            'title': 'Improve Academic Performance',
            'description': f'You have {len(low_scores)} courses with scores below 70%. Consider seeking additional help.',
            'action': 'Schedule office hours with instructors or visit the tutoring center.'
        })
    
    # Attendance recommendation
    if attendance:
        present_count = len([a for a in attendance if a.get('status') == 'present'])
        attendance_rate = present_count / len(attendance) * 100
        
        if attendance_rate < 80:
            recommendations.append({
                'title': 'Improve Attendance',
                'description': f'Your attendance rate is {attendance_rate:.1f}%. Regular attendance is crucial for success.',
                'action': 'Set reminders for classes and prioritize attendance.'
            })
    
    # Engagement recommendation
    library_data = student_data.get('library_checkouts', [])
    if len(library_data) < 5:
        recommendations.append({
            'title': 'Increase Resource Usage',
            'description': 'Low library resource usage detected. Additional resources can enhance learning.',
            'action': 'Explore library databases and check out relevant books for your courses.'
        })
    
    # Default recommendation if no specific issues
    if not recommendations:
        recommendations.append({
            'title': 'Keep Up the Great Work!',
            'description': 'Your academic performance looks good. Continue with your current study habits.',
            'action': 'Consider challenging yourself with additional projects or advanced courses.'
        })
    
    return recommendations

if __name__ == "__main__":
    main()