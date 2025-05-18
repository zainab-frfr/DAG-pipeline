import pandas as pd
import os

CLEANED_DATA_DIR = "/home/zainab/university_dwh_project/data/cleaned"
TRANSFORMED_DATA_DIR = "/home/zainab/university_dwh_project/data/transformed"

def calculate_attendance_rate(attendance_df):
    attendance_df['Status'] = attendance_df['Status'].str.upper().str.strip()
    attended_statuses = ['PRESENT', 'LATE']
    attendance_df['Attended'] = attendance_df['Status'].apply(lambda x: 1 if x in attended_statuses else 0)

    attendance_rate = attendance_df.groupby(['StudentID', 'CourseID']).agg(
        AttendedSessions=('Attended', 'sum'),
        TotalSessions=('Status', 'count')
    ).reset_index()

    attendance_rate['AttendanceRate'] = attendance_rate['AttendedSessions'] / attendance_rate['TotalSessions']
    return attendance_rate[['StudentID', 'CourseID', 'AttendanceRate']]

def transform_data():
    # Load cleaned CSVs
    enrollments = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "student_course_enrollments_clean.csv"))
    faculty_assignments = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "faculty_course_assignments_clean.csv"))
    students = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "students_clean.csv"))
    feedback_responses = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "feedback_responses_clean.csv"))
    exam_results = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "exam_results_clean.csv"))
    attendance = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "attendance_records_clean.csv"))
    dropouts = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "student_dropout_log_clean.csv"))
    semesters = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "semesters_clean.csv"))
    courses = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "courses_clean.csv"))
    faculties = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "faculties_clean.csv"))

    # 1. Inner join enrollments and faculty_assignments on CourseID and SemesterID
    base = pd.merge(
        enrollments,
        faculty_assignments,
        on=['CourseID', 'SemesterID'],
        how='inner',
        suffixes=('_student', '_faculty')
    )

    # 2. Join with students to get AdmissionDate and create dim_date and DateID
    students['AdmissionDate'] = pd.to_datetime(students['AdmissionDate'], errors='coerce')
    base = base.merge(
        students[['StudentID', 'AdmissionDate']],
        on='StudentID',
        how='inner'
    )

    unique_dates = students['AdmissionDate'].dropna().drop_duplicates().sort_values().reset_index(drop=True)
    dim_date = pd.DataFrame({
        'DateID': range(1, len(unique_dates) + 1),
        'AdmissionDate': unique_dates
    })
    dim_date['AdmissionDay'] = dim_date['AdmissionDate'].dt.day
    dim_date['AdmissionMonth'] = dim_date['AdmissionDate'].dt.month
    dim_date['AdmissionYear'] = dim_date['AdmissionDate'].dt.year

    dateid_map = dim_date.set_index('AdmissionDate')['DateID']
    base['DateID'] = base['AdmissionDate'].map(dateid_map)

    # 3. Compute feedback deviation
    avg_feedback = feedback_responses.groupby("CourseID")["Rating"].mean().reset_index().rename(columns={"Rating": "AvgRating"})
    feedback_responses = feedback_responses.merge(avg_feedback, on="CourseID", how="left")
    feedback_responses["FeedbackDeviation"] = feedback_responses["Rating"] - feedback_responses["AvgRating"]
    feedback_trimmed = feedback_responses[["StudentID", "CourseID", "FeedbackDeviation"]]

    # 4. Calculate ExamScoreAvg per student and course
    exam_results['ExamScoreAvg'] = exam_results.groupby(
        ['StudentID', 'CourseID']
    )['Score'].transform('mean')

    # 5. Calculate attendance rate
    attendance_rate_df = calculate_attendance_rate(attendance)

    # 6. Build fact table
    fact = base[['StudentID', 'CourseID', 'SemesterID', 'FacultyID', 'DateID']].copy()
    fact = fact.merge(exam_results[['StudentID', 'CourseID', 'ExamScoreAvg']], on=['StudentID', 'CourseID'], how='left')
    fact = fact.merge(attendance_rate_df, on=['StudentID', 'CourseID'], how='left')
    fact = fact.merge(feedback_trimmed, on=['StudentID', 'CourseID'], how='left')
    fact = fact.merge(dropouts[['StudentID', 'DropoutDate']], on='StudentID', how='left')
    fact = fact.merge(semesters[['SemesterID', 'EndDate']], on='SemesterID', how='left')

    # 7. Fill missing AttendanceRate and FeedbackDeviation with 'Data Unrecorded' (convert to object)
    fact['AttendanceRate'] = fact['AttendanceRate'].astype(object).fillna('Data Unrecorded')
    fact['FeedbackDeviation'] = fact['FeedbackDeviation'].astype(object).fillna('Data Unrecorded')
    fact['ExamScoreAvg'] = fact['ExamScoreAvg'].fillna(0)

    # 8. Create DropoutFlag
    fact["DropoutFlag"] = (pd.to_datetime(fact["DropoutDate"], errors="coerce") > pd.to_datetime(fact["EndDate"], errors="coerce")).astype(int)

    # 9. Select final columns for fact table
    fact_final = fact[[
        "StudentID", "CourseID", "FacultyID", "SemesterID", "DateID",
        "ExamScoreAvg", "AttendanceRate", "FeedbackDeviation", "DropoutFlag"
    ]].copy()
    fact_final.insert(0, "FactID", range(1, len(fact_final) + 1))

    # 10. Drop AdmissionDate before saving dim_date
    dim_date = dim_date.drop(columns=['AdmissionDate'])

    # Drop AdmissionDate from dim_students
    students = students.drop(columns=['AdmissionDate'], errors='ignore')
    students = students.drop(columns=['DepartmentID'], errors='ignore')
    # Drop DepartmentID from dim_courses and dim_faculty
    courses = courses.drop(columns=['DepartmentID'], errors='ignore')
    faculties    = faculties.drop(columns=['DepartmentID'], errors='ignore')

    # 11. Save fact and dimension tables
    os.makedirs(TRANSFORMED_DATA_DIR, exist_ok=True)
    fact_final.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "fact_academic_engagement.csv"), index=False)
    students.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_students.csv"), index=False)
    courses.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_courses.csv"), index=False)
    faculties.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_faculty.csv"), index=False)
    semesters.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_semesters.csv"), index=False)
    dim_date.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_date.csv"), index=False)

    print(f"Transformation complete. Files saved in {TRANSFORMED_DATA_DIR}")

if __name__ == "__main__":
    transform_data()


# import pandas as pd
# import os

# CLEANED_DATA_DIR = "/home/zainab/university_dwh_project/data/cleaned"
# TRANSFORMED_DATA_DIR = "/home/zainab/university_dwh_project/data/transformed"

# def calculate_attendance_rate(attendance_df):
#     attendance_df['Status'] = attendance_df['Status'].str.upper().str.strip()
#     attended_statuses = ['PRESENT', 'LATE']
#     attendance_df['Attended'] = attendance_df['Status'].apply(lambda x: 1 if x in attended_statuses else 0)

#     attendance_rate = attendance_df.groupby(['StudentID', 'CourseID']).agg(
#         AttendedSessions=('Attended', 'sum'),
#         TotalSessions=('Status', 'count')
#     ).reset_index()

#     attendance_rate['AttendanceRate'] = attendance_rate['AttendedSessions'] / attendance_rate['TotalSessions']
#     return attendance_rate[['StudentID', 'CourseID', 'AttendanceRate']]

# def transform_data():
#     # Load cleaned CSVs
#     feedback_responses = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "feedback_responses_clean.csv"))
#     exam_results = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "exam_results_clean.csv"))
#     attendance = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "attendance_records_clean.csv"))
#     dropouts = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "student_dropout_log_clean.csv"))
#     semesters = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "semesters_clean.csv"))
#     students = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "students_clean.csv"))
#     courses = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "courses_clean.csv"))
#     faculties = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "faculties_clean.csv"))
#     enrollments = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "student_course_enrollments_clean.csv"))
#     faculty_assignments = pd.read_csv(os.path.join(CLEANED_DATA_DIR, "faculty_course_assignments_clean.csv"))

#     # Calculate ExamScoreAvg per student, course
#     exam_results['ExamScoreAvg'] = exam_results.groupby(
#         ['StudentID', 'CourseID']
#     )['Score'].transform('mean')

#     # Recompute FeedbackDeviation = student rating - avg rating per course
#     avg_feedback = feedback_responses.groupby("CourseID")["Rating"].mean().reset_index().rename(columns={"Rating": "AvgRating"})
#     feedback_responses = feedback_responses.merge(avg_feedback, on="CourseID", how="left")
#     feedback_responses["FeedbackDeviation"] = feedback_responses["Rating"] - feedback_responses["AvgRating"]
#     feedback_trimmed = feedback_responses[["StudentID", "CourseID", "FeedbackDeviation"]]

#     # Add SemesterID and FacultyID to exam_results
#     exam_results = exam_results.merge(
#         enrollments[['StudentID', 'CourseID', 'SemesterID']],
#         on=['StudentID', 'CourseID'],
#         how='left'
#     )
#     exam_results = exam_results.merge(
#         faculty_assignments[['CourseID', 'FacultyID']],
#         on='CourseID',
#         how='left'
#     )

#     # Calculate attendance rate from attendance records
#     attendance_rate_df = calculate_attendance_rate(attendance)

#     # Ensure AdmissionDate is datetime
#     students['AdmissionDate'] = pd.to_datetime(students['AdmissionDate'], errors='coerce')

#     # Create dim_date table: unique AdmissionDates with IDs
#     unique_dates = students['AdmissionDate'].dropna().drop_duplicates().sort_values().reset_index(drop=True)
#     dim_date = pd.DataFrame({
#         'DateID': range(1, len(unique_dates) + 1),
#         'AdmissionDate': unique_dates
#     })

#     # Extract date components
#     dim_date['AdmissionDay'] = dim_date['AdmissionDate'].dt.day
#     dim_date['AdmissionMonth'] = dim_date['AdmissionDate'].dt.month
#     dim_date['AdmissionYear'] = dim_date['AdmissionDate'].dt.year

#     # Map DateID back to students
#     dateid_map = dim_date.set_index('AdmissionDate')['DateID']
#     students['DateID'] = students['AdmissionDate'].map(dateid_map)

#     # Build fact table
#     fact = exam_results[['StudentID', 'CourseID', 'SemesterID', 'FacultyID', 'ExamScoreAvg']].copy()
#     fact = fact.merge(attendance_rate_df, on=['StudentID', 'CourseID'], how='left')
#     fact = fact.merge(feedback_trimmed, on=['StudentID', 'CourseID'], how='left')
#     fact = fact.merge(dropouts[['StudentID', 'DropoutDate']], on='StudentID', how='left')
#     fact = fact.merge(semesters[['SemesterID', 'EndDate']], on='SemesterID', how='left')
#     fact = fact.merge(students[['StudentID', 'DateID']], on='StudentID', how='left')

#     # Create DropoutFlag
#     fact["DropoutFlag"] = (pd.to_datetime(fact["DropoutDate"], errors="coerce") > pd.to_datetime(fact["EndDate"], errors="coerce")).astype(int)

#     # Select columns for final fact table
#     fact_final = fact[[
#         "StudentID", "CourseID", "FacultyID", "SemesterID", "DateID",
#         "ExamScoreAvg", "AttendanceRate", "FeedbackDeviation", "DropoutFlag"
#     ]].copy()

#     fact_final.insert(0, "FactID", range(1, len(fact_final) + 1))

#     # Remove 'AdmissionDate' column from dim_date before saving
#     dim_date = dim_date.drop(columns=['AdmissionDate'])

#     # Save fact and dimension tables to transformed directory
#     os.makedirs(TRANSFORMED_DATA_DIR, exist_ok=True)
#     fact_final.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "fact_academic_engagement.csv"), index=False)
#     students.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_students.csv"), index=False)
#     courses.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_courses.csv"), index=False)
#     faculties.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_faculty.csv"), index=False)
#     semesters.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_semesters.csv"), index=False)
#     dim_date.to_csv(os.path.join(TRANSFORMED_DATA_DIR, "dim_date.csv"), index=False)

#     print(f"Transformation complete. Files saved in {TRANSFORMED_DATA_DIR}")

# if __name__ == "__main__":
#     transform_data()
