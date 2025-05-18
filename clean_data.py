import pandas as pd
import os
import re

DATA_DIR = "/home/zainab/university_dwh_project/data/extracted"
OUT_DIR = "/home/zainab/university_dwh_project/data/cleaned"

def clean_attendance():
    path = os.path.join(DATA_DIR, "attendance_records_raw.csv")
    df = pd.read_csv(path)

    # Convert Date to datetime
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Drop rows with missing Status
    df = df.dropna(subset=['Status'])

    # Normalize Status values
    df['Status'] = df['Status'].str.strip().str.upper()
    df['Status'] = df['Status'].replace({'P': 'PRESENT', 'A': 'ABSENT', 'PRESENT': 'PRESENT', 'ABSENT': 'ABSENT'})

    # Remove duplicates based on StudentID, CourseID, Date
    df.drop_duplicates(subset=['StudentID', 'CourseID', 'Date'], inplace=True)

    # Trim whitespace on IDs
    df['StudentID'] = df['StudentID'].str.strip()
    df['CourseID'] = df['CourseID'].str.strip()

    # Save cleaned data
    out_path = os.path.join(OUT_DIR, "attendance_records_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned attendance saved to {out_path}")

def clean_classrooms():
    path = os.path.join(DATA_DIR, "classrooms_raw.csv")
    df = pd.read_csv(path)

    # Drop rows with Capacity == 0
    df = df[df['Capacity'] != 0]

    # Calculate mode capacity per ResourceType
    modes = df.dropna(subset=['ResourceType']).groupby('ResourceType')['Capacity'].agg(lambda x: x.mode().iloc[0])

    def fill_resource_type(row):
        if pd.notna(row['ResourceType']):
            return row['ResourceType'].strip()
        # Find closest ResourceType by capacity
        diffs = (modes - row['Capacity']).abs()
        closest_type = diffs.idxmin()
        return closest_type

    df['ResourceType'] = df.apply(fill_resource_type, axis=1)

    # Trim whitespace for string columns
    df['RoomID'] = df['RoomID'].str.strip()
    df['BuildingName'] = df['BuildingName'].str.strip()
    df['ResourceType'] = df['ResourceType'].str.strip()

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    out_path = os.path.join(OUT_DIR, "classrooms_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned classrooms saved to {out_path}")

def clean_course_prerequisites():
    path = os.path.join(DATA_DIR, "course_prerequisites_raw.csv")
    df = pd.read_csv(path)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Trim whitespace and standardize case
    df['CourseID'] = df['CourseID'].str.strip()
    df['PrerequisiteCourseID'] = df['PrerequisiteCourseID'].str.strip()

    # Remove self-referencing rows
    df = df[df['CourseID'] != df['PrerequisiteCourseID']]

    out_path = os.path.join(OUT_DIR, "course_prerequisites_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned course prerequisites saved to {out_path}")

def clean_courses():
    path = os.path.join(DATA_DIR, "courses_raw.csv")
    df = pd.read_csv(path)

    # Fill missing Title and Type
    df['Title'] = df['Title'].fillna("Unknown Title").str.strip()
    df['Type'] = df['Type'].fillna("Unknown Type").str.strip()

    # Replace -1 credits with median (excluding -1)
    median_credits = df.loc[df['Credits'] != -1, 'Credits'].median()
    df.loc[df['Credits'] == -1, 'Credits'] = median_credits

    # Remove duplicates on CourseID
    df.drop_duplicates(subset=['CourseID'], inplace=True)

    out_path = os.path.join(OUT_DIR, "courses_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned courses saved to {out_path}")

def clean_departments():
    path = os.path.join(DATA_DIR, "departments_raw.csv")
    df = pd.read_csv(path)

    # Fill missing DepartmentName with "Unknown"
    df['DepartmentName'] = df['DepartmentName'].fillna("Unknown").str.strip()

    # Trim whitespace and standardize casing for other string columns
    df['DepartmentID'] = df['DepartmentID'].str.strip()
    df['Location'] = df['Location'].str.strip()

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "departments_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned departments saved to {out_path}")

def clean_exam_results():
    path = os.path.join(DATA_DIR, "exam_results_raw.csv")
    df = pd.read_csv(path)

    # Convert Date to datetime
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Replace negative scores with 0
    df.loc[df['Score'] < 0, 'Score'] = 0

    # Trim whitespace and lowercase ExamType
    df['ExamType'] = df['ExamType'].astype(str).str.strip().str.lower()

    # Map variants to standard labels
    examtype_map = {
        'mid': 'Midterm',
        'midterm': 'Midterm',
        'final': 'Final',
        'quiz': 'Quiz',
        'assessment': 'Assessment'
    }
    df['ExamType'] = df['ExamType'].map(examtype_map).fillna('Unknown')

    # Trim whitespace for StudentID and CourseID
    df['StudentID'] = df['StudentID'].str.strip()
    df['CourseID'] = df['CourseID'].str.strip()

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "exam_results_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned exam results saved to {out_path}")

def clean_exam_schedule():
    path = os.path.join(DATA_DIR, "exam_schedule_raw.csv")
    df = pd.read_csv(path)

    # Convert ScheduledDate to datetime
    df['ScheduledDate'] = pd.to_datetime(df['ScheduledDate'], errors='coerce')

    # Fill missing Type with 'Unknown Type'
    df['Type'] = df['Type'].fillna('Unknown Type').str.strip().str.title()

    # Trim whitespace on ID columns
    df['CourseID'] = df['CourseID'].str.strip()
    df['FacultyID'] = df['FacultyID'].str.strip()
    df['RoomID'] = df['RoomID'].str.strip()

    # Drop rows with invalid IDs
    df = df[
        (df['CourseID'] != 'INVALID_COURSE') &
        (df['FacultyID'] != 'INVALID_FAC') &
        (df['RoomID'] != 'INVALID_ROOM')
    ]

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "exam_schedule_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned exam schedule saved to {out_path}")

def clean_faculties():
    path = os.path.join(DATA_DIR, "faculties_raw.csv")
    df = pd.read_csv(path)

    # Convert HireDate to datetime
    df['HireDate'] = pd.to_datetime(df['HireDate'], errors='coerce')

    # Fill missing Specialization with 'Unknown'
    df['Specialization'] = df['Specialization'].fillna('Unknown').str.strip()

    # Trim whitespace on string columns
    df['FacultyID'] = df['FacultyID'].str.strip()
    df['Name'] = df['Name'].str.strip()
    df['DepartmentID'] = df['DepartmentID'].str.strip()

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "faculties_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned faculties saved to {out_path}")

def clean_faculty_course_assignments():
    path = os.path.join(DATA_DIR, "faculty_course_assignments_raw.csv")
    df = pd.read_csv(path)

    # Trim whitespace on string columns
    df['FacultyID'] = df['FacultyID'].str.strip()
    df['CourseID'] = df['CourseID'].str.strip()
    df['SemesterID'] = df['SemesterID'].str.strip()
    df['Role'] = df['Role'].astype(str).str.strip()

    # Fill missing Role with 'Unknown'
    df['Role'] = df['Role'].replace({'nan': None}).fillna('Unknown')

    # Standardize Role values
    role_map = {
        'instructor': 'Instructor',
        'Instructor': 'Instructor',
        'INSTRUCTOR': 'Instructor',
        'ta': 'Teaching Assistant',
        'TA': 'Teaching Assistant',
        'Teaching Assistant': 'Teaching Assistant',
        'teaching assistant': 'Teaching Assistant'
    }
    df['Role'] = df['Role'].replace(role_map)

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "faculty_course_assignments_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned faculty course assignments saved to {out_path}")

def clean_feedback_responses():
    path = os.path.join(DATA_DIR, "feedback_responses_raw.csv")
    df = pd.read_csv(path)

    # Drop rows with missing Rating
    df = df.dropna(subset=['Rating'])

    df['Comments'] = df['Comments'].fillna('No Comment').str.strip()

    for col in ['StudentID', 'CourseID', 'FacultyID', 'SemesterID']:
        df[col] = df[col].astype(str).str.strip()

    import re
    def valid_semester(sem):
        return bool(re.fullmatch(r'SEM\d{1,2}', sem))

    df = df[df['SemesterID'].apply(valid_semester)]

    df.drop_duplicates(inplace=True)

    out_path = os.path.join(OUT_DIR, "feedback_responses_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned feedback responses saved to {out_path}")



def clean_semesters():
    path = os.path.join(DATA_DIR, "semesters_raw.csv")
    df = pd.read_csv(path)

    # Fix typos in Term
    df['Term'] = df['Term'].str.strip().str.lower()
    df['Term'] = df['Term'].replace({'sprng': 'spring'})

    # Capitalize first letter of Term
    df['Term'] = df['Term'].str.capitalize()

    # Convert StartDate and EndDate to datetime
    df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
    df['EndDate'] = pd.to_datetime(df['EndDate'], errors='coerce')

    # Trim whitespace on string columns
    df['SemesterID'] = df['SemesterID'].str.strip()

    # Save cleaned data
    out_path = os.path.join(OUT_DIR, "semesters_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned semesters saved to {out_path}")

def clean_student_course_enrollments():
    path = os.path.join(DATA_DIR, "student_course_enrollments_raw.csv")
    df = pd.read_csv(path)

    # Trim whitespace on string columns
    for col in ['StudentID', 'CourseID', 'SemesterID', 'EnrollmentStatus']:
        df[col] = df[col].astype(str).str.strip()

    # Remove rows with invalid SemesterID (not matching SEM<number>)
    df = df[df['SemesterID'].str.match(r'^SEM\d+$', na=False)]

    # Fill missing EnrollmentStatus with 'Unknown'
    df['EnrollmentStatus'] = df['EnrollmentStatus'].replace({'nan': None}).fillna('Unknown')

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "student_course_enrollments_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned student course enrollments saved to {out_path}")

def clean_grades():
    path = os.path.join(DATA_DIR, "grades_raw.csv")
    df = pd.read_csv(path)

    # Strip whitespace first
    df['ScoreRange'] = df['ScoreRange'].astype(str).str.strip()

    # Keep only rows where ScoreRange matches pattern: digits - digits (e.g. 0-9, 80-89)
    pattern = re.compile(r'^\d{1,3}-\d{1,3}$')

    def valid_score_range(val):
        return bool(pattern.match(val))

    df = df[df['ScoreRange'].apply(valid_score_range)]

    # Trim other columns
    df['GradeID'] = df['GradeID'].str.strip()
    df['Grade'] = df['Grade'].str.strip()

    df.drop_duplicates(inplace=True)

    out_path = os.path.join(OUT_DIR, "grades_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned grades saved to {out_path}")


def clean_student_dropout_log():
    path = os.path.join(DATA_DIR, "student_dropout_log_raw.csv")
    df = pd.read_csv(path)

    # Convert DropoutDate to datetime
    df['DropoutDate'] = pd.to_datetime(df['DropoutDate'], errors='coerce')

    # Fill missing Reason with 'Unknown'
    df['Reason'] = df['Reason'].fillna('Unknown').str.strip()

    # Trim whitespace on string columns
    for col in ['StudentID', 'Reason', 'LastSemester']:
        df[col] = df[col].str.strip()

    # Optional: filter LastSemester matching SEM<number>
    df = df[df['LastSemester'].str.match(r'^SEM\d+$', na=False)]

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "student_dropout_log_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned student dropout log saved to {out_path}")

def clean_students():
    path = os.path.join(DATA_DIR, "students_raw.csv")
    df = pd.read_csv(path)

    # Convert DOB and AdmissionDate to datetime, coerce errors to NaT
    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce', dayfirst=True)
    df['AdmissionDate'] = pd.to_datetime(df['AdmissionDate'], errors='coerce')

    # Standardize Gender values
    gender_map = {
        'F': 'Female',
        'Female': 'Female',
        'M': 'Male',
        'Male': 'Male',
        'Other': 'Other',
        'O': 'Other'
    }
    df['Gender'] = df['Gender'].str.strip().map(gender_map).fillna('Unknown')

    # Trim whitespace on string columns
    for col in ['StudentID', 'Name', 'Nationality', 'DepartmentID']:
        df[col] = df[col].str.strip()

    # Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Save cleaned file
    out_path = os.path.join(OUT_DIR, "students_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Cleaned students saved to {out_path}")


def run():
    clean_attendance()
    clean_classrooms()
    clean_course_prerequisites()
    clean_courses()
    clean_departments()
    clean_exam_results()
    clean_exam_schedule()
    clean_faculties()
    clean_faculty_course_assignments()
    clean_feedback_responses()
    clean_grades()
    clean_semesters()
    clean_student_course_enrollments()
    clean_student_dropout_log()
    clean_students()

if __name__ == "__main__":
    run()

