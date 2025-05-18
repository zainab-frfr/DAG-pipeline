import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime

fake = Faker()

NUM_ROWS = 10000
NUM_DEPARTMENTS = 10
NUM_FACULTIES = 300
NUM_COURSES = 100
NUM_SEMESTERS = 6
NUM_CLASSROOMS = 30

def generate_ids(prefix, count):
    return [f"{prefix}{str(i).zfill(4)}" for i in range(1, count + 1)]

department_ids = generate_ids("D", NUM_DEPARTMENTS)
faculty_ids = generate_ids("F", NUM_FACULTIES)
student_ids = generate_ids("S", NUM_ROWS)
course_ids = generate_ids("C", NUM_COURSES)
semester_ids = [f"SEM{i+1}" for i in range(NUM_SEMESTERS)]
room_ids = generate_ids("R", NUM_CLASSROOMS)

# Departments
departments = pd.DataFrame({
    "DepartmentID": department_ids,
    "DepartmentName": [random.choice(["CompSci", "Computer Science", "CS", "Comp Science"]) if i % 7 == 0 else fake.unique.company() for i in range(NUM_DEPARTMENTS)],
    "Location": [fake.city() for _ in range(NUM_DEPARTMENTS)],
})
departments.loc[0, "DepartmentName"] = None  # inconsistency

# Faculties
faculties = pd.DataFrame({
    "FacultyID": faculty_ids,
    "Name": [fake.name() for _ in range(NUM_FACULTIES)],
    "HireDate": [fake.date_between(start_date='-10y', end_date='-1y') for _ in range(NUM_FACULTIES)],
    "Specialization": [random.choice(["AI", "A.I", "Artificial Intelligence", None]) if i % 10 == 0 else fake.job() for i in range(NUM_FACULTIES)],
    "DepartmentID": np.random.choice(department_ids, NUM_FACULTIES)
})

# Students
students = pd.DataFrame({
    "StudentID": student_ids,
    "Name": [fake.name() for _ in range(NUM_ROWS)],
    "Gender": np.random.choice(["Male", "Female", "Other", "M", "F"], NUM_ROWS),
    "DOB": [fake.date_of_birth(minimum_age=18, maximum_age=30) if i % 50 != 0 else "31/02/2000" for i in range(NUM_ROWS)],
    "Nationality": [fake.country() for _ in range(NUM_ROWS)],
    "AdmissionDate": [fake.date_between(start_date='-6y', end_date='-1y') for _ in range(NUM_ROWS)],
    "DepartmentID": np.random.choice(department_ids + ["D9999"], NUM_ROWS)  # inconsistency
})

# Courses
courses = pd.DataFrame({
    "CourseID": course_ids,
    "Title": [fake.catch_phrase() if i % 8 != 0 else "" for i in range(NUM_COURSES)],  # some blank titles
    "Credits": np.random.choice([2, 3, 4, 5, -1], NUM_COURSES),  # some invalid credits
    "Type": np.random.choice(["Theory", "Lab", "lecture", None], NUM_COURSES),
    "DepartmentID": np.random.choice(department_ids, NUM_COURSES)
})

# Semesters
semesters = pd.DataFrame({
    "SemesterID": semester_ids,
    "AcademicYear": [f"20{20+i}" for i in range(NUM_SEMESTERS)],
    "Term": np.random.choice(["Spring", "Fall", "Sprng", "summer"], NUM_SEMESTERS),
    "StartDate": [datetime(2020+i, 1, 10).date() for i in range(NUM_SEMESTERS)],
    "EndDate": [datetime(2020+i, 5, 10).date() for i in range(NUM_SEMESTERS)],
})

# Classrooms
classrooms = pd.DataFrame({
    "RoomID": room_ids,
    "BuildingName": [fake.street_name() for _ in range(NUM_CLASSROOMS)],
    "Capacity": np.random.choice([20, 30, 50, 100, 0], NUM_CLASSROOMS),  # zero capacity inconsistency
    "ResourceType": np.random.choice(["Lab", "Lecture Hall", "Class", None], NUM_CLASSROOMS)
})

# Student-Course Enrollments
student_course_enrollments = pd.DataFrame({
    "StudentID": np.random.choice(student_ids + ["S9999"], NUM_ROWS),
    "CourseID": np.random.choice(course_ids + ["C9999"], NUM_ROWS),
    "SemesterID": np.random.choice(semester_ids + ["INVALID_SEM"], NUM_ROWS),
    "EnrollmentStatus": np.random.choice(["Active", "Dropped", "active", "dropped", None], NUM_ROWS)
})

# Save initial main tables
with pd.ExcelWriter("university_data_with_inconsistencies.xlsx", engine='xlsxwriter') as writer:
    departments.to_excel(writer, sheet_name="departments", index=False)
    faculties.to_excel(writer, sheet_name="faculties", index=False)
    students.to_excel(writer, sheet_name="students", index=False)
    courses.to_excel(writer, sheet_name="courses", index=False)
    semesters.to_excel(writer, sheet_name="semesters", index=False)
    classrooms.to_excel(writer, sheet_name="classrooms", index=False)
    student_course_enrollments.to_excel(writer, sheet_name="student_course_enrollments", index=False)

# Additional tables
faculty_course_assignments = pd.DataFrame({
    "FacultyID": np.random.choice(faculty_ids + ["F9999"], NUM_ROWS),
    "CourseID": np.random.choice(course_ids + ["C9999"], NUM_ROWS),
    "SemesterID": np.random.choice(semester_ids + ["INVALID_SEM"], NUM_ROWS),
    "Role": np.random.choice(["Instructor", "TA", "instructor", "Teaching Assistant", None], NUM_ROWS)
})

attendance_records = pd.DataFrame({
    "StudentID": np.random.choice(student_ids + ["S0000"], NUM_ROWS),
    "CourseID": np.random.choice(course_ids + ["C0000"], NUM_ROWS),
    "Date": [fake.date_between(start_date='-6y', end_date='today') for _ in range(NUM_ROWS)],
    "Status": np.random.choice(["Present", "Absent", "Late", "P", "A", None], NUM_ROWS)
})

exam_results = pd.DataFrame({
    "StudentID": np.random.choice(student_ids + ["S1111"], NUM_ROWS),
    "CourseID": np.random.choice(course_ids + ["C1111"], NUM_ROWS),
    "ExamType": np.random.choice(["Midterm", "Final", "Quiz", "final", "mid"], NUM_ROWS),
    "Score": np.random.choice(list(range(-10, 110)), NUM_ROWS),
    "MaxScore": np.random.choice([100, 50, 75, 80, 0], NUM_ROWS),
    "Date": [fake.date_between(start_date='-6y', end_date='today') for _ in range(NUM_ROWS)]
})

feedback_responses = pd.DataFrame({
    "StudentID": np.random.choice(student_ids + ["S2222"], NUM_ROWS),
    "CourseID": np.random.choice(course_ids + ["C2222"], NUM_ROWS),
    "FacultyID": np.random.choice(faculty_ids + ["F2222"], NUM_ROWS),
    "SemesterID": np.random.choice(semester_ids + ["SEM999"], NUM_ROWS),
    "Rating": np.random.choice([1, 2, 3, 4, 5, 6, 0, None], NUM_ROWS),
    "Comments": [fake.sentence() if i % 10 != 0 else None for i in range(NUM_ROWS)]
})

exam_schedule = pd.DataFrame({
    "ExamID": generate_ids("E", NUM_ROWS),
    "CourseID": np.random.choice(course_ids + ["INVALID_COURSE"], NUM_ROWS),
    "FacultyID": np.random.choice(faculty_ids + ["INVALID_FAC"], NUM_ROWS),
    "RoomID": np.random.choice(room_ids + ["INVALID_ROOM"], NUM_ROWS),
    "ScheduledDate": [fake.date_between(start_date='-6y', end_date='today') for _ in range(NUM_ROWS)],
    "Type": np.random.choice(["Midterm", "Final", "Quiz", "Assessment", None], NUM_ROWS)
})

grades = pd.DataFrame({
    "GradeID": generate_ids("G", 10),
    "ScoreRange": ["90-100", "80-89", "70-79", "60-69", "50-59", "40-49", "30-39", "20-29", "10-19", "0-9"],
    "Grade": ["A", "B+", "B", "C+", "C", "D+", "D", "E", "F", "F-"],
    "GPA": [4.0, 3.7, 3.3, 3.0, 2.7, 2.3, 2.0, 1.7, 1.0, 0.0]
})

student_dropout_log = pd.DataFrame({
    "StudentID": np.random.choice(student_ids + ["S3333"], NUM_ROWS),
    "DropoutDate": [fake.date_between(start_date='-6y', end_date='-1y') for _ in range(NUM_ROWS)],
    "Reason": np.random.choice(["Financial", "Academic", "Transfer", "", None], NUM_ROWS),
    "LastSemester": np.random.choice(semester_ids + ["SEM000"], NUM_ROWS)
})

course_prerequisites = pd.DataFrame({
    "CourseID": np.random.choice(course_ids, NUM_ROWS),
    "PrerequisiteCourseID": np.random.choice(course_ids + ["INVALID_COURSE"], NUM_ROWS)
})

with pd.ExcelWriter("university_data_with_inconsistencies_full.xlsx", engine='xlsxwriter') as writer:
    faculty_course_assignments.to_excel(writer, sheet_name="faculty_course_assignments", index=False)
    attendance_records.to_excel(writer, sheet_name="attendance_records", index=False)
    exam_results.to_excel(writer, sheet_name="exam_results", index=False)
    feedback_responses.to_excel(writer, sheet_name="feedback_responses", index=False)
    exam_schedule.to_excel(writer, sheet_name="exam_schedule", index=False)
    grades.to_excel(writer, sheet_name="grades", index=False)
    student_dropout_log.to_excel(writer, sheet_name="student_dropout_log", index=False)
    course_prerequisites.to_excel(writer, sheet_name="course_prerequisites", index=False)
