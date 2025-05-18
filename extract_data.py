import pandas as pd
import os

def extract_excel(file_path, output_dir):
    """Extract all sheets from an Excel file into output_dir as CSVs"""
    print(f"[INFO] Extracting {file_path}...")
    df_sheets = pd.read_excel(file_path, sheet_name=None)

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for sheet_name, df in df_sheets.items():
        safe_name = sheet_name.strip().replace(" ", "_").lower()
        output_path = os.path.join(output_dir, f"{safe_name}_raw.csv")
        df.to_csv(output_path, index=False)
        print(f"[EXTRACTED] {sheet_name} → {output_path}")

def run():
    base_path = "/home/zainab/university_dwh_project/data"
    output_dir = "/home/zainab/university_dwh_project/data/extracted"

    files = [
        "university_data_with_inconsistencies.xlsx",
        "university_data_with_inconsistencies_full.xlsx"
    ]

    for f in files:
        full_path = os.path.join(base_path, f)
        if os.path.exists(full_path):
            extract_excel(full_path, output_dir)
        else:
            print(f"[ERROR] File not found: {full_path}")
run()