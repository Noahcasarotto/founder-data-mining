import csv
import re
import os
import time

# --- Configuration ---
INPUT_CSV_FILE = 'Unicorns_cleaned_with_founders.csv' # Output from the previous script
OUTPUT_CSV_FILE = 'Unicorns_founders_standardized.csv'
LOG_FILE = 'standardize_founders_log.txt'
COMPANY_NAME_COLUMN = 'Company' # Expected header for company name
FOUNDERS_COLUMN = 'Founders' # Expected header for founders data

NOT_FOUND_MARKER = "Not Found"
ERROR_MARKERS = [
    "Error_API_Call_Failed", 
    "Error_OpenAI_Client_Not_Initialized", 
    "Error_Unexpected_API",
    "Founders_Not_Yet_Looked_Up" # From earlier script versions
]

def log_message(message):
    """Appends a message to the log file and prints it."""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    full_message = f"{timestamp} - {message}"
    print(full_message)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(full_message + "\n")
    except Exception as e:
        print(f"Error writing to log file: {e}")

def clean_founder_data(raw_founder_text, company_name):
    """Cleans the raw founder text extracted from the previous script."""
    if not raw_founder_text or raw_founder_text.strip() == "":
        return NOT_FOUND_MARKER

    text = raw_founder_text.strip()

    # Check for known error/not found markers first
    if text in ERROR_MARKERS or "not found" in text.lower() or "couldn't find" in text.lower() or "unable to find" in text.lower():
        return NOT_FOUND_MARKER

    # Remove common boilerplate phrases more aggressively
    # Order matters here: remove more specific phrases first.
    phrases_to_remove = [
        f"The founders of {company_name} are ",
        f"Founders of {company_name} are ",
        f"{company_name}'s founders are ",
        f"The founders of the company '{company_name}' are ",
        "The founders are ",
        "Founders are ",
        f"The founder of {company_name} is ",
        f"Founder of {company_name} is ",
        f"{company_name}'s founder is ",
        f"The founder of the company '{company_name}' is ",
        "The founder is ",
        "Founder is ",
    ]
    for phrase in phrases_to_remove:
        # Case-insensitive removal of these phrases
        text = re.sub(re.escape(phrase), "", text, flags=re.IGNORECASE).strip()
    
    # Normalize separators: replace " and " with "," before splitting by comma
    # Also handle cases like "Name1, Name2 and Name3"
    text = re.sub(r'\s+and\s+', ", ", text, flags=re.IGNORECASE)
    
    # Split by comma, then clean up each name
    names = [name.strip() for name in text.split(',') if name.strip()]
    
    # Remove duplicates while preserving order (if desired, though order isn't strictly guaranteed here)
    seen = set()
    unique_names = []
    for name in names:
        # Further clean each name: remove trailing punctuation, possessives, etc.
        cleaned_name = name.strip()
        if cleaned_name.endswith("."): # Remove trailing period
            cleaned_name = cleaned_name[:-1].strip()
        if cleaned_name.endswith("'s"): # Remove possessive 's (e.g. from "Elon Musk's company")
            cleaned_name = cleaned_name[:-2].strip() 
        
        if cleaned_name and cleaned_name not in seen:
            # Basic check to avoid adding leftover boilerplate as a name
            if len(cleaned_name) > 1 and not cleaned_name.lower().startswith("the company was founded by") \
               and not cleaned_name.lower().startswith("founded by") \
               and not cleaned_name.lower() == company_name.lower():
                unique_names.append(cleaned_name)
                seen.add(cleaned_name)
    
    if not unique_names:
        # If after cleaning, no valid names remain, but original text was not a clear "not found"
        log_message(f"Could not extract valid founder names for '{company_name}' from: '{raw_founder_text}'. Marking as Not Found.")
        return NOT_FOUND_MARKER
        
    return ", ".join(unique_names)

def standardize_csv_data():
    """Reads the input CSV, cleans founder data, and writes to a new CSV."""
    log_message(f"Starting standardization process for {INPUT_CSV_FILE}...")
    processed_rows = []

    try:
        with open(INPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as infile:
            reader = csv.DictReader(infile)
            
            # Clean fieldnames from the input file
            original_fieldnames = [fn.strip() for fn in reader.fieldnames if fn is not None] if reader.fieldnames else []
            
            if COMPANY_NAME_COLUMN not in original_fieldnames or FOUNDERS_COLUMN not in original_fieldnames:
                log_message(f"ERROR: Required columns ('{COMPANY_NAME_COLUMN}', '{FOUNDERS_COLUMN}') not found in {INPUT_CSV_FILE}.")
                log_message(f"Available columns: {original_fieldnames}")
                return
            
            # The output fieldnames will be the same as input, as we are modifying in place
            output_fieldnames = original_fieldnames
            processed_rows.append(dict(zip(output_fieldnames, output_fieldnames))) # conceptual header

            for i, row in enumerate(reader):
                # Clean keys in the row dictionary, as they might have whitespace from input file
                cleaned_row_data = {k.strip() if k else k: v for k, v in row.items()}

                company_name = cleaned_row_data.get(COMPANY_NAME_COLUMN, "").strip()
                raw_founders = cleaned_row_data.get(FOUNDERS_COLUMN, "")

                if not company_name:
                    log_message(f"Skipping row {i+2} due to missing company name.")
                    # Keep other data, set founders to NOT_FOUND_MARKER
                    new_row = {fn: cleaned_row_data.get(fn, '') for fn in output_fieldnames if fn != FOUNDERS_COLUMN}
                    new_row[FOUNDERS_COLUMN] = NOT_FOUND_MARKER 
                    processed_rows.append(new_row)
                    continue
                
                log_message(f"Standardizing founders for company ({i+1}): {company_name}")
                cleaned_founders_str = clean_founder_data(raw_founders, company_name)
                
                # Create the new row for output
                output_row = {fn: cleaned_row_data.get(fn, '') for fn in output_fieldnames}
                output_row[FOUNDERS_COLUMN] = cleaned_founders_str
                processed_rows.append(output_row)

    except FileNotFoundError:
        log_message(f"ERROR: Input file {INPUT_CSV_FILE} not found.")
        return
    except Exception as e:
        log_message(f"An error occurred during CSV reading or processing: {e}")
        # Consider re-raising or handling more gracefully if it's a critical error
        return

    try:
        with open(OUTPUT_CSV_FILE, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)
            # Write the actual header first
            writer.writerow(processed_rows[0]) 
            # Write the data rows (excluding the conceptual header we added to processed_rows)
            writer.writerows(processed_rows[1:]) 
        log_message(f"Successfully standardized founder data. Output written to {OUTPUT_CSV_FILE}")
    except Exception as e:
        log_message(f"An error occurred while writing to {OUTPUT_CSV_FILE}: {e}")

if __name__ == '__main__':
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"Founder Standardization Log - Session Start: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    standardize_csv_data() 