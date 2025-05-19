import csv
import time
import os
import openai # Make sure to install this: pip install openai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
INPUT_CSV_FILE = 'Unicorns_cleaned.csv'
OUTPUT_CSV_FILE = 'Unicorns_cleaned_with_founders.csv'
COMPANY_NAME_COLUMN = 'Company' # Header of the company name column in your input CSV
LOG_FILE = 'founder_lookup_log.txt'
API_CALL_DELAY_SECONDS = 2 # Delay between API calls to respect rate limits

# --- OpenAI API Setup ---
# Ensure your OPENAI_API_KEY is set in your environment variables or a .env file
# For example, create a .env file in the same directory with: OPENAI_API_KEY="your_key_here"
# And run with: python -m dotenv run python company_founder_lookup.py (if using python-dotenv)
# Alternatively, set the environment variable directly in your shell.

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("ERROR: OPENAI_API_KEY environment variable not found. Please set it up.")
    # exit() # You might want to exit if the key isn't found, or handle it gracefully

client = None
if OPENAI_API_KEY:
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print(f"Error initializing OpenAI client: {e}")

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

def get_founders_via_openai(company_name):
    """
    Attempts to find founders of a company using the OpenAI API.
    Returns a comma-separated string of founder names or 'Error_API_Call_Failed' or 'Not_Found_By_AI'.
    """
    if not client:
        log_message("OpenAI client not initialized. Skipping API call.")
        return "Error_OpenAI_Client_Not_Initialized"

    log_message(f"Attempting to find founders for '{company_name}' via OpenAI API...")
    try:
        prompt = f"Who are the founders of the company '{company_name}'? Please list their full names, separated by commas. If you cannot find the founders, please respond with only the text 'Not Found'."
        
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo", # You can change the model if needed
            messages=[
                {"role": "system", "content": "You are a helpful assistant that provides founder names."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2, # Lower temperature for more factual, less creative answers
            max_tokens=100
        )
        
        time.sleep(API_CALL_DELAY_SECONDS) # Respect rate limits

        response_text = completion.choices[0].message.content.strip()
        
        if "not found" in response_text.lower() or not response_text:
            log_message(f"Founders for '{company_name}' not found by AI.")
            return "Not_Found_By_AI"
        
        # Simple cleaning: remove any introductory phrases if the model adds them.
        # This part might need refinement based on typical model responses.
        if "founders of" in response_text.lower() and ":" in response_text:
            response_text = response_text.split(":", 1)[-1].strip()
        if response_text.startswith("The founders are"):
            response_text = response_text.replace("The founders are", "").strip()
        if response_text.startswith("The founder is"):
            response_text = response_text.replace("The founder is", "").strip()
        if response_text.endswith("."): # Remove trailing period
            response_text = response_text[:-1]

        log_message(f"Founders for '{company_name}' from AI: {response_text}")
        return response_text
        
    except openai.APIError as e:
        log_message(f"OpenAI API Error for '{company_name}': {e}")
        return "Error_API_Call_Failed"
    except Exception as e:
        log_message(f"An unexpected error occurred during OpenAI API call for '{company_name}': {e}")
        return "Error_Unexpected_API"

def process_companies():
    """
    Reads the input CSV, looks up founders for each company using OpenAI API,
    and writes the results row-by-row to the output CSV.
    """
    if not OPENAI_API_KEY or not client:
        log_message("OpenAI API key or client is not configured. Cannot proceed with founder lookup.")
        return

    try:
        write_header = not os.path.exists(OUTPUT_CSV_FILE) or os.path.getsize(OUTPUT_CSV_FILE) == 0

        with open(INPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as infile, \
             open(OUTPUT_CSV_FILE, mode='a', encoding='utf-8', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            # Strip whitespace from field names
            reader.fieldnames = [fieldname.strip() for fieldname in reader.fieldnames if fieldname is not None]
            original_fieldnames = reader.fieldnames if reader.fieldnames else []
            
            if COMPANY_NAME_COLUMN not in original_fieldnames:
                log_message(f"ERROR: Company name column '{COMPANY_NAME_COLUMN}' not found in {INPUT_CSV_FILE} after stripping header whitespace.")
                log_message(f"Available columns: {original_fieldnames}")
                return

            output_fieldnames = original_fieldnames + ['Founders']
            writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)

            if write_header:
                writer.writeheader()
                log_message(f"Header written to {OUTPUT_CSV_FILE}")

            log_message(f"Starting to process companies from {INPUT_CSV_FILE}...")
            
            for i, row in enumerate(reader):
                # Strip whitespace from keys in the row dictionary as well
                cleaned_row = {k.strip(): v for k, v in row.items() if k is not None}

                company_name_original = cleaned_row.get(COMPANY_NAME_COLUMN)
                
                if not company_name_original:
                    log_message(f"Skipping row {i+2} in {INPUT_CSV_FILE} due to missing company name.")
                    new_row_data = {fn: cleaned_row.get(fn, '') for fn in original_fieldnames}
                    new_row_data['Founders'] = '' 
                    writer.writerow(new_row_data)
                    continue
                
                company_name_cleaned = company_name_original.strip()
                log_message(f"Processing company ({i+1}): '{company_name_original}' (cleaned: '{company_name_cleaned}')")
                
                founders_str = get_founders_via_openai(company_name_cleaned)
                
                new_row_data = {fn: cleaned_row.get(fn, '') for fn in original_fieldnames}
                new_row_data['Founders'] = founders_str
                writer.writerow(new_row_data)

        log_message(f"Successfully processed companies. Output appended to {OUTPUT_CSV_FILE}")

    except FileNotFoundError:
        log_message(f"ERROR: Input file {INPUT_CSV_FILE} not found.")
        return
    except Exception as e:
        log_message(f"An error occurred during CSV processing or writing: {e}")
        return

if __name__ == '__main__':
    # Clear or initialize log file at the start of a new run session
    # If you want to append to the log across multiple full runs, change 'w' to 'a'
    # and remove the initial log message write if appending.
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"Founder Lookup Log - Session Start: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    if not OPENAI_API_KEY:
        log_message("Script will not run process_companies() as OPENAI_API_KEY is not set.")
    else:
        process_companies() 