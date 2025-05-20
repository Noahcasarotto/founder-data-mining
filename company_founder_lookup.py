import csv
import time
import os
import openai # Make sure to install this: pip install openai
from dotenv import load_dotenv # Make sure to install this: pip install python-dotenv
import requests # For HTTP requests: pip install requests
from bs4 import BeautifulSoup # For parsing HTML: pip install beautifulsoup4
import re # For regular expressions
from urllib.parse import quote_plus # For URL encoding search queries

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---\
INPUT_CSV_FILE = 'Unicorns_cleaned.csv'
OUTPUT_CSV_FILE = 'Unicorns_cleaned_with_founders.csv' # Output file
COMPANY_NAME_COLUMN = 'Company'
FOUNDERS_COLUMN = 'Founders' # Name of the column to add/update
LOG_FILE = 'founder_lookup_log.txt'
API_CALL_DELAY_SECONDS = 2       # Delay between OpenAI API calls
SCRAPE_DELAY_SECONDS = 3     # Delay between general web scraping requests
REQUEST_TIMEOUT_SECONDS = 15   # Timeout for web requests
MAX_SEARCH_SNIPPETS = 5        # Number of search result snippets to feed to OpenAI

NOT_FOUND_MARKER = "Not Found"
ERROR_MARKERS = [
    "Error_API_Call_Failed",
    "Error_OpenAI_Client_Not_Initialized",
    "Error_Unexpected_API",
    "Founders_Not_Yet_Looked_Up" # From earlier script versions if any
]

# --- OpenAI API Setup ---\
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = None
if OPENAI_API_KEY:
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        print("OpenAI client initialized successfully.")
    except Exception as e:
        print(f"Error initializing OpenAI client: {e}")
else:
    print("WARNING: OPENAI_API_KEY environment variable not found. OpenAI API calls will be skipped.")

COMMON_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
}

def log_message(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    full_message = f"{timestamp} - {message}"
    print(full_message)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(full_message + "\n")
    except Exception as e:
        print(f"Error writing to log file: {e}")

def clean_founder_string(text, company_name):
    """Cleans a string of founder names, removing boilerplate, normalizing separators."""
    if not text or not isinstance(text, str) or text.strip() == "":
        return NOT_FOUND_MARKER
    
    text = text.strip()
    normalized_text_lower = text.lower()

    # Check for known error/not found markers first
    if text in ERROR_MARKERS or \
       "not found" in normalized_text_lower or \
       "couldn't find" in normalized_text_lower or \
       "unable to find" in normalized_text_lower or \
       "no founder information" in normalized_text_lower or \
       "i do not have access to that information" in normalized_text_lower or \
       "i don't have access to that information" in normalized_text_lower or \
       "i cannot provide" in normalized_text_lower or \
       "information on the founders is not publicly available" in normalized_text_lower or \
       "does not have clearly defined founders in the traditional sense" in normalized_text_lower or \
       text == "N/A": # Common way LLMs might indicate not found
        return NOT_FOUND_MARKER

    # Remove common boilerplate phrases
    # Using re.escape on company_name in case it has special regex characters
    escaped_company_name = re.escape(company_name)
    phrases_to_remove = [
        f"The founders of {escaped_company_name} are typically listed as ",
        f"The founders of {escaped_company_name} are ",
        f"Founders of {escaped_company_name} are ",
        f"{escaped_company_name}'s founders are ",
        f"The founders of the company '{escaped_company_name}' are ",
        f"The founder of {escaped_company_name} is ",
        f"Founder of {escaped_company_name} is ",
        f"{escaped_company_name}'s founder is ",
        f"The founder of the company '{escaped_company_name}' is ",
        "The founders are typically listed as ",
        "The founders of this company are ",
        "The founders are ",
        "Founders are ",
        "The founder is ",
        "Founder is ",
        "Key figures associated with the founding include ",
        "The company was co-founded by ",
        "The company was founded by ",
        "Co-founded by ",
        "Founded by ",
    ]
    phrases_to_remove.extend([p.replace(" are", " include") for p in phrases_to_remove if " are" in p])
    phrases_to_remove.extend([p.replace(" is", " includes") for p in phrases_to_remove if " is" in p])

    for phrase in phrases_to_remove:
        text = re.sub(phrase, "", text, flags=re.IGNORECASE).strip()
    
    # Normalize separators: " and ", " & "
    text = re.sub(r'\s+(?:and|&)\s+', ", ", text, flags=re.IGNORECASE)
    text = text.replace(';', ',') # Replace semicolons with commas
    text = text.replace('•', ',') # Replace bullets with commas
    
    # Split by comma, then clean up each name
    names = [name.strip() for name in text.split(',') if name.strip() and len(name.strip()) > 1]
    
    seen = set()
    unique_names = []
    for name_part in names:
        cleaned_name = re.sub(r'\s*\(.*?\)\s*', '', name_part).strip()
        if cleaned_name.endswith("."):
            cleaned_name = cleaned_name[:-1].strip()
        
        if cleaned_name and cleaned_name.lower() != company_name.lower() and \
           len(cleaned_name.split()) < 5 and \
           not any(kw in cleaned_name.lower() for kw in ["various", "associates", "others", "group of", "team of", "llc", "inc", "co.", "limited", "corp", "gmbh"]):
            if cleaned_name not in seen:
                unique_names.append(cleaned_name)
                seen.add(cleaned_name)
    
    if not unique_names:
        return NOT_FOUND_MARKER
        
    return ", ".join(unique_names)

def search_duckduckgo_for_snippets(company_name):
    log_message(f"Searching DuckDuckGo for '{company_name}' founders...")
    search_query = f"{company_name} founders"
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}"
    
    snippets = []
    try:
        response = requests.get(url, headers=COMMON_REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        time.sleep(SCRAPE_DELAY_SECONDS)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        results = soup.find_all('div', class_='result__body')
        
        for i, result in enumerate(results):
            if i >= MAX_SEARCH_SNIPPETS:
                break
            title_tag = result.find('a', class_='result__a')
            snippet_tag = result.find('a', class_='result__snippet')
            title = title_tag.get_text(strip=True) if title_tag else ""
            snippet_text = snippet_tag.get_text(strip=True) if snippet_tag else ""
            if title or snippet_text:
                snippets.append(f"Title: {title}\nSnippet: {snippet_text}")
        
        if snippets:
            log_message(f"Found {len(snippets)} snippets from DuckDuckGo for '{company_name}'.")
        else:
            log_message(f"No snippets found on DuckDuckGo for '{company_name}'.")
        return "\n---\n".join(snippets)
        
    except requests.exceptions.RequestException as e:
        log_message(f"Error scraping DuckDuckGo for '{company_name}': {e}")
        return ""
    except Exception as e:
        log_message(f"Unexpected error during DuckDuckGo search for '{company_name}': {e}")
        return ""

def get_founders_via_openai_with_context(company_name, context_snippets):
    if not client:
        log_message("OpenAI client not initialized. Cannot call API.")
        return NOT_FOUND_MARKER

    log_message(f"Attempting to find founders for '{company_name}' via OpenAI API with search context...")
    try:
        prompt = f"Based on the following search result snippets, who are the founders of the company '{company_name}'? Please list their full names, separated by commas. Snippets:\n\n{context_snippets}\n\nIf you cannot determine the founders from this text, respond with only the text 'Not Found'.\""
        
        # Truncate prompt if too long (OpenAI has token limits)
        # A more sophisticated approach would use tiktoken to count tokens
        max_prompt_length = 3500 # Approximate character limit for safety
        if len(prompt) > max_prompt_length:
            prompt = prompt[:max_prompt_length] + "... [prompt truncated]"
            log_message(f"Prompt for {company_name} was truncated due to length.")

        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that extracts founder names from provided text. Respond with only comma-separated names or 'Not Found'."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0, # Sticking to facts from the text
            max_tokens=150
        )
        time.sleep(API_CALL_DELAY_SECONDS)
        response_text = completion.choices[0].message.content.strip()
        return response_text # Raw response, will be cleaned by clean_founder_string
        
    except openai.APIError as e:
        log_message(f"OpenAI API Error (with context) for '{company_name}': {e}")
        return ERROR_MARKERS[0]
    except Exception as e:
        log_message(f"Unexpected error during OpenAI API call (with context) for '{company_name}': {e}")
        return ERROR_MARKERS[2]

def get_founders_for_company(company_name):
    log_message(f"Starting founder search for '{company_name}'.")
    
    # Strategy 1: Search DuckDuckGo for snippets, then use OpenAI API to extract from snippets
    search_snippets = search_duckduckgo_for_snippets(company_name)
    
    if search_snippets and client:
        api_response_from_snippets = get_founders_via_openai_with_context(company_name, search_snippets)
        cleaned_founders = clean_founder_string(api_response_from_snippets, company_name)
        if cleaned_founders != NOT_FOUND_MARKER:
            log_message(f"Using founders from DuckDuckGo snippets + OpenAI API for '{company_name}': {cleaned_founders}")
            return cleaned_founders
    elif not client:
        log_message("OpenAI client not available, cannot process search snippets with AI.")

    # Fallback or if DDG search yielded nothing useful for the API:
    # Could add a direct Wikipedia scrape here again if desired, or another strategy.
    # For now, if the above fails, we mark as not found by this enhanced method.
    log_message(f"Could not find founders for '{company_name}' using DuckDuckGo + API method.")
    return NOT_FOUND_MARKER

def process_companies():
    log_message(f"Output will be written to: {OUTPUT_CSV_FILE}")
    output_file_exists = os.path.exists(OUTPUT_CSV_FILE)
    
    processed_companies_set = set()
    if output_file_exists:
        try:
            with open(OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as outfile_read:
                reader = csv.DictReader(outfile_read)
                reader.fieldnames = [fn.strip() for fn in reader.fieldnames if fn] if reader.fieldnames else []
                if COMPANY_NAME_COLUMN in reader.fieldnames:
                    for row in reader:
                        company_name_in_output = row.get(COMPANY_NAME_COLUMN)
                        if company_name_in_output:
                            processed_companies_set.add(company_name_in_output.strip())
            log_message(f"Found {len(processed_companies_set)} companies already in {OUTPUT_CSV_FILE}. Will skip these.")
        except Exception as e:
            log_message(f"Error reading existing output file {OUTPUT_CSV_FILE} for resume: {e}. Will start fresh or append.")
            output_file_exists = False 
            processed_companies_set.clear()

    try:
        with open(INPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as infile, \
             open(OUTPUT_CSV_FILE, mode='a' if output_file_exists and processed_companies_set else 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            original_fieldnames = [fn.strip() for fn in reader.fieldnames if fn] if reader.fieldnames else []

            if COMPANY_NAME_COLUMN not in original_fieldnames:
                log_message(f"ERROR: Column '{COMPANY_NAME_COLUMN}' not found in {INPUT_CSV_FILE}. Available: {original_fieldnames}")
                return

            # Ensure FOUNDERS_COLUMN is added if not present, or is the last one if it is
            output_fieldnames = [fn for fn in original_fieldnames if fn != FOUNDERS_COLUMN] + [FOUNDERS_COLUMN]
            
            writer = csv.DictWriter(outfile, fieldnames=output_fieldnames, lineterminator='\n')

            if not output_file_exists or (outfile.tell() == 0 and not processed_companies_set): # Write header if new file or empty & not appending to existing content
                writer.writeheader()
                log_message(f"Header written to {OUTPUT_CSV_FILE}")

            log_message(f"Starting company processing from {INPUT_CSV_FILE}...")
            companies_processed_this_run = 0
            for i, row in enumerate(reader):
                cleaned_row_input = {k.strip() if k and isinstance(k, str) else k: v for k, v in row.items()}
                company_name_original = cleaned_row_input.get(COMPANY_NAME_COLUMN)

                if not company_name_original:
                    log_message(f"Skipping row {i+2} in {INPUT_CSV_FILE}: missing company name.")
                    continue
                
                company_name_cleaned = company_name_original.strip()

                if company_name_cleaned in processed_companies_set:
                    continue
                
                companies_processed_this_run += 1
                log_message(f"--- Processing new company ({companies_processed_this_run}): '{company_name_original}' ---")
                
                founders_str = get_founders_for_company(company_name_cleaned)
                
                output_row_dict = {fn: cleaned_row_input.get(fn, '') for fn in original_fieldnames}
                output_row_dict[FOUNDERS_COLUMN] = founders_str
                
                final_output_row = {fn: output_row_dict.get(fn, '') for fn in output_fieldnames}
                writer.writerow(final_output_row)
                outfile.flush() 

        log_message(f"Finished processing. Total new companies processed in this run: {companies_processed_this_run}. Output in {OUTPUT_CSV_FILE}")

    except FileNotFoundError:
        log_message(f"ERROR: Input file {INPUT_CSV_FILE} not found.")
    except Exception as e:
        log_message(f"An error occurred during main processing loop: {e}")
        import traceback
        log_message(traceback.format_exc())


if __name__ == '__main__':
    # Initialize log file for the session
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"Founder Lookup Log - Session Start: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    if not OPENAI_API_KEY or not client:
        log_message("OpenAI API key not set or client failed to initialize. Founder lookup via API will be significantly impacted or skipped.")

    process_companies()