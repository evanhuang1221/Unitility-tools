from glob import glob
import tabula
import pandas as pd
import os
from tabulate import tabulate
from PyPDF2 import PdfReader
import re
import json

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows (be careful with large data)
pd.set_option('display.width', None)            # No line wrapping
pd.set_option('display.max_colwidth', None)     # Show full content in each cell

ENCODINGS_TO_TRY = ['utf-8', 'latin-1', 'cp1252']
def count_table_name_occurrences_in_pdf(pdf_path):
    table_names = []
    try:
        # Use PyPDF2 to read the PDF text
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
   
        # Count occurrences of "Table Name"
        
        lines = text.splitlines()  
        for line in lines:
            if "Table Name" in line:
                match = re.search(r"Table Name[::]?\s*(\S+)", line)
                if match:
                        table_names.append(match.group(1)) 
        print(f"Found {len(table_names)} 'Table Name' entries in the PDF.")                
        return table_names
    except Exception as e:
        return []

def pdf_to_markdown(pdf_path, output_dir):
    """
    Reads tables from a PDF using tabula with different modes and saves each table as a Markdown file.
    Prints diagnostic counts.
    """
    print(f"Reading tables from {pdf_path} using tabula...")
    detected_tables = []

    try:
       # If stream fails or gets too few tables, try 'lattice' mode (good for tables with clear grid lines)
       detected_tables = tabula.read_pdf(
           pdf_path,
           pages='all',
           multiple_tables=True,
           guess=True, # Still let tabula guess
           lattice=True, # Use lattice mode
           area=[30, 0, 842, 595] # Skip top 30 points to avoid watermark
       )
       tabula_mode_used = "lattice"
       print(f"Tabula (lattice mode) detected {len(detected_tables)} potential tables.")
    except Exception as tabula_lattice_error:
        print(f"Error during tabula lattice mode extraction: {tabula_lattice_error}")
        print("Tabula extraction failed in both stream and lattice modes.")
        tabula_mode_used = "Failed"
        detected_tables = [] # Ensure empty list if all attempts fail


    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    print(f"Saving tables to {output_dir}...")

    # Save each table as a Markdown file
    saved_count = 0
    skipped_count = 0
    for i, table in enumerate(detected_tables):
        markdown_file = os.path.join(output_dir, f'table_{i+1}.md')

        # Check if tabula returned a valid, non-empty DataFrame
        if not isinstance(table, pd.DataFrame) or table.empty:
             # print(f"Skipping table {i+1}: Not a DataFrame or is empty.")
             skipped_count += 1
             continue # Skip if not a DataFrame or is empty

        try:
            # Using tabulate to format the DataFrame into Markdown
            # showindex=False prevents pandas index from being written to MD
            # headers='keys' uses DataFrame column names
            markdown_output = tabulate(table, headers='keys', tablefmt='pipe', showindex=False)
            # Use utf-8 encoding when writing, as it's standard and avoids issues like 0x96
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(markdown_output)
            # f.write("\n\n") # Optional: add newline after each table
            saved_count += 1
        except Exception as tabulate_error:
            print(f"Error formatting or writing table {i+1} to Markdown ({markdown_file}): {tabulate_error}")
            skipped_count += 1


    print(f"--- Tabula Processing Summary ({tabula_mode_used} mode) ---")
    print(f"Tables detected by tabula: {len(detected_tables)}")
    print(f"Tables successfully saved as Markdown files: {saved_count}")
    print(f"Tables skipped (not DataFrame or empty, or write error): {skipped_count}")
    print("-" * 30)


# Function to count and extract "Table Name" from Markdown files using encoding fallback
def extract_table_names_in_markdown(folder):
    """
    Counts occurrences of "Table Name | ..." in Markdown files within a folder
    and extracts the value following the pipe. Handles different encodings.
    """
    print(f"Scanning markdown files in '{folder}' for 'Table Name | ...' pattern...")
    table_names = []
    total_count = 0

    # Find all markdown files in the folder
    files = sorted(glob(f"{folder}/*.md"))
    print(f"Found {len(files)} markdown files.")

    if not files:
        print(f"No markdown files found in '{folder}'.")
        return [] # Return empty list if no files found

    # Look for "Table Name" followed by optional space, pipe, optional space, and then the name
    markdown_pattern = r"Table Name\s*\|\s*(\S+)"
    
    for file in files:

        file_content = None # To hold the content after successful decoding

        # Try different encodings to read the file
        for encoding in ENCODINGS_TO_TRY:
            try:
                with open(file, 'r', encoding=encoding) as md_file:

                    file_content = md_file.read() # Read the whole file content
                # If reading was successful, break out of the encoding loop
                # print(f"Successfully read file '{os.path.basename(file)}' with encoding '{encoding}'.")
                break # Exit encoding loop for this file
            except UnicodeDecodeError:
                # print(f"Failed to decode '{os.path.basename(file)}' with encoding '{encoding}'. Trying next...")
                continue # Try the next encoding
            except FileNotFoundError:
                 print(f"Error: File not found during encoding test: {file}")
                 file_content = None # Ensure file_content is None to indicate failure
                 break # File not found, no need to try more encodings
            except Exception as e:
                 print(f"An unexpected error occurred reading file {file} with encoding {encoding}: {e}")
                 file_content = None # Ensure file_content is None
                 break # Stop trying encodings for this file


        # After trying encodings, process the content using regex if successfully read
        if file_content is not None:
            matches = re.findall(markdown_pattern, file_content)
            # For debugging, print which files contain "Table Name" and how many times
            # if matches:
            #    print(f"  Found {len(matches)} 'Table Name' in {os.path.basename(file)}")
            table_names.extend(matches) # Add found names to the main list
            total_count += len(matches)
             

        else:
            print(f"Warning: Could not read file '{os.path.basename(file)}' with any of the attempted encodings. Skipping scan for Table Names in this file.")
            # File not read, cannot count names from it

    # print(f"Finished scanning markdown files. Found {total_count} 'Table Name' entries across all files.")
    # The return value is the list, its length is the total count
    return table_names 

def extract_table_details_from_markdown(output_dir, output_json_path=None):
    files = sorted(glob(f"{output_dir}/*.md"), key=lambda x: int(re.search(r'table_(\d+)\.md', x).group(1)) if re.search(r'table_(\d+)\.md', x) else 0)
    results = []
    if not files:
        print(f"No markdown files found in '{output_dir}'.")
        return []
    in_key_field_section = False
    in_field_section = False
    table_name = None
    metadata = {}

    for file in files:
        try:
            # Read and filter out markdown separator lines before parsing
            with open(file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Filter out lines that contain markdown table separators (lines with dashes and colons)
            filtered_lines = []
            for line in lines:
                # Skip lines that are markdown table separators (contain pattern like ---|:--|--- or ":---":")
                line_stripped = line.strip()
                # Pattern 1: Traditional markdown separators like ---|:--|---
                pattern1 = re.match(r'^\s*\|[\s\-\:]+\|\s*$', line_stripped)
                # Pattern 2: JSON-style separators like ":-----": ":----..."
                pattern2 = re.match(r'^\s*"?:[^\|]*"?\s*:\s*"?:[^\|]*"?\s*,?\s*$', line_stripped)
                # Pattern 3: Lines with only colons, dashes, quotes and pipes
                pattern3 = re.match(r'^[\s\|\"\:\-\,]*$', line_stripped)
                
                if not (pattern1 or pattern2 or pattern3):
                    filtered_lines.append(line)
            
            # Use StringIO to create an in-memory file for pandas to read
            from io import StringIO
            filtered_content = ''.join(filtered_lines)
            
            # Read the filtered content with pandas without treating first row as header
            df = pd.read_csv(StringIO(filtered_content), sep='|', skipinitialspace=True, engine='python', header=None, encoding='utf-8', on_bad_lines='skip')

            # Rename columns by index, not by name
            df.columns = [f'column_{i}' for i in range(df.shape[1])]
            


            prev_row = None
            for idx, row in df.iterrows():
                # Safely get column values with proper error handling
                try:
                    col1 = str(row.get('column_1', '') if 'column_1' in df.columns else '').strip()    # entry indicator
                    col2 = str(row.get('column_2', '') if 'column_2' in df.columns else '').strip()    # 'Table Name' and field name 
                    col3 = str(row.get('column_3', '') if 'column_3' in df.columns else '').strip()    # Table name and data type 
                    col4 = str(row.get('column_4', '') if 'column_4' in df.columns else '').strip()    # description
                    col5 = str(row.get('column_5', '') if 'column_5' in df.columns else '').strip()    # foreign key or other info
                except Exception as row_error:
                    print(f"Error processing row {idx} in {file}: {row_error}")
                    continue
                # Detect table name
                if 'Table Name' in col2:
                    table_name = col3
                    results.append(table_name)  
                    in_field_section = False
                    in_key_field_section = False
                    key_field_obj = {}
                    field_info = {}
                    table_synonym=None
                    table_description=None
                    module_name=None
                    metadata[table_name] =    {
                        "Table Name": table_name,
                        "Table Synonym": table_synonym,
                        "Table Description": table_description,
                        "Module Name": module_name ,
                        "Key Fields": key_field_obj,
                        "Normal Fields": field_info }
                    if col3 == 'B_PG_PICBX4008_TDA_ACCRUAL':
                        print(f"Processing table: {table_name} in file {file}")

                    
                
                # update table synonym 
                elif 'Table Synonym' in col2 :
                    table_synonym = col3    
                    if metadata[table_name]:
                        metadata[table_name]["Table Synonym"] = table_synonym                 

                # update table description 
                elif 'Table Comments' in col2:
                    table_description = col3 
                    if metadata[table_name]:
                        metadata[table_name]["Table Description"] = table_description                     
                # update module name
                elif 'Module Name' in col2 :
                    module_name = col3 
                    if metadata[table_name]:
                        metadata[table_name]["Module Name"] = module_name                        

                # Detect start of key field section
                elif 'Key Field Name' in col2:

          
                    in_key_field_section = True
                    in_field_section = False
                    current_Key_Field_Fullname = None  # Initialize tracking variable

                    
                # Detect start of normal field section
                elif 'Field Name' in col2:
                    in_field_section = True
                    in_key_field_section = False
                    current_Normal_Field_Fullname = None  # Initialize tracking variable

                # Detect blank row (section end) or if next row is "Field Name"
                elif col2 is None and col3 is None and col4 is None:   
                    continue
                    
                else:
                    # Only process fields if we have a valid table_name
                    if not table_name:
                        continue
                        
                    # Process Key Fields
                    if in_key_field_section:
                        field = col2
                        data_type = col3
                        desc = col4
                        foreign_key=col5
                        if col1=='nan' and field and field!="nan" :  # Entry header indicator --col1 is nan                                                        
                            # Check if field name is likely get truncated (length >= 19)
                            if len(field) >= 19:   

                                # check if next row exists
                                if idx + 1 < len(df):
                                    next_row = df.iloc[idx + 1]
                                    next_field_entry_indicator = str(next_row.get('column_1', '') if 'column_1' in df.columns else '').strip()
                                    next_field_name_piece = str(next_row.get('column_2', '') if 'column_2' in df.columns else '').strip()                 
                                    
                                    # check if field name has continuation 
                                    if next_field_entry_indicator!='nan' and next_field_name_piece !='nan':
                                        current_Key_Field_Fullname = field + next_field_name_piece  # Concatenate field name

                                        metadata[table_name]["Key Fields"][current_Key_Field_Fullname] = {
                                            "type": data_type ,
                                            "description": desc ,
                                            "foreign_key": foreign_key if foreign_key != 'nan' else ''
                                        }
                        
                            else:  
                                current_Key_Field_Fullname = field                                
                                metadata[table_name]["Key Fields"][current_Key_Field_Fullname] = {
                                            "type": data_type ,
                                            "description": desc ,
                                            "foreign_key": foreign_key if foreign_key != 'nan' else ''
                                        }
              
                        else :
                            # This is a continuation line for the current key field
                            if current_Key_Field_Fullname and desc !="nan" and desc !="Unnamed: 3" and desc:
                                # Append description to the last key field
                                full_description = metadata[table_name]["Key Fields"][current_Key_Field_Fullname]["description"]+f" {desc}"
                                metadata[table_name]["Key Fields"][current_Key_Field_Fullname]["description"] = full_description
                                
                            
                    # Process normal fields
                    elif in_field_section:
                        field = col2
                        data_type = col3
                        desc = col4
                        foreign_key=col5 
 
                        # Check if col1 is 'nan' and field is not empty
                        if col1=='nan' and field and field!="nan" :  # Entry header indicator --col1 is nan                                                        
                            # Check if field name is likely get truncated (length >= 19)
                            if len(field) >= 19: 
                            
                                # check if next row exists
                                if idx + 1 < len(df):
                                    next_row = df.iloc[idx + 1]
                                    next_field_entry_indicator = str(next_row.get('column_1', '') if 'column_1' in df.columns else '').strip()
                                    next_field_name_piece = str(next_row.get('column_2', '') if 'column_2' in df.columns else '').strip()                 
                                    
                                    # check if field name has continuation 
                                    if next_field_entry_indicator!='nan' and next_field_name_piece !='nan':
                                        current_Normal_Field_Fullname = field + next_field_name_piece  # Concatenate field name
                                        metadata[table_name]["Normal Fields"][current_Normal_Field_Fullname] = {
                                            "type": data_type ,
                                            "description": desc ,
                                            "foreign_key": foreign_key if foreign_key != 'nan' else ''
                                        }
  
                            # field name not getting truncated                             
                            else:  
                                current_Normal_Field_Fullname = field
                                metadata[table_name]["Normal Fields"][current_Normal_Field_Fullname] = {
                                            "type": data_type ,
                                            "description": desc  ,
                                            "foreign_key": foreign_key if foreign_key != 'nan' else ''
                                        }
                                if table_name=="B_PG_PICBX4008_TDA_ACCRUAL":
                                    if field=="status":
                                        print(field)
                                        print(file)
                                        print(desc)

                        else :
                            # This is a continuation line for the current normal field
                            if current_Normal_Field_Fullname and desc !="nan" and desc !="Unnamed: 3" and desc:
                                # Append description to the last normal field
                                full_description = metadata[table_name]["Normal Fields"][current_Normal_Field_Fullname]["description"]+f" {desc}"
                                metadata[table_name]["Normal Fields"][current_Normal_Field_Fullname]["description"] = full_description
                             
            
            # At the end of each file processing, update the last metadata in results with all collected fields
            if table_name and results:
                # Get the last metadata from results list
                last_metadata = results[-1]
                

                
            
        except Exception as e:
            print(f"Error processing file {file}: {e}")
    # Write metadata to JSON file
    if output_json_path:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    return metadata

def custom_clean_text(text):
    """
    Clean and normalize text by fixing common typos and standardizing terms.
    
    Args:
        text (str): The text to clean
        
    Returns:
        str: The cleaned text
    """
    if not text or text == 'nan':
        return text
    
    # Replace common typos
    text = text.replace('start_daite', 'start_date')
    text = text.replace('Daite', 'Date')
    text = text.replace('DAITE', 'DATE')
    
    # Add other common replacements as needed
    # text = text.replace('teh', 'the')
    # text = text.replace('recrod', 'record')
    
    return text

if __name__ == "__main__":      
    pdf_path = r'Finacle Core 11_15 Data Dictionary.pdf'
    output_dir = 'markdown_tables'
    table_prefix = 'table'
    
    # print(f"Counting 'Table Name' occurrences in the PDF...")
    #pdf_table_list = count_table_name_occurrences_in_pdf(pdf_path)
    
    # print(f"\nConverting PDF tables to Markdown files...")
    pdf_to_markdown(pdf_path, output_dir)
    
    # print(f"Occurrences of 'Table Name' in the PDF: {len(pdf_table_names)}")
    
    # print(f"\nCounting 'Table Name' occurrences in the Markdown files...")
    markdown_table_list =extract_table_names_in_markdown(output_dir)
    print(f"Occurrences of 'Table Name' in the Markdown files: {len(markdown_table_list)}")

    tables=extract_table_details_from_markdown(output_dir,' tables1.json')
    

    print(f"Extracted {len(tables)} tables from Markdown files.")








