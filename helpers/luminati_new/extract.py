import zipfile
import os

def extract_all_zips_in_folder(folder_path, extract_to=None):
    """
    Extracts all .zip files in the specified folder.

    Args:
        folder_path (str): Path to the folder containing .zip files.
        extract_to (str, optional): Directory where the zip contents will be extracted.
                                    If None, files will be extracted to the same folder as the .zip file.

    Returns:
        None
    """
    if not os.path.isdir(folder_path):
        print(f"Error: The folder '{folder_path}' does not exist.")
        return

    zip_files = [f for f in os.listdir(folder_path) if f.endswith('.zip')]

    if not zip_files:
        print("No .zip files found in the folder.")
        return

    for zip_file in zip_files:
        zip_path = os.path.join(folder_path, zip_file)
        target_path = extract_to or folder_path
        extract_path = os.path.join(target_path, os.path.splitext(zip_file)[0])

        os.makedirs(extract_path, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
                print(f"Extracted '{zip_file}' to '{extract_path}'")
        except zipfile.BadZipFile:
            print(f"Warning: '{zip_file}' is not a valid zip file.")

# Example usage:
extract_all_zips_in_folder('./')
