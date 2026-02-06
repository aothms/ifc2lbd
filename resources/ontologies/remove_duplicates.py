# Look at downloaded folder and remove duplicate when they match with checksum
import os
import hashlib
from collections import defaultdict
downloaded_folder = os.path.join(os.path.dirname(__file__), 'downloaded')
print(f"Checking for duplicates in folder: {downloaded_folder}")
def calculate_checksum(file_path):
    """Calculate SHA512 checksum of a file."""
    sha256 = hashlib.sha512()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
def remove_duplicates(folder_path):
    """Remove duplicate files in the given folder based on checksum."""
    checksum_map = defaultdict(list)
    
    # Calculate checksums and group files
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            checksum = calculate_checksum(file_path)
            checksum_map[checksum].append(file_path)
            #print(f"File: {file_path}, Checksum: {checksum}")
    
    for file_list in checksum_map.values():
        if len(file_list) > 1:
            # Print just names (strip the folder path)
            file_names = [os.path.basename(f) for f in file_list]
            print(f"{file_names}")
    
    
    # Identify and remove duplicates
    duplicates_removed = 0
    for file_list in checksum_map.values():
        if len(file_list) > 1:
            # Keep the first file, remove the rest
            for duplicate_file in file_list[1:]:
                os.remove(duplicate_file)
                duplicates_removed += 1
                print(f"Removed duplicate: {duplicate_file}")
    
    print(f"Total duplicates removed: {duplicates_removed}")
    

# Run the duplicate removal on the downloaded folder
if __name__ == "__main__":
    remove_duplicates(downloaded_folder)