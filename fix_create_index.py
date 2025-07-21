#!/usr/bin/env python3
import re
import sys

def fix_create_index(content):
    # Pattern to match create_index calls with 5 parameters
    pattern = r'(\.create_index\(\s*(?:[^,]+,\s*){4}[^,]+,)\s*\)'
    
    # Function to check if this already has 6 parameters
    def check_and_fix(match):
        full_match = match.group(0)
        # Count commas to see if we already have 6 parameters
        comma_count = full_match.count(',')
        if comma_count == 5:
            # Already has 6 parameters
            return full_match
        else:
            # Add true as the 6th parameter
            return match.group(1) + '\n                true,\n            )'
    
    # Apply the fix
    fixed_content = re.sub(pattern, check_and_fix, content, flags=re.MULTILINE | re.DOTALL)
    return fixed_content

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        with open(file_path, 'r') as f:
            content = f.read()
        
        fixed_content = fix_create_index(content)
        
        with open(file_path, 'w') as f:
            f.write(fixed_content)
        
        print(f"Fixed {file_path}")
    else:
        print("Usage: python fix_create_index.py <file_path>")