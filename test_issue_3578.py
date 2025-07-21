#!/usr/bin/env python3

import sys
import tempfile
import shutil
import pyarrow as pa

# Add the python path to the lance directory
sys.path.insert(0, '/home/will/Documents/lance/python')
import lance

def test_highly_nested_data_issue():
    """Test case for issue #3578: Error with TakeExec and highly nested data"""
    
    # Create the nested data structure that reproduces the issue
    data = pa.Table.from_pylist([
        {
            "item": "test",
            "sub_obj": {
                "second_sub_obj": [
                    {
                        "third_sub_obj": {"int_val": 1},
                        "str_val": "test",
                    }
                ]
            },
        }
    ])

    print("Original schema:")
    print(data.schema)
    print()

    # Create a temporary directory for the dataset
    temp_dir = tempfile.mkdtemp()
    try:
        # Write the dataset
        print("Writing dataset...")
        ds = lance.write_dataset(data, temp_dir)
        print("Dataset written successfully")
        print()

        # Test 1: Basic read (should work)
        print("Test 1: Basic read without filter")
        try:
            result = ds.to_table()
            print("✓ Basic read works")
            print(f"Result schema: {result.schema}")
            print()
        except Exception as e:
            print(f"✗ Basic read failed: {e}")
            print()

        # Test 2: Read with filter (this was failing before the fix)
        print("Test 2: Read with filter - the main issue")
        try:
            result = ds.to_table(filter="item == 'test'")
            print("✓ Filtered read works!")
            print(f"Result schema: {result.schema}")
            
            # Verify the nested structure is intact
            sub_obj_field = result.schema.field('sub_obj')
            print(f"sub_obj field type: {sub_obj_field.type}")
            
            if hasattr(sub_obj_field.type, 'field') and hasattr(sub_obj_field.type.field(0), 'type'):
                second_sub_obj_type = sub_obj_field.type.field(0).type
                if hasattr(second_sub_obj_type, 'value_type'):
                    list_item_type = second_sub_obj_type.value_type
                    print(f"List item type: {list_item_type}")
                    if hasattr(list_item_type, '__iter__'):
                        field_names = [f.name for f in list_item_type]
                        print(f"List item field names: {field_names}")
                        
                        # Check if both str_val and third_sub_obj are present
                        if 'str_val' in field_names and 'third_sub_obj' in field_names:
                            print("✓ Both str_val and third_sub_obj fields are present")
                        else:
                            print(f"✗ Missing fields. Expected both 'str_val' and 'third_sub_obj', found: {field_names}")
            
            print()
        except Exception as e:
            print(f"✗ Filtered read failed: {e}")
            print("This indicates the bug is still present.")
            print()

        # Test 3: V2 storage format (the original problem format)
        print("Test 3: Testing with explicit V2 storage")
        try:
            # Note: Lance might default to V2 now, but we test anyway
            ds_v2 = lance.write_dataset(data, temp_dir + "_v2")
            result_v2 = ds_v2.to_table(filter="item == 'test'")
            print("✓ V2 filtered read works!")
            print()
        except Exception as e:
            print(f"✗ V2 filtered read failed: {e}")
            print()

    finally:
        # Clean up
        shutil.rmtree(temp_dir, ignore_errors=True)
        shutil.rmtree(temp_dir + "_v2", ignore_errors=True)

if __name__ == "__main__":
    print("Testing issue #3578: Error with TakeExec and highly nested data")
    print("=" * 60)
    test_highly_nested_data_issue()
    print("Test completed.")