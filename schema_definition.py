"""
- Takes in input from the user and creates a schema definition for the same in initial_schema.json
- The schema defination is in the form of a dictionary with the following keys
    - 

- Input: From the user
- Output: Initial schema definition in initial_schema.json
"""

import json

defined_field_names = [
    "string", "integer", "float", "boolean", "object",
    "array_string", "array_int", "array_float" 
]
defined_formats = [
    "email", "date-time", "date", "time", "uri", "uuid", "ipv4", "ipv6", "hostname"
]

def take_user_input():
    schema_properties = {}
    
    while True:
        more_to_add = input("Do you have any more fields to define? (Y/N): ")
        if (more_to_add == "Y"):
            field_name = input("Please enter the name of your field: ")
            if not field_name:
                print("Please enter a non empty field name, Try again:")
                continue
                
            
            while True:
                field_type = input("Please enter the datatype of this field: ")
                if (field_type.lower() not in defined_field_names):
                    print("Please enter a valid datatype from the following list:")
                    print(defined_field_names)
                else:
                    field_type = field_type.lower()
                    break;
            field_metadata = {"type": field_type}
            if (field_type == "string"):
                condition = input("Is this string a specific format? (email, date-time, uri, uud): (Y/N)")
                if (condition == "Y"):
                    while True:
                        format_type = input(f"Please enter a specific format: (press 'N' to exit the loop)")
                        if (format_type == "N"):
                            field_metadata["specific_format"] = "null"
                            break
                        if (format_type in defined_formats):
                            field_metadata["specific_format"] = format_type
                            break
                        else:
                            print("Please enter a valid format from the following:")
                            print(defined_formats)
            
            
            condition_unique = input(f"Is {field_name} unique? (Y/N):")
            if condition_unique == "Y":
                condition_unique = True
            else:
                condition_unique = False
            field_metadata["unique"] = (condition_unique)
                
            condition_not_null = input(f"Is {field_name} NOT NULL? (Y/N):")
            if condition_not_null == "Y":
                condition_not_null = True
            else:
                condition_not_null = False
            field_metadata["not_null"] = (condition_not_null)
            
            schema_properties[field_name] = field_metadata
            
        elif (more_to_add == "N"):
            print("Schema defination is done!!")
            break
            
        else:
            print("Please enter a valid input")
            
    final_json = json.dumps(schema_properties, indent=4)
    return final_json
        
if __name__ == "__main__":
    resulting_json = take_user_input()
    
    print("\nHere is your generated JSON:")
    print(resulting_json)
    