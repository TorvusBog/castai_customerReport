import json
import base64
import os
import glob

def obfuscate_string(value):
    # Encode the value using base64 for obfuscation
    return base64.b64encode(value.encode()).decode()

def obfuscate_json(data):
    if isinstance(data, dict):
        return {key: obfuscate_json(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [obfuscate_json(element) for element in data]
    elif isinstance(data, str):
        return obfuscate_string(data)
    else:
        return data

def main():
    # Get the current directory
    current_directory = os.getcwd()

    # Find all JSON files in the current directory
    json_files = glob.glob(os.path.join(current_directory, '*.json'))

    # Process each JSON file
    for input_file in json_files:
        output_file = f"obfuscated_{os.path.basename(input_file)}"

        try:
            # Read the original JSON data
            with open(input_file, 'r') as f:
                data = json.load(f)

            # Obfuscate the JSON data
            obfuscated_data = obfuscate_json(data)

            # Write the obfuscated data to a new JSON file
            with open(output_file, 'w') as f:
                json.dump(obfuscated_data, f, indent=4)

            print(f"Obfuscated file created: {output_file}")
        except json.JSONDecodeError as e:
            print(f"Error reading {input_file}: {e}")

if __name__ == "__main__":
    main()

