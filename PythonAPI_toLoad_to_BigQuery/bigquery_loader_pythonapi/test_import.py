# test_import.py

from google.api_core.exceptions import GoogleAPIError

print("Import Successfully")

# test_yaml_import.py

import yaml

def main():
    sample_yaml = """
    name: John Doe
    age: 30
    email: john.doe@example.com
    """
    data = yaml.safe_load(sample_yaml)
    print(data)

if __name__ == "__main__":
    main()
