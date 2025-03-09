import yaml
import csv
import random
import string
from datetime import datetime, timedelta
from faker import Faker

def load_yaml_definition(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def generate_test_data(definition, num_records=10):
    fake = Faker()
    table_def = definition['table']
    columns = table_def['columns']
    
    # Generate test data
    test_data = []
    used_ids = set()  # For unique values
    
    for _ in range(num_records):
        record = {}
        for col in columns:
            value = generate_column_value(col, fake, used_ids)
            record[col['name']] = value
        test_data.append(record)
    
    return test_data

def generate_column_value(column, fake, used_ids):
    col_type = column['type']
    rules = column.get('rules', [])
    
    if col_type == 'integer':
        min_val = next((r['min'] for r in rules if 'min' in r), 0)
        max_val = next((r['max'] for r in rules if 'max' in r), 9999)
        value = random.randint(min_val, max_val)
        
        # Handle unique constraint
        if 'unique' in rules:
            while value in used_ids:
                value = random.randint(min_val, max_val)
            used_ids.add(value)
            
    elif col_type == 'string':
        if 'email' in column['name'].lower():
            value = fake.email()
        else:
            min_length = next((r['min_length'] for r in rules if 'min_length' in r), 5)
            max_length = next((r['max_length'] for r in rules if 'max_length' in r), 100)
            value = fake.name()[:max_length]
            
    elif col_type == 'date':
        min_date = datetime.strptime(next((r['min'] for r in rules if 'min' in r), '2020-01-01'), '%Y-%m-%d')
        max_date = datetime.strptime(next((r['max'] for r in rules if 'max' in r), '2024-12-31'), '%Y-%m-%d')
        days_between = (max_date - min_date).days
        random_days = random.randint(0, days_between)
        value = (min_date + timedelta(days=random_days)).strftime('%Y-%m-%d')
        
    return value

def write_csv(data, output_file):
    if not data:
        return
    
    fieldnames = data[0].keys()
    with open(output_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    # File paths
    yaml_file = 'table_definition.yaml'
    output_csv = 'test_data.csv'
    
    # Load definition and generate data
    definition = load_yaml_definition(yaml_file)
    test_data = generate_test_data(definition)
    
    # Write to CSV
    write_csv(test_data, output_csv)
    print(f"Test data generated successfully in {output_csv}")

if __name__ == "__main__":
    main()