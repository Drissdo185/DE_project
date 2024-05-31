from faker import Faker
import random
import pandas as pd
import pytz
import os

# Initialize Faker
fake = Faker()
# meta data https://www.kaggle.com/datasets/prakharrathi25/banking-dataset-marketing-targets/data
# Generate fake banking data
def generate_banking_data(num_rows):
    data = []
    for _ in range(num_rows):
        age = random.randint(18, 80)
        job = random.choice(["admin.", "unknown", "unemployed", "management", "housemaid", 
                             "entrepreneur", "student", "blue-collar", "self-employed", 
                             "retired", "technician", "services"])
        marital = random.choice(['married', 'divorced', 'single'])
        education = random.choice(['unknown', 'secondary', 'primary', 'tertiary'])
        default = random.choice(['yes', 'no'])
        balance = random.randint(-10000, 10000)
        housing = random.choice(['yes', 'no'])
        loan = random.choice(['yes', 'no'])
        contact = random.choice(['unknown', 'telephone', 'cellular'])
        day = random.randint(1, 31)
        month = random.choice(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])
        duration = random.randint(1, 3600)
        campaign = random.randint(1, 10)
        pdays = random.randint(-1, 30)
        previous = random.randint(0, 10)
        poutcome = random.choice(['unknown', 'other', 'failure', 'success'])
        
        data.append([age, job, marital, education, default, balance, housing, loan, 
                     contact, day, month, duration, campaign, pdays, previous, poutcome])
    return data


num_rows_per_file = 1000
num_files = 2
output_directory = 'data' 

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

output_prefix = 'banking_data'




for i in range(num_files):
    data = generate_banking_data(num_rows_per_file)
    df = pd.DataFrame(data, columns=['age', 'job', 'marital', 'education', 'default', 'balance', 
                                     'housing', 'loan', 'contact', 'day', 'month', 'duration', 
                                     'campaign', 'pdays', 'previous', 'poutcome'])
    file_path = os.path.join(output_directory, f'{output_prefix}_{i}.parquet')
    df.to_parquet(file_path)
    print(f"Saved {file_path}")

print("Fake banking data generation complete.")
