import random
import string
import csv

def generate_random_key(existing_keys):
    key_length = 10  # Set the desired key length
    while True:
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=key_length))
        if key not in existing_keys:
            return key

def generate_dataset_and_save_to_csv(size, filename):
    existing_keys = set()
    with open(filename, mode='w', newline='') as file:
        fieldnames = ['key', 'value']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(size):
            key = generate_random_key(existing_keys)
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=20))  # Random value length of 20 characters
            item = {'key': key, 'value': value}
            writer.writerow(item)
            existing_keys.add(key)


generate_dataset_and_save_to_csv(10, 'data_10.csv')
generate_dataset_and_save_to_csv(100, 'data_100.csv')
generate_dataset_and_save_to_csv(1000, 'data_1000.csv')
generate_dataset_and_save_to_csv(10000, 'data_10000.csv')

def read_dataset_from_csv(filename):
    dataset = []
    with open(filename, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dataset.append(row)
    return dataset

n_clients = 3  # Number of clients
client_datasets = []
for i in range(n_clients):
    client_datasets.append(read_dataset_from_csv('data_10.csv'))
print(client_datasets)