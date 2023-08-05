import random
import string

def generate_random_key(existing_keys):
    key_length = 10  # Set the desired key length
    while True:
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=key_length))
        if key not in existing_keys:
            return key

def generate_dataset(size):
    dataset = []
    existing_keys = set()
    for _ in range(size):
        key = generate_random_key(existing_keys)
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=20))  # Random value length of 20 characters
        item = {'key': key, 'value': value}
        dataset.append(item)
        existing_keys.add(key)
    return dataset

# Generating an array of dictionaries with 'key' and 'value' structure
# dataset_size = 3
# client_datasets = []
# for i in range(3):
#     client_datasets.append(generate_dataset(dataset_size))
# print(client_datasets)




# Generating a dataset with 10,000 unique keys and random values
# dataset_size = 10000
# my_dataset = generate_dataset(dataset_size)

# # Saving the dataset to a CSV file
# csv_file = 'dataset.csv'
# with open(csv_file, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerow(['Key', 'Value'])
#     for key, value in my_dataset.items():
#         writer.writerow([key, value])

# print(f"Dataset with {dataset_size} keys saved to {csv_file}.")
