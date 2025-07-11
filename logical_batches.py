# import pandas as pd
# import numpy as np

# # Parameters
# num_rows = 10000
# max_partition_size = 10  # Maximum number of rows in a partition

# # Generate random partition sizes that sum to num_rows
# partition_sizes = []
# total = 0
# while total < num_rows:
#     size = np.random.randint(1, max_partition_size + 1)
#     if total + size > num_rows:
#         size = num_rows - total
#     partition_sizes.append(size)
#     total += size

# # Generate data
# conversations = []
# subqueries = []
# for size in partition_sizes:
#     # First row of partition is 'new', rest are 'continue'
#     conversations.extend([f"Conversation text {i}" for i in range(len(conversations), len(conversations) + size)])
#     subqueries.extend(['new'] + ['continue'] * (size - 1))

# # Create DataFrame
# df = pd.DataFrame({'conversation': conversations, 'subquery': subqueries})
# df.to_csv('conversations.csv', index=False)


# import pandas as pd
# import os
# import numpy as np
# # Function to load CSV and partition it into logical batches

# def load_and_partition_csv(file_path, num_batches):
#     # Load the DataFrame from CSV
#     df = pd.read_csv(file_path)
    
#     # Create a group id for each subquery
#     df['group_id'] = (df['subquery'] == 'new').cumsum()
    
#     # Get all unique group_ids and split them into num_batches
#     unique_groups = df['group_id'].unique()
#     batches = np.array_split(unique_groups, num_batches)
    
#     # Create a list of DataFrames, one for each batch
#     batch_dfs = [df[df['group_id'].isin(batch)].reset_index(drop=True) for batch in batches]
    
#     return batch_dfs

# batch_dfs = load_and_partition_csv('conversations.csv', 10)

# # Directory to save CSV files
# output_dir = 'batches_csv'
# os.makedirs(output_dir, exist_ok=True)

# # Save each batch to a CSV file
# for i, batch_df in enumerate(batch_dfs, 1):
#     filename = os.path.join(output_dir, f'batch_{i}.csv')
#     batch_df.to_csv(filename, index=False)
#     print(f'Saved {filename} with {len(batch_df)} rows')

import pandas as pd
import os
import numpy as np

def load_and_partition_csv_max_rows(file_path, max_rows_per_batch):
    df = pd.read_csv(file_path)
    df['group_id'] = (df['subquery'] == 'new').cumsum()
    groups = [g for _, g in df.groupby('group_id')]
    
    batches = []
    current_batch = []
    current_count = 0

    for group in groups:
        group_size = len(group)
        # If adding this group would exceed max_rows_per_batch, start a new batch
        if current_count + group_size > max_rows_per_batch and current_batch:
            batches.append(pd.concat(current_batch).reset_index(drop=True))
            current_batch = []
            current_count = 0
        current_batch.append(group)
        current_count += group_size

    # Add the last batch if not empty
    if current_batch:
        batches.append(pd.concat(current_batch).reset_index(drop=True))
    return batches

# Example usage:
batch_dfs = load_and_partition_csv_max_rows('conversations.csv', 100)  # 100 rows max per batch

output_dir = 'batches_csv'
os.makedirs(output_dir, exist_ok=True)

for i, batch_df in enumerate(batch_dfs, 1):
    filename = os.path.join(output_dir, f'batch_{i}.csv')
    batch_df.to_csv(filename, index=False)
    print(f'Saved {filename} with {len(batch_df)} rows')