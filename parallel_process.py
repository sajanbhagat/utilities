import concurrent.futures
import pandas as pd
import os

# Example function to process a batch and save result
# Replace this with your actual processing logic
# Each result is saved as a CSV file in 'results_csv' directory
def process_and_save(csv_path):
    df = pd.read_csv(csv_path)
    # --- Your processing code here ---
    # For demonstration, just count rows
    result = pd.DataFrame({'filename': [os.path.basename(csv_path)], 'row_count': [len(df)]})
    # Save result
    result_dir = 'results_csv'
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, os.path.basename(csv_path).replace('.csv', '_result.csv'))
    result.to_csv(result_path, index=False)
    print(f"Saved result for {csv_path} to {result_path}")
    return result_path

# List all batch CSV files
batch_dir = 'batches_csv'
batch_files = [os.path.join(batch_dir, f) for f in os.listdir(batch_dir) if f.endswith('.csv')]

# Run in parallel and save results as they arrive
with concurrent.futures.ThreadPoolExecutor () as executor:
    futures = {executor.submit(process_and_save, f): f for f in batch_files}
    for future in concurrent.futures.as_completed(futures):
        try:
            result_path = future.result()
        except Exception as exc:
            print(f'Error processing {futures[future]}: {exc}')