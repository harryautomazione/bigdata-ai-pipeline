import pandas as pd
import os
import shutil

def main():
    raw_file_path = "E:/bigdata-ai-pipeline/data/transactions.json"
    processed_path = "E:/bigdata-ai-pipeline/data/processed/user_spend"

    # Clean output folder
    if os.path.exists(processed_path):
        shutil.rmtree(processed_path)
    os.makedirs(processed_path, exist_ok=True)

    # Read JSON
    df = pd.read_json(raw_file_path, lines=True)

    # Basic cleaning
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"] > 0]

    # Aggregation
    user_spend = (
        df.groupby("user_id")
          .agg(
              total_spend=("amount", "sum"),
              transaction_count=("amount", "count")
          )
          .reset_index()
    )

    # Write output
    output_file = os.path.join(processed_path, "user_spend.csv")
    user_spend.to_csv(output_file, index=False)

    print("Output generated using pandas")
    print(user_spend.head())

if __name__ == "__main__":
    main()
