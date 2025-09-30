# producer_kinesis.py
# Sends sample orders to Kinesis Data Stream (simulate streaming).
import csv, time, json, boto3, os

STREAM_NAME = os.getenv("KINESIS_STREAM", "orders-stream")
CLIENT = boto3.client("kinesis")

def send_record(record):
    CLIENT.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(record),
        PartitionKey=str(record["order_id"])
    )

if __name__ == "__main__":
    with open("sample_orders.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # cast types
            row["quantity"] = int(row["quantity"])
            row["price"] = float(row["price"])
            row["total_price"] = row["quantity"] * row["price"]
            send_record(row)
            print("sent", row["order_id"])
            time.sleep(0.2)
