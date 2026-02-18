import boto3
import json
import requests
from datetime import datetime

def fetch_from_api():
    products_res = requests.get("https://dummyjson.com/products?limit=100").json()
    users_res    = requests.get("https://dummyjson.com/users?limit=100").json()
    carts_res    = requests.get("https://dummyjson.com/carts?limit=100").json()

    print("Products keys:", list(products_res.keys()))
    print("Users keys:",    list(users_res.keys()))
    print("Carts keys:",    list(carts_res.keys()))

    products = products_res.get("products", [])
    users    = users_res.get("users", [])
    carts    = carts_res.get("carts", [])

    user_map    = {u["id"]: u for u in users}
    product_map = {p["id"]: p for p in products}

    orders = []
    for cart in carts:
        user = user_map.get(cart["userId"], {})
        for item in cart["products"]:
            product = product_map.get(item["id"], {})
            orders.append({
                "order_id":        f"ORD-{cart['id']}-{item['id']}",
                "order_timestamp": datetime.now().isoformat(),
                "customer": {
                    "customer_id": cart["userId"],
                    "name":        f"{user.get('firstName','')} {user.get('lastName','')}",
                    "country":     user.get("address", {}).get("country", "Unknown"),
                    "email":       user.get("email", "")
                },
                "product": {
                    "product_id":  item["id"],
                    "name":        item.get("title", product.get("title", "")),
                    "category":    product.get("category", ""),
                    "unit_price":  item.get("price", 0),
                    "cost_price":  round(item.get("price", 0) * 0.6, 2)
                },
                "quantity":        item["quantity"],
                "status":          "completed"
            })
    return orders

def upload_to_s3(orders):
    s3 = boto3.client('s3', region_name='ap-southeast-1')
    today = datetime.now()
    key = f"orders/year={today.year}/month={today.month:02d}/day={today.day:02d}/orders.json"

    # ✅ FIXED: newline-delimited JSON (JSONL format)
    body = "\n".join(json.dumps(order) for order in orders)

    s3.put_object(
        Bucket="ecom-raw-zone-kishore",
        Key=key,
        Body=body,
        ContentType="application/json"
    )
    print(f"✅ Uploaded {len(orders)} orders → s3://ecom-raw-zone-kishore/{key}")

if __name__ == "__main__":
    orders = fetch_from_api()
    print(f"Total orders fetched: {len(orders)}")
    upload_to_s3(orders)
