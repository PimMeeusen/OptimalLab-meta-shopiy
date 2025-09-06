import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# -----------------------------
# Configuratie
# -----------------------------
token_file = r"C:\Program Files (x86)\Python\Projects\Weekcijfers Shopify\Credentials_shopify.txt"
excel_file = r"C:\Program Files (x86)\Python\Projects\Weekcijfers Shopify\shopify_orders.xlsx"
shop_name = "a7dfef"  # Zonder .myshopify.com
vat_nl = 0.09
vat_be = 0.06

# -----------------------------
# Lees token
# -----------------------------
with open(token_file, 'r') as f:
    access_token = f.read().strip()

headers = {
    "X-Shopify-Access-Token": access_token,
    "Content-Type": "application/json"
}

# -----------------------------
# Functie om orders op te halen (paginated)
# -----------------------------
def get_orders(start_date, end_date):
    url = f"https://{shop_name}.myshopify.com/admin/api/2025-01/orders.json"
    params = {
        "status": "any",
        "financial_status": "any",
        "created_at_min": start_date + "T00:00:00",
        "created_at_max": end_date + "T23:59:59",
        "limit": 250
    }

    all_orders = []
    while url:
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 429:
            print("⚠️ Rate limit bereikt, wacht 1 sec...")
            time.sleep(1)
            continue
        elif r.status_code != 200:
            print(f"❌ Fout: {r.status_code} {r.text}")
            return None
        data = r.json()
        orders = data.get("orders", [])

        # Filter: geen geannuleerde of test orders
        filtered_orders = []
        for o in orders:
            if o.get("cancelled_at"):
                continue
            if o.get("test") == True:
                continue
            if "Test Order" in o.get("tags", ""):
                continue
            filtered_orders.append(o)

        all_orders.extend(filtered_orders)

        # Check next page
        next_url = None
        if 'Link' in r.headers:
            links = r.headers['Link'].split(",")
            for link in links:
                if 'rel="next"' in link:
                    next_url = link[link.find("<")+1:link.find(">")]
        url = next_url
        params.clear()
        if url:
            time.sleep(0.5)
    return all_orders

# -----------------------------
# Lees bestaande orders
# -----------------------------
if os.path.exists(excel_file):
    df_existing = pd.read_excel(excel_file, sheet_name=None)
    df_orders_existing = df_existing.get("Orders", pd.DataFrame())
    existing_order_ids = df_orders_existing['order_id'].astype(str).tolist() if not df_orders_existing.empty else []
else:
    df_orders_existing = pd.DataFrame()
    existing_order_ids = []

# -----------------------------
# Full sync: 2 jaar
# -----------------------------
start_date_full = (datetime.today() - timedelta(days=365*2)).strftime('%Y-%m-%d')
end_date_full = datetime.today().strftime('%Y-%m-%d')

print(f"📦 Full sync van {start_date_full} tot {end_date_full}")

orders_full = get_orders(start_date_full, end_date_full)
if orders_full is None:
    print("❌ Geen orders opgehaald.")
    exit()

print(f"📥 Totaal opgehaald (zonder geannuleerd/test): {len(orders_full)} orders")

# -----------------------------
# Verwerk orders en bereken BTW + land + refund status
# -----------------------------
def process_orders(orders):
    processed_orders = []
    for o in orders:
        order_id = str(o['id'])
        if order_id in existing_order_ids:
            continue

        created_at = o['created_at'][:10]
        country = o.get('shipping_address', {}).get('country_code', 'NL')
        vat_rate = vat_nl if country == 'NL' else vat_be

        total_price_incl_shipping = float(o.get('total_price', 0))
        total_excl_vat = round(total_price_incl_shipping / (1 + vat_rate), 2)
        total_vat_amount = round(total_price_incl_shipping - total_excl_vat, 2)

        # ✅ Refund status
        financial_status = o.get('financial_status', 'paid')
        if financial_status == 'refunded':
            refund_status = 'refunded'
        elif financial_status == 'partially_refunded':
            refund_status = 'partially_refunded'
        else:
            refund_status = 'paid'

        for line in o['line_items']:
            processed_orders.append({
                "order_id": order_id,
                "date": created_at,
                "country": country,
                "product_name": line['title'],
                "quantity": line['quantity'],
                "total_incl_shipping": total_price_incl_shipping,
                "total_excl_vat": total_excl_vat,
                "total_vat_amount": total_vat_amount,
                "refund_status": refund_status
            })
    return processed_orders

processed_orders_full = process_orders(orders_full)
print(f"📥 Orders verwerkt: {len(processed_orders_full)}")

# -----------------------------
# Verwerk refunds (alle soorten) met datum pending/processed
# -----------------------------
def process_refunds(orders):
    refunds_list = []
    for o in orders:
        order_id = str(o['id'])
        for refund in o.get('refunds', []):
            # Datum van refund (processed_at of created_at fallback)
            refund_date = refund.get('processed_at', refund.get('created_at', o['created_at']))[:10]
            refund_total = 0.0

            # 1️⃣ Line item refunds
            for rli in refund.get('refund_line_items', []):
                subtotal = float(rli.get('subtotal', 0))
                total_tax = float(rli.get('total_tax', 0))
                refund_total += subtotal + total_tax

            # 2️⃣ Shipping refunds
            shipping_refund = refund.get('shipping', {})
            if shipping_refund:
                refund_total += float(shipping_refund.get('amount', 0))
                refund_total += float(shipping_refund.get('tax_amount', 0))

            # 3️⃣ Extra transactions fallback
            for tx in refund.get('transactions', []):
                refund_total += float(tx.get('amount', 0))

            # 4️⃣ Alleen toevoegen als niet 0
            if refund_total != 0:
                refunds_list.append({
                    "order_id": order_id,
                    "refund_date": refund_date,
                    "refund_amount": -refund_total  # negatief
                })
    return refunds_list

processed_refunds = process_refunds(orders_full)
print(f"📥 Refunds verwerkt: {len(processed_refunds)}")

# -----------------------------
# Schrijf alles naar Excel (Orders + Refunds)
# -----------------------------
df_orders_new = pd.DataFrame(processed_orders_full)
df_refunds_new = pd.DataFrame(processed_refunds)

# Voeg bestaande data toe
if not df_orders_existing.empty:
    df_orders_final = pd.concat([df_orders_existing, df_orders_new], ignore_index=True)
else:
    df_orders_final = df_orders_new

df_refunds_existing = df_existing.get("Refunds", pd.DataFrame()) if os.path.exists(excel_file) else pd.DataFrame()
if not df_refunds_existing.empty:
    df_refunds_final = pd.concat([df_refunds_existing, df_refunds_new], ignore_index=True)
else:
    df_refunds_final = df_refunds_new

# Opslaan
with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
    df_orders_final.to_excel(writer, index=False, sheet_name="Orders")
    df_refunds_final.to_excel(writer, index=False, sheet_name="Refunds")

print(f"✅ Excel bijgewerkt. Orders: {len(df_orders_final)}, Refunds: {len(df_refunds_final)}")
