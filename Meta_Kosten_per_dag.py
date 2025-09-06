import requests
import pandas as pd
from datetime import datetime, timedelta

# --- Config ---
token_file = r'C:\Program Files (x86)\Python\Projects\Weekcijfers Shopify\credentials_Meta.txt'
excel_file = r'C:\Program Files (x86)\Python\Projects\Weekcijfers Shopify\kosten_per_dag.xlsx'
ad_account_id = 'act_1457730908162735'

# Hoeveel dagen terug controleren
DAGEN_TERUG = 7  # bijvoorbeeld laatste 7 dagen

# --- Datum instellen ---
today = datetime.today()
start_date = (today - timedelta(days=DAGEN_TERUG)).strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d')

# --- Lees token ---
with open(token_file, 'r') as f:
    access_token = f.read().strip()

# --- Functie voor API ---
def get_insights(params):
    all_data = []
    url = f'https://graph.facebook.com/v14.0/{ad_account_id}/insights'
    while url:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print(f"Fout bij ophalen API: {r.text}")
            break
        result = r.json()
        all_data.extend(result.get('data', []))
        url = result.get('paging', {}).get('next')
        params = None
    return all_data

# --- API parameters ---
params = {
    'access_token': access_token,
    'fields': 'campaign_name,spend,impressions,clicks,cpc,cpm',
    'level': 'campaign',
    'time_range': f'{{"since":"{start_date}","until":"{end_date}"}}',
    'time_increment': 1,
    'breakdowns': 'country'
}

# --- Ophalen data ---
data = get_insights(params)

# --- Omzetten naar dataframe ---
df_list = []
for item in data:
    date = item['date_start']
    country = item.get('country', 'unknown')
    campaign = item['campaign_name']
    spend = round(float(item.get('spend', 0)), 2)
    cpc = float(item.get('cpc', 0)) if item.get('cpc') else None
    cpm = float(item.get('cpm', 0)) if item.get('cpm') else None
    df_list.append({
        'date': date,
        'country': country,
        'campaign': campaign,
        'spend': spend,
        'cpc': cpc,
        'cpm': cpm
    })

df_new = pd.DataFrame(df_list)

# --- Unieke landen in nieuwe data ---
unique_countries = df_new['country'].unique().tolist()

# --- Groepeer API-data per dag en land ---
df_grouped = df_new.groupby(['date', 'country'])['spend'].sum().reset_index()

# --- Lees bestaande Excel (als die er is) ---
try:
    df_existing = pd.read_excel(excel_file)
except FileNotFoundError:
    df_existing = pd.DataFrame(columns=['date', 'country', 'spend', 'cpc', 'cpm'])

# --- Houd oude data intact, update alleen laatste N dagen ---
df_older = df_existing[~df_existing['date'].isin(pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'))]
df_combined = pd.concat([df_older, df_grouped], ignore_index=True)

# --- Controle op ontbrekende dagen NL/BE ---
for country in ['NL', 'BE']:
    mask = (df_combined['country'] == country) & (df_combined['spend'].isna())
    if mask.any():
        print(f"⚠️ Missende dagen voor {country}: {df_combined.loc[mask, 'date'].tolist()}")
        df_combined.loc[mask, 'spend'] = 0  # vul met 0 als nog geen data aanwezig

# --- Rond af ---
df_combined['spend'] = df_combined['spend'].fillna(0).round(2)

# --- Opslaan Excel ---
df_combined.to_excel(excel_file, index=False)
print(f"✅ Excel bijgewerkt: {excel_file}")
