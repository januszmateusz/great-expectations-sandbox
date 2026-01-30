import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Ustaw seed dla powtarzalności
np.random.seed(42)

# Generuj dane - symulacja danych lotniczych LOT
n_records = 1000

# Generuj daty
start_date = datetime(2024, 1, 1)
dates = [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(n_records)]

# Generuj dane z celowymi problemami jakości danych
data = {
    'flight_id': [f'LO{str(i).zfill(4)}' for i in range(1, n_records + 1)],
    'flight_date': dates,
    'departure_airport': np.random.choice(['WAW', 'KRK', 'GDN', 'WRO', 'KTW', None], n_records, p=[0.5, 0.2, 0.15, 0.1, 0.04, 0.01]),  # 1% NULL
    'arrival_airport': np.random.choice(['JFK', 'ORD', 'LHR', 'FRA', 'CDG', 'AMS', None], n_records, p=[0.2, 0.15, 0.25, 0.2, 0.15, 0.04, 0.01]),
    'scheduled_departure': [datetime(2024, 1, 1, np.random.randint(6, 23), np.random.choice([0, 15, 30, 45])) for _ in range(n_records)],
    'actual_departure': [],  # Wypełnimy poniżej
    'delay_minutes': [],  # Wypełnimy poniżej
    'passenger_count': np.random.randint(50, 300, n_records),
    'aircraft_type': np.random.choice(['B737', 'B787', 'E195', 'E175', ''], n_records, p=[0.3, 0.25, 0.2, 0.23, 0.02]),  # 2% puste stringi
    'ticket_revenue': np.random.uniform(50000, 500000, n_records),
    'fuel_cost': np.random.uniform(10000, 100000, n_records),
    'status': np.random.choice(['COMPLETED', 'CANCELLED', 'DELAYED', 'ON_TIME'], n_records, p=[0.7, 0.05, 0.15, 0.1]),
}

# Generuj opóźnienia i rzeczywiste wyloty
for i in range(n_records):
    if data['status'][i] == 'CANCELLED':
        data['delay_minutes'].append(None)
        data['actual_departure'].append(None)
    else:
        # Większość lotów ma małe opóźnienie, niektóre duże
        if np.random.random() < 0.8:
            delay = np.random.randint(-5, 45)  # -5 to wczesny wylot
        else:
            delay = np.random.randint(45, 300)  # Duże opóźnienia
        
        data['delay_minutes'].append(delay)
        data['actual_departure'].append(data['scheduled_departure'][i] + timedelta(minutes=delay))

# Dodaj celowe problemy jakości:
# 1. Duplikaty
duplicate_indices = np.random.choice(range(n_records), 20, replace=False)
data['flight_id'][duplicate_indices[0]] = data['flight_id'][duplicate_indices[1]]

# 2. Negatywna liczba pasażerów (błąd danych)
data['passenger_count'][np.random.choice(range(n_records), 5)] = -10

# 3. Revenue mniejsze niż fuel_cost (nierealistyczne)
problematic_flights = np.random.choice(range(n_records), 30, replace=False)
for idx in problematic_flights:
    data['ticket_revenue'][idx] = data['fuel_cost'][idx] * 0.5

# 4. Passenger_count > 400 (za dużo dla tych samolotów)
data['passenger_count'][np.random.choice(range(n_records), 10)] = np.random.randint(400, 500)

# 5. Outliers w opóźnieniach
extreme_delays = np.random.choice(range(n_records), 5, replace=False)
for idx in extreme_delays:
    if data['delay_minutes'][idx] is not None:
        data['delay_minutes'][idx] = np.random.randint(1000, 2000)

# Utwórz DataFrame
df = pd.DataFrame(data)

# Zapisz do CSV
output_path = 'C:/DATA_ENGINEER/great-expectations-sandbox/gx/uncommitted/working_files/flight_data_sample.csv'
df.to_csv(output_path, index=False)

print(f"✅ Utworzono plik: {output_path}")
print(f"📊 Liczba rekordów: {len(df)}")
print(f"\n🔍 Podsumowanie problemów jakości danych:")
print(f"  - Wartości NULL w departure_airport: {df['departure_airport'].isna().sum()}")
print(f"  - Wartości NULL w arrival_airport: {df['arrival_airport'].isna().sum()}")
print(f"  - Puste stringi w aircraft_type: {(df['aircraft_type'] == '').sum()}")
print(f"  - Negatywne wartości passenger_count: {(df['passenger_count'] < 0).sum()}")
print(f"  - Revenue < Fuel Cost: {(df['ticket_revenue'] < df['fuel_cost']).sum()}")
print(f"  - Passenger count > 400: {(df['passenger_count'] > 400).sum()}")
print(f"  - Extreme delays (>1000 min): {(df['delay_minutes'] > 1000).sum()}")
print(f"\n📋 Pierwsze 5 wierszy:")
print(df.head())