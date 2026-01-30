import great_expectations as gx
import pandas as pd
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_PATH = PROJECT_ROOT / "great-expectations-sandbox" / "gx" / "uncommitted" / "working_files" / "flight_data_sample.csv"

# Load data
if not DATA_PATH.exists():
    raise FileNotFoundError(f"Data file not found: {DATA_PATH}")

df = pd.read_csv(DATA_PATH)
print(f"Loaded data: {len(df)} rows, {len(df.columns)} columns")

# Load GX context
context = gx.get_context()
print(f"Contex type: {type(context).__name__}")
print(f"GX version: {gx.__version__}")

datasource_name = "flight_data_pandas"

try:
    data_source = context.data_sources.add_pandas(datasource_name)
    print(f"Created datasource: {data_source}")
except Exception as e:
    if "already exists" in str(e).lower():
        data_source = context.data_sources.get(datasource_name)
        print(f"Datasource '{datasource_name}' already exists")
    else:
        raise

# Add dataframe asset
asset_name = "flight_data"

try:
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    print(f"Created asset: {data_asset}")
except Exception as e:
    if "already exists" in str(e).lower():
        data_asset = data_source.get_asset(asset_name)
    else:
        raise

# Create batch definition
batch_def_name = "flight_data_batch"

try:
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_def_name)
    print(f"Created batch definition: {batch_def_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        batch_definition = data_asset.get_batch_definition(batch_def_name)
    else:
        raise

# Get batch with data
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

print(f"Created batch with {len(df)} rows")

print("\n" + "="*60)
print("CREATING AND TESTING EXPECTATIONS")
print("="*60)

# Test 1: Passenger count in reasonable range
print("\nTest 1: Passenger count between 1 and 400")
expectation1 = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count",
    min_value=1,
    max_value=400,
    mostly=0.95,
    severity="warning"
)

validation_result1 = batch.validate(expectation1)
print(f"   Result: {'PASS' if validation_result1.success else 'FAIL'}")
print(f"   Observations: {validation_result1.result}")

# Test 2: Departure airport cannot be NULL
print("\nTest 2: Departure airport cannot be NULL")
expectation2 = gx.expectations.ExpectColumnValuesToNotBeNull(
  column="departure_airport"
)

validation_result2 = batch.validate(expectation2)
print(f"   Result: {'PASS' if validation_result2.success else 'FAIL'}")
print(f"   Observations: {validation_result2.result}")

# Test 3: Departure airport from allowed list
print("\nTest 3: Departure airport from LOT airports list")
expectation3 = gx.expectations.ExpectColumnValuesToBeInSet(
  column = "departure_airport",
  value_set=["WAW", "KRK", "GDN", "WRO", "KTW"],
  mostly=0.95
)

validation_result3 = batch.validate(expectation3)
print(f"   Result: {'PASS' if validation_result3.success else 'FAIL'}")
print(f"   Observations: {validation_result3.result}")

# Test 4: Revenue greater than fuel cost
print("\nTest 4: Revenue > Fuel Cost (business logic)")
expectation4 = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="ticket_revenue",
    column_B="fuel_cost",
    mostly=0.90
)

validation_result4 = batch.validate(expectation4)
print(f"   Result: {'PASS' if validation_result4.success else 'FAIL'}")
print(f"   Observations: {validation_result4.result}")

print("\n" + "="*60)
print("VALIDATION COMPLETED")
print("="*60)