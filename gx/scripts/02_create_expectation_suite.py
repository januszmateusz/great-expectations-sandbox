import great_expectations as gx
from great_expectations.expectations.row_conditions import Column
import pandas as pd
from pathlib import Path

#Setup
context = gx.get_context()
datasource_name = "flight_data_pandas"
asset_name = "flight_data"
batch_def_name = "flight_data_batch"
suite_name = "flight_data_quality_suite"
print(f"Contex type: {type(context).__name__}")
print(f"GX version: {gx.__version__}")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_PATH = PROJECT_ROOT / "great-expectations-sandbox" / "gx" / "uncommitted" / "working_files" / "flight_data_sample.csv"

# Load data
if not DATA_PATH.exists():
    raise FileNotFoundError(f"Data file not found: {DATA_PATH}")

df = pd.read_csv(DATA_PATH)
# Convert departure_time and actual_departure from object to datetime
df["scheduled_departure_dt"] = pd.to_datetime(
    df["scheduled_departure"],
    errors="coerce"
)
df["actual_departure_dt"] = pd.to_datetime(
    df["actual_departure"],
    errors="coerce"
)

print("CREATING COMPREHENSIVE EXPECTATION SUITE")
print("=" * 60)

# Add datasource
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
try:
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    print(f"Created asset: {data_asset}")
except Exception as e:
    if "already exists" in str(e).lower():
        data_asset = data_source.get_asset(asset_name)
    else:
        raise

# Add dataframe batch definition
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

# Create expectation suite
try:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    print(f"Created suite: {suite_name}\n")
except Exception  as e:
    if "already exists" in str(e).lower():
        suite = context.suites.get(suite_name)
        print(f"ℹ️  Suite '{suite_name}' already exists - loading it\n")
    else:
        raise

results = []

# ============================================
# 1: SCHEMA VALIDATION
# ============================================
# Test 1: Check if all columns exists
print("1. Checking required columns exist...")
expected_columns = [
    "flight_id",
    "flight_date",
    "departure_airport",
    "arrival_airport",
    "scheduled_departure",
    "actual_departure",
    "delay_minutes",
    "passenger_count",
    "aircraft_type",
    "ticket_revenue",
    "fuel_cost",
    "status",
    "scheduled_departure_dt",
    "actual_departure_dt"
]

expectation_columns = gx.expectations.ExpectTableColumnsToMatchOrderedList(column_list=expected_columns)
suite.add_expectation(expectation_columns)
result = batch.validate(expectation_columns)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

# Test 2: Check number of columns
print("2. Checking column count...")
expectation_column_count = gx.expectations.ExpectTableColumnCountToEqual(value=len(expected_columns))
suite.add_expectation(expectation_column_count)
result = batch.validate(expectation_column_count)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

# ============================================
# CATEGORY 2: COMPLETENESS (NULL CHECKS)
# ============================================
print("\nCATEGORY 2: Data Completeness")
print("-" * 60)

# Critical columns cannot be NULL
critical_columns = [
    "flight_id",
    "flight_date",
    "departure_airport",
    "actual_departure",
    "scheduled_departure",
    "arrival_airport"
]

for col in critical_columns:
    print(f"3. Checking {col} for NUlls...")
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column=col,
        row_condition=Column("status").is_in(["ON_TIME", "COMPLETED", "DELAYED"]),
        mostly=0.95
    )
    suite.add_expectation(expectation)
    result = batch.validate(expectation)
    results.append(result)
    print(f"   {'✅ PASS' if result.success else '❌ FAIL'} - Unexpected NULLs: {result.result.get('unexpected_count', 0)}")

# ============================================
# CATEGORY 3: VALUE RANGES
# ============================================
print("\n📈 CATEGORY 3: Value Ranges")
print("-" * 60)

# Passenger count in reasonable range
print("4. Checking passenger_count range (1-400)...")
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count",
    min_value = 1,
    max_value = 400,
    mostly=0.95 # 95% of records must be within range
)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")
if not result.success:
    print(f"   Unexpected: {result.result.get('unexpected_count')} records out of range")

# Revenue > 0
print("5. Checking ticket_revenue > 0...")
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="ticket_revenue",
    min_value=0,
    strict_min=True # > 0, not >= 0
)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

# ============================================
# CATEGORY 4: SET MEMBERSHIP (codes)
# ============================================
print("\n🏢 CATEGORY 4: Set Membership")
print("-" * 60)

# Departure airports - LOT Polish Airlines main airports
print("6. Checking departure_airport from LOT airports...")
lot_airports = ["WAW", "KRK", "GDN", "WRO", "KTW", "POZ", "RZE", "SZZ"]

expectation = gx.expectations.ExpectColumnValuesToBeInSet(
    column="departure_airport", 
    value_set=lot_airports,
    mostly=0.95)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")
if not result.success:
    unexpected = result.result.get('partial_unexpected_list', [])
    print(f"   Unexpected airports: {unexpected[:5]}")
#print(result.result.get("missing_count"))

# ============================================
# CATEGORY 5: STRING PATTERNS
# ============================================
print("\n🔤 CATEGORY 5: String Patterns")
print("-" * 60)

# Flight number format: LO + digits (e.g., LO123, LO4567)
print("7. Checking flight_number format (LO + digits)...")
expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
    column="flight_id",
    regex=r"^LO\d{2,4}$"  # LO + 2-4 digits
)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

# Airport codes: 3 uppercase letters (IATA)
print("8. Checking airport codes format (3 letters)...")
for airport_col in ["departure_airport", "arrival_airport"]:
    expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
        column=airport_col,
        regex=r"^[A-Z]{3}$"
    )
    suite.add_expectation(expectation)
    result = batch.validate(expectation)
    results.append(result)
    print(f"   {airport_col}: {'✅ PASS' if result.success else '❌ FAIL'}")

# ============================================
# CATEGORY 6: BUSINESS LOGIC
# ============================================
print("\n💼 CATEGORY 6: Business Rules")
print("-" * 60)

# Revenue > Fuel Cost (otherwise flight is unprofitable)
print("9. Checking revenue > fuel_cost...")
expectation = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="ticket_revenue",
    column_B="fuel_cost",
    mostly=0.9 # 90% of flights must be profitable
)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

# actual departure > scheduled departure (no time travel!)
print("10. Checking departure_time < arrival_time...")

expectation = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="actual_departure_dt",
    column_B="scheduled_departure_dt",
    or_equal = True,
    ignore_row_if="either_value_is_missing",
    row_condition=Column("status") != "CANCELLED",
    mostly=0.9 # 90% of flights must meet this condition
)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

# ============================================
# CATEGORY 7: UNIQUENESS
# ============================================
print("\n🔑 CATEGORY 7: Uniqueness")
print("-" * 60)

# Flight number + flight date should be unique (compound key)
print("11. Checking compound uniqueness (flight + flight_date)...")
expectation = gx.expectations.ExpectCompoundColumnsToBeUnique(
    column_list=["flight_id", "flight_date"]
)
suite.add_expectation(expectation)
result = batch.validate(expectation)
results.append(result)
print(f"   {'✅ PASS' if result.success else '❌ FAIL'}")

context.suites.add_or_update(suite)

print("\n" + "=" * 60)
print("📊 VALIDATION SUMMARY")
print("=" * 60)

if results:  # Tylko jeśli trackujesz results
    passed = sum(1 for r in results if r.success)
    failed = len(results) - passed
    success_rate = (passed / len(results) * 100) if results else 0
    
    print(f"Total expectations: {len(results)}")
    print(f"✅ Passed: {passed} ({success_rate:.1f}%)")
    print(f"❌ Failed: {failed} ({100-success_rate:.1f}%)")
    
    if failed > 0:
        print(f"\n⚠️  Action required: Review failed expectations")
    else:
        print(f"\n🎉 All expectations passed!")
else:
    print("⚠️  No results tracked (add results.append(result) to track)")

print("\n" + "=" * 60)
print("✅ EXPECTATION SUITE SAVED")
print(f"📁 Location: gx/expectations/{suite_name}.json")
print("=" * 60)