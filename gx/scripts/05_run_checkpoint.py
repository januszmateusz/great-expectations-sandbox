import great_expectations as gx
from pathlib import Path
import pandas as pd

# Setup
GX_ROOT = Path(__file__).resolve().parents[2]
context = gx.get_context(project_root_dir=GX_ROOT)
data_path = GX_ROOT /"gx" / "uncommitted" / "working_files" / "flight_data_sample.csv"

print("🚀 RUNNING CHECKPOINT")
print("=" * 60)

checkpoint_name = "flight_data_checkpoint"

print(f"Checkpoint: {checkpoint_name}")
print(f"Data file: {data_path}")
print(f"File exists: {data_path.exists()}")

# Load data
print(f"\n📊 Loading data...")
df = pd.read_csv(data_path)
print(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")
df["scheduled_departure_dt"] = pd.to_datetime(
    df["scheduled_departure"],
    errors="coerce"
)
df["actual_departure_dt"] = pd.to_datetime(
    df["actual_departure"],
    errors="coerce"
)

# Run checkpoint
print(f"\n🎯 Running checkpoint...")
print("-" * 60)

# Pobierz checkpoint
checkpoint = context.checkpoints.get("flight_data_checkpoint")
result = checkpoint.run(
    batch_parameters={"dataframe": df}
)
print("-" * 60)

# ============================================================
# ANALYZE RESULTS (SIMPLIFIED)
# ============================================================
print(f"\n📊 VALIDATION RESULTS")
print("=" * 60)

success = result.success
print(f"\n🎯 Overall Result: {'✅ PASS' if success else '❌ FAIL'}")

# Get statistics
try:
    first_run_result = list(result.run_results.values())[0]
    stats = first_run_result.statistics
    
    print(f"\n📈 Statistics:")
    print(f"   Total expectations: {stats['evaluated_expectations']}")
    print(f"   ✅ Successful: {stats['successful_expectations']}")
    print(f"   ❌ Failed: {stats['unsuccessful_expectations']}")
    print(f"   Success rate: {stats['success_percent']:.1f}%")
    
    if stats['unsuccessful_expectations'] > 0:
        print(f"\n⚠️  {stats['unsuccessful_expectations']} expectations failed")
        print(f"   See details in Data Docs (run: python gx/scripts/03_build_data_docs.py)")
        
except Exception as e:
    print(f"\n⚠️  Statistics unavailable: {e}")