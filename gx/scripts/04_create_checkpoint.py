import great_expectations as gx
from pathlib import Path
from great_expectations.checkpoint import UpdateDataDocsAction

GX_ROOT = Path(__file__).resolve().parents[2]
context = gx.get_context(project_root_dir=GX_ROOT)
print("GX root:", context.root_directory)

print("🎯 CREATING CHECKPOINT")
print("=" * 60)

# Parameters
datasource_name = "flight_data_pandas"
asset_name = "flight_data"
batch_def_name = "flight_data_batch"
suite_name = "flight_data_quality_suite"
checkpoint_name = "flight_data_checkpoint"
definition_name = "flight_data_validation_definition"


suite = context.suites.get(suite_name)

batch_definition = (
    context.data_sources.get(datasource_name)
    .get_asset(asset_name)
    .get_batch_definition(batch_def_name)
)

validation_definition = gx.ValidationDefinition(
    data=batch_definition,
    suite=suite,
    name=definition_name
)

validation_definition= context.validation_definitions.add_or_update(validation_definition)

# ============================================================
# ✅ ADD CHECKPOINT WITH ACTIONS (GX 1.11.3)
# ============================================================
action_list = [
    UpdateDataDocsAction(
        name="update_all_data_docs",
    )
]

checkpoint = gx.Checkpoint(
    name=checkpoint_name,
    validation_definitions=[{"name": definition_name}],
    actions=action_list,
    result_format={"result_format": "COMPLETE"}
)

context.checkpoints.add_or_update(checkpoint)

print(f"✅ Checkpoint '{checkpoint_name}' created successfully")

# Verification
print(f"\n🔍 Verifying checkpoint...")
try:
    saved_checkpoint = context.checkpoints.get(checkpoint_name)
    print(f"✅ Checkpoint loaded: {saved_checkpoint.name}")
except Exception as e:
    print(f"⚠️  Verification failed: {e}")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 60)
print("✅ CHECKPOINT SETUP COMPLETE!")
print("=" * 60)

print(f"\n📂 Created:")
print(f"   ✅ Validation Definition: {definition_name}")
print(f"   ✅ Checkpoint: {checkpoint_name}")
print(f"   ✅ Action: UpdateDataDocsAction")

print(f"\n🚀 Next step: RUN THE CHECKPOINT!")
print(f"   Create: gx/scripts/05_run_checkpoint.py")
