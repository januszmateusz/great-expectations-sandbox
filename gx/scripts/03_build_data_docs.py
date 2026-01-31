import great_expectations as gx

context = gx.get_context()

# Generuj Data Docs
print("🎨 Building Data Docs...")
context.build_data_docs()

# Otwórz w przeglądarce
print("🌐 Opening Data Docs...")
context.open_data_docs()