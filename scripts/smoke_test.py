import pandas as pd
from src.core.models import ColumnSchema, DType, Schema
from src.core.validator import validate

# Schema definieren
schema = Schema(columns=[
    ColumnSchema("name",  DType.STRING, nullable=False),
    ColumnSchema("score", DType.FLOAT,  nullable=False, min_value=0.0, max_value=100.0),
    ColumnSchema("grade", DType.STRING, nullable=True,
                 allowed_values=["A", "B", "C", "D", "F"]),
])

# Valides DataFrame
df_ok = pd.DataFrame({
    "name":  ["Alice", "Bob"],
    "score": [85.0, 92.0],
    "grade": ["A", "B"],
})

# Invalides DataFrame – mehrere Fehler gleichzeitig
df_bad = pd.DataFrame({
    "name":  ["Alice", None],       # None in non-nullable
    "score": [85.0, 150.0],         # 150 > max_value
    "grade": ["A", "X"],            # X nicht in allowed_values
})

report_ok  = validate(df_ok,  schema)
report_bad = validate(df_bad, schema)

print("=== Valides DataFrame ===")
print(report_ok.summary)

print("\n=== Invalides DataFrame ===")
print(report_bad.summary)
for v in report_bad.violations:
    print(f"  [{v.rule}] {v.column}: {v.message}")
    print(f"  Betroffene Zeilen: {v.row_indices}")