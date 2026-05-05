import json
import pandas as pd

INPUT_JSON_FILE = r"C:\Py Scripts\cause_effect_web\cause_effect_data.json"
OUTPUT_EXCEL_FILE = r"C:\Py Scripts\cause_effect_web\JsonExcel.xlsx"

def convert_json_to_excel():
    # Load JSON file
    with open(INPUT_JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("records", [])

    causes_rows = []
    effects_rows = []

    for rec in records:
        # Cause row
        cause_row = {
            "sheet": rec.get("sheet", ""),
            "input_tag": rec.get("input_tag", ""),
            "cause_identifier": rec.get("cause_identifier", ""),
            "signal": rec.get("signal", ""),
            "warn": rec.get("warn", ""),
            "w_dc": rec.get("w_dc", ""),
            "a_dc": rec.get("a_dc", ""),
            "a_dg": rec.get("a_dg", ""),
            "safety_limit": rec.get("safety_limit", ""),
            "hyst": rec.get("hyst", ""),
            "unit": rec.get("unit", ""),
            "delay": rec.get("delay", ""),
            "func1": rec.get("func1", ""),
            "func2": rec.get("func2", ""),
            "func3": rec.get("func3", ""),
            "cause_description": rec.get("cause_description", ""),
            "comment": rec.get("comment", ""),
            "effects_count": len(rec.get("effects", []))
        }

        causes_rows.append(cause_row)

        # Effects rows
        for eff in rec.get("effects", []):
            effect_row = {
                "sheet": rec.get("sheet", ""),
                "input_tag": rec.get("input_tag", ""),
                "cause_identifier": rec.get("cause_identifier", ""),
                "cause_description": rec.get("cause_description", ""),
                "effect_key": eff.get("effect_key", ""),
                "effect_id": eff.get("effect_id", ""),
                "effect_desc": eff.get("effect_desc", ""),
                "output_tag": eff.get("output_tag", ""),
                "action": eff.get("action", ""),
                "marker": eff.get("marker", "")
            }

            effects_rows.append(effect_row)

    # Convert to DataFrames
    df_causes = pd.DataFrame(causes_rows)
    df_effects = pd.DataFrame(effects_rows)

    # Write to Excel
    with pd.ExcelWriter(OUTPUT_EXCEL_FILE, engine="openpyxl") as writer:
        df_causes.to_excel(writer, sheet_name="Causes", index=False)
        df_effects.to_excel(writer, sheet_name="Effects", index=False)

    print(f"Excel file generated successfully: {OUTPUT_EXCEL_FILE}")


if __name__ == "__main__":
    convert_json_to_excel()