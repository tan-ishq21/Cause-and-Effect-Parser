import os
import json
import pandas as pd
from flask import Flask, request, jsonify, render_template
from openpyxl import load_workbook



app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(BASE_DIR, "data", "ELYP-300_PFA020_C&E-Matrix-ElySys_V3.4_001_IN_WORK (1).xlsx")
OUTPUT_JSON_PATH = os.path.join(BASE_DIR, "cause_effect_data.json")

ALLOWED_MARKERS = {"S", "S*", "N", "N*", "NP", "NP*"}

# This will store everything loaded from JSON
master_data = {}

def read_excel_sheet_with_merged(EXCEL_PATH, sheet_name):
    wb = load_workbook(EXCEL_PATH, data_only=True)
    ws = wb[sheet_name]

    # Read raw data into matrix
    data = []
    for row in ws.iter_rows(values_only=True):
        data.append(list(row))

    # Fill merged cells inside the matrix
    for merged_range in ws.merged_cells.ranges:
        min_row = merged_range.min_row - 1  # convert to 0-based
        min_col = merged_range.min_col - 1
        max_row = merged_range.max_row - 1
        max_col = merged_range.max_col - 1

        top_left_value = data[min_row][min_col]

        for r in range(min_row, max_row + 1):
            for c in range(min_col, max_col + 1):
                data[r][c] = top_left_value

    return pd.DataFrame(data)

# ============================================================
# NORMALIZATION HELPERS
# ============================================================
def normalize_text(val: str) -> str:
    """
    Strong normalization for Excel text values.
    Removes newlines, tabs, extra spaces.
    Converts to uppercase.
    """
    if val is None:
        return ""

    text = str(val)

    # remove line breaks and tabs
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")

    # remove multiple spaces
    text = " ".join(text.split())

    return text.strip().upper()


def normalize_cell(val) -> str:
    if pd.isna(val):
        return ""
    return normalize_text(val)


def find_cell(df, targets):
    """
    Find the first occurrence of any target string in the dataframe.
    Returns (row, col) or (None, None)
    """
    targets_normalized = [normalize_text(t) for t in targets]

    for r in range(df.shape[0]):
        for c in range(df.shape[1]):
            cell = normalize_cell(df.iat[r, c])
            if cell in targets_normalized:
                return r, c

    return None, None


# ============================================================
# EFFECT DETECTION
# ============================================================
def detect_effect_region(df):
    """
    Detect effect header region.
    Supports both formats:
    - Sheets with Effect Identifier row
    - Sheets without Effect Identifier (only Effect Description row)
    """

    r_id, c_id = find_cell(df, ["Effect Identifier"])
    if r_id is not None:
        return {
            "has_identifier": True,
            "effect_identifier_row": r_id,
            "effect_description_row": r_id + 1,
            "output_tag_row": r_id + 3,
            "action_row": r_id + 4,
            "matrix_start_row": r_id + 5,
            "effects_start_col": c_id + 1
        }

    r_desc, c_desc = find_cell(df, ["Effect Description"])
    if r_desc is not None:
        return {
            "has_identifier": False,
            "effect_identifier_row": None,
            "effect_description_row": r_desc,
            "output_tag_row": r_desc + 2,
            "action_row": r_desc + 3,
            "matrix_start_row": r_desc + 4,
            "effects_start_col": c_desc + 1
        }

    return None


def detect_effect_columns(df, effect_description_row, start_col):
    """
    Scan right until 3 consecutive blank effect headers found.
    """
    blank_count = 0
    end_col = start_col

    for c in range(start_col, df.shape[1]):
        header = normalize_cell(df.iat[effect_description_row, c])

        if header == "":
            blank_count += 1
            if blank_count >= 3:
                break
        else:
            blank_count = 0
            end_col = c

    return start_col, end_col


# ============================================================
# CAUSE HEADER DETECTION
# ============================================================
def detect_cause_columns(df):
    """
    Detect cause-side columns by scanning a window of rows.
    Returns dict of column indexes.
    """

    header_map = {
        "cause_identifier": ["Cause Identifier", "Cause ID", "Cause"],
        "input_tag": ["Input Tag", "InputTag", "Tag"],
        "signal": ["Signal"],
        "warn": ["Warn", "Warning"],
        "safety_limit": ["Safety Limit", "SafetyLimit" , "Switch/Limit"],
        "hyst": ["Hyst.", "Hysteresis"],
        "unit": ["Unit"],
        "delay": ["Delay"],
        "func1": ["Func 1", "Func1"],
        "func3": ["Func 3", "Func3"],
        "cause_description": ["Cause Description", "Description"],
        "comment": ["Comment", "Remarks", "Remark"], 
        "w_dc": ["W_DC", "W DC", "W-DC"],
        "a_dc": ["A_DC", "A DC", "A-DC"],
        "a_dg": ["A_DG", "A DG", "A-DG"],
        "func2": ["Func 2", "Func2"]
    }

    # Normalize variants
    header_map_norm = {k: [normalize_text(x) for x in v] for k, v in header_map.items()}

    found_cols = {}
    header_row_found = None

    # Find input tag header row first
    for r in range(df.shape[0]):
        row_values = [normalize_cell(x) for x in df.iloc[r].values]
        for c, val in enumerate(row_values):
            if val in header_map_norm["input_tag"]:
                found_cols["input_tag"] = c
                header_row_found = r
                break
        if header_row_found is not None:
            break

    if header_row_found is None:
        return None

    # Scan around header row to find all other headers
    start_scan = max(0, header_row_found - 3)
    end_scan = min(df.shape[0], header_row_found + 8)

    for r in range(start_scan, end_scan):
        row_values = [normalize_cell(x) for x in df.iloc[r].values]
        for c, val in enumerate(row_values):
            for key, variants in header_map_norm.items():
                if val in variants:
                    found_cols[key] = c

    return found_cols


# ============================================================
# EXTRACT SHEET
# ============================================================
def extract_sheet(sheet_name, df):
    """
    Extract all cause/effect rows from a sheet.
    """

    region = detect_effect_region(df)
    if not region:
        print(f"[SKIP] No Effect region in sheet: {sheet_name}")
        return []

    effects_start_col, effects_end_col = detect_effect_columns(
        df,
        region["effect_description_row"],
        region["effects_start_col"]
    )

    # Extract effect metadata
    effects_metadata = []
    for c in range(effects_start_col, effects_end_col + 1):

        effect_id = ""
        if region["has_identifier"]:
            effect_id = normalize_cell(df.iat[region["effect_identifier_row"], c])

        effect_desc = normalize_cell(df.iat[region["effect_description_row"], c])
        output_tag = normalize_cell(df.iat[region["output_tag_row"], c])
        action = normalize_cell(df.iat[region["action_row"], c])

        # Skip completely blank columns
        if effect_desc == "" and effect_id == "" and output_tag == "" and action == "":
            continue

        # IMPORTANT FIX:
        # Make effect unique per column by including output_tag/action
        unique_effect_key = " | ".join([
            effect_id if effect_id else effect_desc,
            output_tag,
            action,
            f"COL_{c}"
        ])

        effects_metadata.append({
            "col": c,
            "effect_key": unique_effect_key,   # NEW
            "effect_id": effect_id if effect_id else effect_desc,
            "effect_desc": effect_desc,
            "output_tag": output_tag,
            "action": action
        })

    cause_cols = detect_cause_columns(df)
    if not cause_cols or "input_tag" not in cause_cols:
        print(f"[SKIP] No Input Tag column in sheet: {sheet_name}")
        return []

    records = []
    empty_count = 0

    for r in range(region["matrix_start_row"], df.shape[0]):

        input_tag = normalize_cell(df.iat[r, cause_cols["input_tag"]])

        # Stop when many consecutive blanks
        if input_tag == "":
            empty_count += 1
            if empty_count >= 15:
                break
            continue
        else:
            empty_count = 0

        record = {
            "sheet": sheet_name,
            "input_tag": input_tag,
            "cause_identifier": normalize_cell(df.iat[r, cause_cols["cause_identifier"]]) if "cause_identifier" in cause_cols else "",
            "signal": normalize_cell(df.iat[r, cause_cols["signal"]]) if "signal" in cause_cols else "",
            "warn": normalize_cell(df.iat[r, cause_cols["warn"]]) if "warn" in cause_cols else "",

            "w_dc": normalize_cell(df.iat[r, cause_cols["w_dc"]]) if "w_dc" in cause_cols else "",
            "a_dc": normalize_cell(df.iat[r, cause_cols["a_dc"]]) if "a_dc" in cause_cols else "",
            "a_dg": normalize_cell(df.iat[r, cause_cols["a_dg"]]) if "a_dg" in cause_cols else "",

            "safety_limit": normalize_cell(df.iat[r, cause_cols["safety_limit"]]) if "safety_limit" in cause_cols else "",
            "hyst": normalize_cell(df.iat[r, cause_cols["hyst"]]) if "hyst" in cause_cols else "",
            "unit": normalize_cell(df.iat[r, cause_cols["unit"]]) if "unit" in cause_cols else "",
            "delay": normalize_cell(df.iat[r, cause_cols["delay"]]) if "delay" in cause_cols else "",

            "func1": normalize_cell(df.iat[r, cause_cols["func1"]]) if "func1" in cause_cols else "",
            "func2": normalize_cell(df.iat[r, cause_cols["func2"]]) if "func2" in cause_cols else "",
            "func3": normalize_cell(df.iat[r, cause_cols["func3"]]) if "func3" in cause_cols else "",

            "cause_description": normalize_cell(df.iat[r, cause_cols["cause_description"]]) if "cause_description" in cause_cols else "",
            "comment": normalize_cell(df.iat[r, cause_cols["comment"]]) if "comment" in cause_cols else "",
            "effects": []
        }

        # Extract triggered effects
        for eff in effects_metadata:
            marker = normalize_cell(df.iat[r, eff["col"]])
            if marker in ALLOWED_MARKERS:
                record["effects"].append({
                    "effect_key": eff["effect_key"],   # NEW
                    "effect_id": eff["effect_id"],
                    "effect_desc": eff["effect_desc"],
                    "output_tag": eff["output_tag"],
                    "action": eff["action"],
                    "marker": marker
                })

        records.append(record)

    return records


# ============================================================
# BUILD MASTER JSON
# ============================================================
def build_master_json(records):
    """
    Create one JSON structure with indexes.
    Index stores record indexes (not full records).
    """

    index_by_input_tag = {}
    index_by_cause_identifier = {}

    for i, rec in enumerate(records):
        tag = normalize_text(rec.get("input_tag", ""))
        if tag:
            index_by_input_tag.setdefault(tag, []).append(i)

        sheet_name = normalize_text(rec.get("sheet", ""))

        # Only SIS sheet should be indexed by cause identifier
        if sheet_name == "ELY SYSTEM - SIS":
            cid = normalize_text(rec.get("cause_identifier", ""))
            if cid:
                index_by_cause_identifier.setdefault(cid, []).append(i)

    return {
        "total_records": len(records),
        "records": records,
        "index_by_input_tag": index_by_input_tag,
        "index_by_cause_identifier": index_by_cause_identifier
    }


def save_master_json(master_json):
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(master_json, f, indent=4, ensure_ascii=False)

    print(f"[INFO] JSON saved: {OUTPUT_JSON_PATH}")


def create_json_from_excel():
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")

    wb = load_workbook(EXCEL_PATH, data_only=True)
    all_records = []

    for sheet in wb.sheetnames:
        df = read_excel_sheet_with_merged(EXCEL_PATH, sheet)
        sheet_records = extract_sheet(sheet, df)
        all_records.extend(sheet_records)

    master_json = build_master_json(all_records)
    save_master_json(master_json)


def load_master_json():
    global master_data

    # If JSON does not exist, create it first
    if not os.path.exists(OUTPUT_JSON_PATH):
        print("[INFO] JSON not found. Creating JSON from Excel...")
        create_json_from_excel()

    with open(OUTPUT_JSON_PATH, "r", encoding="utf-8") as f:
        master_data = json.load(f)

    print(f"[INFO] Loaded JSON records: {master_data.get('total_records', 0)}")


# ============================================================
# SEARCH LOGIC (THIS IS THE FIXED PART)
# ============================================================
def search_records(query):
    """
    Correct search logic:
    - Normalize query
    - Search in input_tag index keys (substring)
    - Search in cause_identifier index keys (substring)
    - Collect record indexes, remove duplicates
    - Return grouped by sheet
    """

    q = normalize_text(query)

    record_indexes = set()

    # Search by Input Tag (all sheets)
    for key, idx_list in master_data.get("index_by_input_tag", {}).items():
        if q in key:
            record_indexes.update(idx_list)

    # Search by Cause Identifier (only SIS)
    for key, idx_list in master_data.get("index_by_cause_identifier", {}).items():
        if q in key:
            record_indexes.update(idx_list)

    grouped = {}

    for idx in record_indexes:
        rec = master_data["records"][idx]
        grouped.setdefault(rec["sheet"], []).append(rec)

    # Sort results inside each sheet by input_tag for better view
    for sheet in grouped:
        grouped[sheet].sort(key=lambda x: x.get("input_tag", ""))

    return grouped


# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/")
def home():
    return render_template("index.html")


@app.route("/search", methods=["GET"])
def search_api():
    query = request.args.get("q", "").strip()

    if not query:
        return jsonify({"error": "Empty search query"}), 400

    results = search_records(query)

    return jsonify({
        "query": query,
        "count": sum(len(v) for v in results.values()),
        "results": results
    })


@app.route("/data", methods=["GET"])
def show_data():
    return jsonify({
        "total_records": master_data.get("total_records", 0),
        "sample": master_data.get("records", [])[:5]
    })


@app.route("/reload", methods=["GET"])
def reload_data():
    """
    Rebuild JSON from Excel and reload into memory.
    """
    create_json_from_excel()
    load_master_json()
    return jsonify({"status": "JSON rebuilt and reloaded successfully"})


@app.route("/debug_keys", methods=["GET"])
def debug_keys():
    """
    Debug endpoint: show some keys from indexes.
    Helps verify that indexing is correct.
    """
    input_keys = list(master_data.get("index_by_input_tag", {}).keys())[:20]
    cause_keys = list(master_data.get("index_by_cause_identifier", {}).keys())[:20]

    return jsonify({
        "sample_input_tag_keys": input_keys,
        "sample_cause_identifier_keys": cause_keys
    })


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    load_master_json()
    app.run(host="0.0.0.0", port=5000, debug=True)