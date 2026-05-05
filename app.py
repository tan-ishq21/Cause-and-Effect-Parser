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
ALLOWED_VOTING = {"1OO1", "1OO2", "2OO2", "1OO3", "2OO3", "3OO3", "2OO4", "3OO4"}

master_data = {}


# ============================================================
# EXCEL READ WITH MERGED CELLS FILLED
# ============================================================
def read_excel_sheet_with_merged(excel_path, sheet_name):
    wb = load_workbook(excel_path, data_only=True)
    ws = wb[sheet_name]

    data = []
    for row in ws.iter_rows(values_only=True):
        data.append(list(row))

    # Fill merged cells
    for merged_range in ws.merged_cells.ranges:
        min_row = merged_range.min_row - 1
        min_col = merged_range.min_col - 1
        max_row = merged_range.max_row - 1
        max_col = merged_range.max_col - 1

        top_left_value = data[min_row][min_col]

        for r in range(min_row, max_row + 1):
            for c in range(min_col, max_col + 1):
                data[r][c] = top_left_value

    return pd.DataFrame(data)


# ============================================================
# NORMALIZATION
# ============================================================
def normalize_text(val) -> str:
    if val is None:
        return ""
    text = str(val)
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = " ".join(text.split())
    return text.strip().upper()


def normalize_cell(val) -> str:
    if pd.isna(val):
        return ""
    return normalize_text(val)


def find_cell(df, targets):
    targets_normalized = [normalize_text(t) for t in targets]

    for r in range(df.shape[0]):
        for c in range(df.shape[1]):
            cell = normalize_cell(df.iat[r, c])
            if cell in targets_normalized:
                return r, c
    return None, None


# ============================================================
# EFFECT REGION DETECTION
# ============================================================
def detect_effect_region(df):
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
    header_map = {
        "cause_identifier": ["Cause Identifier", "Cause ID", "Cause"],
        "input_tag": ["Input Tag", "InputTag", "Tag"],
        "signal": ["Signal"],
        "warn": ["Warn", "Warning"],
        "safety_limit": ["Safety Limit", "SafetyLimit", "Switch/Limit"],
        "hyst": ["Hyst.", "Hysteresis"],
        "unit": ["Unit"],
        "delay": ["Delay"],
        "func1": ["Func 1", "Func1"],
        "func2": ["Func 2", "Func2"],
        "func3": ["Func 3", "Func3"],
        "cause_description": ["Cause Description", "Description"],
        "comment": ["Comment", "Remarks", "Remark"],
        "w_dc": ["W_DC", "W DC", "W-DC"],
        "a_dc": ["A_DC", "A DC", "A-DC"],
        "a_dg": ["A_DG", "A DG", "A-DG"],
    }

    header_map_norm = {k: [normalize_text(x) for x in v] for k, v in header_map.items()}

    found_cols = {}
    header_row_found = None

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
# DYNAMIC LOGIC COLUMN DETECTION (FOR YOUR EXCEL FORMAT)
# ============================================================
def detect_logic_columns(df, cause_cols, start_row):
    """
    Your Excel contains AND + 1oo2 logic stored inside Func columns.
    But it is not always consistent.
    So we scan first 50 rows after matrix start and find which Func column
    contains most 'AND' and which contains most voting strings.
    """

    func_candidates = []
    for key in ["func1", "func2", "func3"]:
        if key in cause_cols:
            func_candidates.append((key, cause_cols[key]))

    if not func_candidates:
        return {"and_col": None, "voting_col": None}

    scan_end = min(df.shape[0], start_row + 50)

    and_counts = {col: 0 for _, col in func_candidates}
    voting_counts = {col: 0 for _, col in func_candidates}

    for r in range(start_row, scan_end):
        for _, col in func_candidates:
            val = normalize_cell(df.iat[r, col])
            val_clean = val.replace(" ", "")

            if val == "AND":
                and_counts[col] += 1
            if val_clean in ALLOWED_VOTING:
                voting_counts[col] += 1

    and_col = max(and_counts, key=and_counts.get) if max(and_counts.values()) > 0 else None
    voting_col = max(voting_counts, key=voting_counts.get) if max(voting_counts.values()) > 0 else None

    return {"and_col": and_col, "voting_col": voting_col}


# ============================================================
# SHEET EXTRACTION WITH SAFETY LOGIC BLOCKS
# ============================================================
def extract_sheet(sheet_name, ws, df):
    region = detect_effect_region(df)
    if not region:
        print(f"[SKIP] No Effect region in sheet: {sheet_name}")
        return {"records": [], "logic_blocks": []}

    effects_start_col, effects_end_col = detect_effect_columns(
        df,
        region["effect_description_row"],
        region["effects_start_col"]
    )

    # ============================================================
    # Build Effect Metadata
    # ============================================================
    effects_metadata = []
    for c in range(effects_start_col, effects_end_col + 1):

        effect_id = ""
        if region["has_identifier"]:
            effect_id = normalize_cell(df.iat[region["effect_identifier_row"], c])

        effect_desc = normalize_cell(df.iat[region["effect_description_row"], c])
        output_tag = normalize_cell(df.iat[region["output_tag_row"], c])
        action = normalize_cell(df.iat[region["action_row"], c])

        if effect_desc == "" and effect_id == "" and output_tag == "" and action == "":
            continue

        unique_effect_key = " | ".join([
            effect_id if effect_id else effect_desc,
            output_tag,
            action,
            f"COL_{c}"
        ])

        effects_metadata.append({
            "col": c,
            "effect_key": unique_effect_key,
            "effect_id": effect_id if effect_id else effect_desc,
            "effect_desc": effect_desc,
            "output_tag": output_tag,
            "action": action
        })

    # ============================================================
    # Detect Cause Columns
    # ============================================================
    cause_cols = detect_cause_columns(df)
    if not cause_cols or "input_tag" not in cause_cols:
        print(f"[SKIP] No Input Tag column in sheet: {sheet_name}")
        return {"records": [], "logic_blocks": []}

    logic_cols = detect_logic_columns(df, cause_cols, region["matrix_start_row"])
    and_col = logic_cols["and_col"]
    voting_col = logic_cols["voting_col"]

    if and_col is None or voting_col is None:
        print(f"[WARN] Missing AND/Voting cols in sheet {sheet_name}")
        return {"records": [], "logic_blocks": []}

    # ============================================================
    # MERGED RANGE LOOKUP HELPERS
    # ============================================================
    merged_ranges = list(ws.merged_cells.ranges)

    def get_merged_range_id(row_0, col_0):
        """
        Return merged range id string for a cell (0-based row/col).
        If not merged, return None.
        """
        row = row_0 + 1
        col = col_0 + 1

        for rng in merged_ranges:
            if rng.min_row <= row <= rng.max_row and rng.min_col <= col <= rng.max_col:
                return f"{rng.min_row}:{rng.min_col}-{rng.max_row}:{rng.max_col}"
        return None

    # ============================================================
    # MAIN EXTRACTION
    # ============================================================
    records = []
    logic_blocks = []

    current_block = None
    current_and_group = None

    prev_voting_merge_id = None
    prev_and_merge_id = None

    empty_count = 0

    for r in range(region["matrix_start_row"], df.shape[0]):

        input_tag = normalize_cell(df.iat[r, cause_cols["input_tag"]])

        if input_tag == "":
            empty_count += 1
            if empty_count >= 15:
                break
            continue
        else:
            empty_count = 0

        # Voting and AND values
        and_val = normalize_cell(df.iat[r, and_col])
        voting_val = normalize_cell(df.iat[r, voting_col]).replace(" ", "")

        cause_desc = normalize_cell(df.iat[r, cause_cols["cause_description"]]) if "cause_description" in cause_cols else ""
        comment = normalize_cell(df.iat[r, cause_cols["comment"]]) if "comment" in cause_cols else ""

        # merged range id detection
        voting_merge_id = get_merged_range_id(r, voting_col)
        and_merge_id = get_merged_range_id(r, and_col)

        # ============================================================
        # Extract Row Effects
        # ============================================================
        row_effects = []
        for eff in effects_metadata:
            marker = normalize_cell(df.iat[r, eff["col"]])
            if marker in ALLOWED_MARKERS:
                row_effects.append({
                    "effect_key": eff["effect_key"],
                    "effect_id": eff["effect_id"],
                    "effect_desc": eff["effect_desc"],
                    "output_tag": eff["output_tag"],
                    "action": eff["action"],
                    "marker": marker
                })

        record = {
            "sheet": sheet_name,
            "row": r,
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
            "cause_description": cause_desc,
            "comment": comment,
            "and_logic": and_val,
            "voting_logic": voting_val,
            "effects": row_effects
        }

        records.append(record)

        # ============================================================
        # START NEW VOTING BLOCK WHEN MERGED CELL BREAKS
        # ============================================================
        if voting_merge_id != prev_voting_merge_id:
            if current_block is not None:
                logic_blocks.append(current_block)

            current_block = {
                "sheet": sheet_name,
                "block_id": f"{normalize_text(sheet_name)}_BLOCK_{len(logic_blocks)+1}",
                "voting_logic": voting_val,
                "cause_description": cause_desc,
                "comment": comment,
                "has_and_logic": False,
                "and_groups": [],
                "effects": {}
            }

            current_and_group = None
            prev_and_merge_id = None

        if current_block is None:
            current_block = {
                "sheet": sheet_name,
                "block_id": f"{normalize_text(sheet_name)}_BLOCK_{len(logic_blocks)+1}",
                "voting_logic": voting_val,
                "cause_description": cause_desc,
                "comment": comment,
                "has_and_logic": False,
                "and_groups": [],
                "effects": {}
            }

        # ============================================================
        # START NEW AND GROUP WHEN MERGED CELL BREAKS
        # ============================================================
        if and_val == "AND":
            current_block["has_and_logic"] = True
            if and_merge_id != prev_and_merge_id:
                current_and_group = {
                    "group_id": f"AND_{len(current_block['and_groups'])+1}",
                    "tags": []
                }
                current_block["and_groups"].append(current_and_group)

        else:
            # standalone row
            current_and_group = {
                "group_id": f"SINGLE_{len(current_block['and_groups'])+1}",
                "tags": []
            }
            current_block["and_groups"].append(current_and_group)

        # ============================================================
        # ADD TAG TO GROUP
        # ============================================================
        current_and_group["tags"].append({
            "input_tag": record["input_tag"],
            "cause_identifier": record["cause_identifier"],
            "signal": record["signal"],
            "warn": record["warn"],
            "limit": record["safety_limit"],
            "hyst": record["hyst"],
            "unit": record["unit"],
            "delay": record["delay"],
            "comment": record["comment"]
        })

        # ============================================================
        # EFFECT AGGREGATION (NO COUNTS)
        # ============================================================
        for eff in row_effects:
            key = eff["effect_key"]

            if key not in current_block["effects"]:
                current_block["effects"][key] = {
                    "effect_id": eff["effect_id"],
                    "effect_desc": eff["effect_desc"],
                    "output_tag": eff["output_tag"],
                    "action": eff["action"],
                    "markers": []
                }

            if eff["marker"] not in current_block["effects"][key]["markers"]:
                current_block["effects"][key]["markers"].append(eff["marker"])

        # update prev merged ids
        prev_voting_merge_id = voting_merge_id
        prev_and_merge_id = and_merge_id

    if current_block is not None:
        logic_blocks.append(current_block)

    return {"records": records, "logic_blocks": logic_blocks}

# ============================================================
# MASTER JSON BUILD
# ============================================================
def build_master_json(all_records, all_logic_blocks):
    index_by_input_tag = {}
    index_by_cause_identifier = {}
    index_by_logic_block_tag = {}

    for i, rec in enumerate(all_records):
        tag = normalize_text(rec.get("input_tag", ""))
        if tag:
            index_by_input_tag.setdefault(tag, []).append(i)

        sheet_name = normalize_text(rec.get("sheet", ""))

        if sheet_name == "ELY SYSTEM - SIS":
            cid = normalize_text(rec.get("cause_identifier", ""))
            if cid:
                index_by_cause_identifier.setdefault(cid, []).append(i)

    # index blocks by tags inside AND groups
    for bi, block in enumerate(all_logic_blocks):
        for grp in block.get("and_groups", []):
            for tag_obj in grp.get("tags", []):
                t = normalize_text(tag_obj.get("input_tag", ""))
                if t:
                    index_by_logic_block_tag.setdefault(t, []).append(bi)

    return {
        "total_records": len(all_records),
        "total_logic_blocks": len(all_logic_blocks),
        "records": all_records,
        "logic_blocks": all_logic_blocks,
        "index_by_input_tag": index_by_input_tag,
        "index_by_cause_identifier": index_by_cause_identifier,
        "index_by_logic_block_tag": index_by_logic_block_tag
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
    all_logic_blocks = []

    for sheet in wb.sheetnames:
        ws = wb[sheet]
        df = read_excel_sheet_with_merged(EXCEL_PATH, sheet)

        extracted = extract_sheet(sheet, ws, df)

        all_records.extend(extracted["records"])
        all_logic_blocks.extend(extracted["logic_blocks"])

    master_json = build_master_json(all_records, all_logic_blocks)
    save_master_json(master_json)


def load_master_json():
    global master_data

    if not os.path.exists(OUTPUT_JSON_PATH):
        print("[INFO] JSON not found. Creating JSON from Excel...")
        create_json_from_excel()

    with open(OUTPUT_JSON_PATH, "r", encoding="utf-8") as f:
        master_data = json.load(f)

    print(f"[INFO] Loaded records: {master_data.get('total_records', 0)}")
    print(f"[INFO] Loaded logic blocks: {master_data.get('total_logic_blocks', 0)}")


# ============================================================
# SEARCH
# ============================================================
def search_logic_blocks(query):
    q = normalize_text(query)

    block_indexes = set()
    for tag, idx_list in master_data.get("index_by_logic_block_tag", {}).items():
        if q in tag:
            block_indexes.update(idx_list)

    grouped = {}
    for bi in block_indexes:
        block = master_data["logic_blocks"][bi]
        grouped.setdefault(block["sheet"], []).append(block)

    return grouped


def search_records(query):
    q = normalize_text(query)

    record_indexes = set()

    for key, idx_list in master_data.get("index_by_input_tag", {}).items():
        if q in key:
            record_indexes.update(idx_list)

    for key, idx_list in master_data.get("index_by_cause_identifier", {}).items():
        if q in key:
            record_indexes.update(idx_list)

    grouped = {}
    for idx in record_indexes:
        rec = master_data["records"][idx]
        grouped.setdefault(rec["sheet"], []).append(rec)

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

    record_results = search_records(query)
    logic_results = search_logic_blocks(query)

    return jsonify({
        "query": query,
        "record_count": sum(len(v) for v in record_results.values()),
        "logic_block_count": sum(len(v) for v in logic_results.values()),
        "records": record_results,
        "logic_blocks": logic_results
    })


@app.route("/reload", methods=["GET"])
def reload_data():
    create_json_from_excel()
    load_master_json()
    return jsonify({"status": "JSON rebuilt and reloaded successfully"})


@app.route("/debug_keys", methods=["GET"])
def debug_keys():
    return jsonify({
        "sample_input_tag_keys": list(master_data.get("index_by_input_tag", {}).keys())[:20],
        "sample_logic_block_keys": list(master_data.get("index_by_logic_block_tag", {}).keys())[:20],
        "total_blocks": master_data.get("total_logic_blocks", 0)
    })


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    load_master_json()
    app.run(host="0.0.0.0", port=5000, debug=True)