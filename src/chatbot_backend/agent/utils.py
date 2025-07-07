import sqlparse
import pickle
import pandas as pd
from collections import defaultdict

def is_safe_query(query: str) -> bool:
    parsed = sqlparse.parse(query)
    for statement in parsed:
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in {"DROP", "DELETE", "INSERT", "UPDATE", "PRAGMA"}:
                return False
    return True

def get_bin_data(parsed_messages)-> dict:
    dataframes = {}
    for msg_type, msg_data in parsed_messages.items():
        if msg_type not in ['FILE', 'PARM']:
            try:
                if isinstance(msg_data, dict):
                    df_data = {}
                    for field_name, field_values in msg_data.items():
                        if isinstance(field_values, list):
                            df_data[field_name] = field_values
                        elif isinstance(field_values, dict):
                            df_data[field_name] = list(field_values.values())
                    
                    df = pd.DataFrame(df_data)
                    if '[' in msg_type:
                        first, number, = msg_type.split('[')[0], msg_type.split('[')[1].split(']')[0]
                        msg_type = first + '_' + number
                    dataframes[msg_type] = df
            except Exception as e:
                print(f"Error processing message type {msg_type}: {e}")
    return dataframes

def get_bin_documentation(dataframes: dict) -> tuple[dict, list, list]:
    with open("documentation.pkl", "rb") as f:
        documentation = pickle.load(f)

    instances_per_type = defaultdict(list)
    columns_per_type = defaultdict(set)
    column_dtype_hint = {}

    for table_key, df in dataframes.items():
        cleaned_key = table_key.split('_')[0]  # e.g., GPS_0 -> GPS
        instances_per_type[cleaned_key].append(table_key)
        columns_per_type[cleaned_key].update(df.columns)

        for col in df.columns:
            if (cleaned_key, col) not in column_dtype_hint:
                column_dtype_hint[(cleaned_key, col)] = str(df[col].dtype)

    documentation_text = ["Available tables in the documentation: " + ", ".join(dataframes.keys()) + "\n"]
    for msg_type, instances in instances_per_type.items():
        if msg_type not in documentation:
            print(f"[Warning] No documentation found for message type: {msg_type}")
            documentation_text.append(f"\n{msg_type}:\n  No documentation available.")
            continue
        
        doc_columns, doc_units, doc_descriptions = list(documentation[msg_type][0]), list(documentation[msg_type][1]), list(documentation[msg_type][2])
        from_doc = {
            col: (unit, desc)
            for col, unit, desc in zip(doc_columns, doc_units, doc_descriptions)
        }
        doc_lines = [f"\n{msg_type} Documentation" + (f" (Found in tables: {', '.join(instances)})" if len(instances) > 1 else f"(Found in table {instances[0]})")]

        for col in sorted(columns_per_type[msg_type]):
            unit, desc = from_doc.get(col, ("", "No description available."))
            dtype = column_dtype_hint.get((msg_type, col), "unknown")

            line = f"  - {col} ({unit}) [{dtype}]: {desc}"
            doc_lines.append(line)

        documentation_text.append("\n".join(doc_lines))

    return "\n".join(documentation_text)