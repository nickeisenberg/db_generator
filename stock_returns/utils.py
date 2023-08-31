def convert_sql_trigger_to_string(filepath):
    lines = []
    with open(filepath, 'r') as f:
        for line in f.readlines():
            lines.append(line.strip())
    sql_code = ""
    for line in lines:
        sql_code += line
        sql_code += " "
    return sql_code
