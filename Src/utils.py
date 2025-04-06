import json

def read_json_file(path: str):
    with open(path, "r") as f:
        data = json.load(f)

    return data