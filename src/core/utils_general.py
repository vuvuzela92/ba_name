import json
from pathlib import Path

def load_api_tokens(filepath='tokens.json'):
    current_dir = Path(__file__).parent
    tokens_path = current_dir / '..' / '..' / 'creds' / filepath
    with open(tokens_path, 'r', encoding='utf-8') as f:
        tokens = json.load(f)
        return tokens
    
def load_data_file(filename='data.json'):    
    current_dir = Path(__file__).parent
    tokens_path = current_dir / '..' / '..' / 'data' / filename
    with open(tokens_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data
    

def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]