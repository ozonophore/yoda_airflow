import os
import tempfile

import pandas as pd


def copy_data_in_chunks(file_path, sql, conn, chunk_size):
    with open(file_path, 'r') as f:
        cursor = conn.cursor()
        file_size = 0
        for chunk in pd.read_csv(f, chunksize=chunk_size, header=None):
            with tempfile.NamedTemporaryFile(mode='w') as f:
                temp_file_name = f.name
                chunk.to_csv(temp_file_name, index=False, header=False)
                file_size += os.path.getsize(temp_file_name)
                with open(temp_file_name, 'r') as fr:
                    cursor.copy_expert(sql, fr)
        conn.commit()
        print(f"File size: {file_path} bytes")