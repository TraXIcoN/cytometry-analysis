import base64
from io import BytesIO
import pandas as pd

def get_table_download_link(df, filename="data.csv", link_text="Download CSV"):
    """Generates a link to download a pandas DataFrame as a CSV file."""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # Bytes -> Base64 -> String
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{link_text}</a>'
    return href
