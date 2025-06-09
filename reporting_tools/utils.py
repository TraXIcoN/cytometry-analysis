import base64
from io import BytesIO
import pandas as pd

def get_table_download_link(df, filename="data.csv", link_text="Download CSV"):
    """Generates a link to download a pandas DataFrame as a CSV file."""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # Bytes -> Base64 -> String
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{link_text}</a>'
    return href

def get_excel_download_link(df, filename="data.xlsx", link_text="Download Excel"):
    """Generates a link to download a pandas DataFrame as an Excel file."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}">{link_text}</a>'
    return href


def df_to_csv_bytes(df):
    """Converts a pandas DataFrame to CSV bytes."""
    return df.to_csv(index=False).encode('utf-8')

def df_to_excel_bytes(df):
    """Converts a pandas DataFrame to Excel bytes."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()
