"""
Handles the generation of PDF reports.
"""
import io
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib import colors
import pandas as pd
import plotly.io as pio


def df_to_reportlab_table(df):
    """Converts a pandas DataFrame to a ReportLab Table object."""
    data = [df.columns.to_list()] + df.values.tolist()
    table = Table(data)
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ])
    table.setStyle(style)
    return table

def fig_to_reportlab_image(fig, width=6*inch):
    """Converts a Plotly figure to a ReportLab Image object."""
    img_bytes = pio.to_image(fig, format='png', scale=2) # Increase scale for better resolution
    img_file = io.BytesIO(img_bytes)
    img = Image(img_file, width=width, height=width * (fig.layout.height / fig.layout.width if fig.layout.width and fig.layout.height else 0.6))
    return img

def generate_pdf_report(summary_df, plots=None, stats=None, filename="cytometry_report.pdf"):
    """Generates a PDF report with summary data, plots, and statistics."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                        rightMargin=72, leftMargin=72,
                        topMargin=72, bottomMargin=18)
    styles = getSampleStyleSheet()
    story = []

    # Title
    story.append(Paragraph("Cytometry Analysis Report", styles['h1']))
    story.append(Spacer(1, 0.2*inch))

    # Statistics
    if stats:
        story.append(Paragraph("Key Statistics:", styles['h2']))
        if isinstance(stats, dict):
            for key, value in stats.items():
                story.append(Paragraph(f"<b>{key}:</b> {value}", styles['Normal']))
        elif isinstance(stats, str):
            story.append(Paragraph(stats, styles['Normal']))
        story.append(Spacer(1, 0.2*inch))

    # Summary Data Table
    if summary_df is not None and not summary_df.empty:
        story.append(Paragraph("Summary Data:", styles['h2']))
        table = df_to_reportlab_table(summary_df.head(20)) # Display first 20 rows
        story.append(table)
        story.append(Spacer(1, 0.2*inch))

    # Plots
    if plots:
        story.append(Paragraph("Plots:", styles['h2']))
        for i, plot_fig in enumerate(plots):
            if plot_fig:
                story.append(Paragraph(f"Plot {i+1}: {plot_fig.layout.title.text if plot_fig.layout.title else ''}", styles['h3']))
                img = fig_to_reportlab_image(plot_fig)
                story.append(img)
                story.append(Spacer(1, 0.2*inch))
    
    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes
