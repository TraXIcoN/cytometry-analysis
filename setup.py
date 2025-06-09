from setuptools import setup, find_packages

setup(
    name="teiko-cytometry",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "streamlit==1.29.0",
        "pandas==2.0.3",
        "sqlalchemy==2.0.23",
        "plotly==5.18.0",
        "scipy==1.11.4",
        "redis==5.0.1",
        "upstash-redis==0.1.0",
        "aiohttp==3.9.1",
        "asyncio==3.4.3",
        "python-dotenv==1.0.0",
        "openpyxl==3.1.2",
        "reportlab==4.2.0",
        "kaleido==0.2.1"
    ],
    python_requires='>=3.8',
)
