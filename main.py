from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os
import shutil
from datetime import datetime
import logging
import openpyxl
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Excel/CSV to SQL Converter")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create necessary directories
os.makedirs("static", exist_ok=True)
os.makedirs("uploads", exist_ok=True)
os.makedirs("downloads", exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


def read_excel_file(file_path: str) -> pd.DataFrame:
    """
    Read Excel file with proper error handling
    """
    try:
        if file_path.endswith('.xls'):
            # For older .xls files
            return pd.read_excel(file_path, engine='xlrd')
        else:
            # For .xlsx files
            return pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        logger.error(f"Error reading Excel file: {str(e)}")
        raise Exception(f"Failed to read Excel file: {str(e)}")


def create_sql_file(df: pd.DataFrame, table_name: str) -> str:
    """
    Create SQL file from DataFrame
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sql_file = f"downloads/{table_name}_{timestamp}.sql"

        with open(sql_file, 'w', encoding='utf-8') as f:
            # Write CREATE TABLE statement
            columns = []
            for col in df.columns:
                # Clean column name
                clean_col = str(col).strip().replace(' ', '_').replace('-', '_')
                # Determine column type based on data
                sample_values = df[col].dropna()
                if len(sample_values) > 0:
                    sample_value = sample_values.iloc[0]
                    if isinstance(sample_value, (int, float)):
                        col_type = 'NUMERIC'
                    else:
                        col_type = 'TEXT'
                else:
                    col_type = 'TEXT'  # Default to TEXT if no data
                columns.append(f"{clean_col} {col_type}")

            f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (\n")
            f.write(',\n'.join(f"    {col}" for col in columns))
            f.write("\n);\n\n")

            # Write INSERT statements
            for _, row in df.iterrows():
                values = []
                for val in row:
                    if pd.isna(val):
                        values.append('NULL')
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        # Escape single quotes and handle special characters
                        val_str = str(val).replace("'", "''")
                        values.append(f"'{val_str}'")

                f.write(f"INSERT INTO {table_name} VALUES ({', '.join(values)});\n")

        return sql_file
    except Exception as e:
        logger.error(f"Error creating SQL file: {str(e)}")
        raise Exception(f"Failed to create SQL file: {str(e)}")


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Handle file upload and conversion
    """
    try:
        # Validate file extension
        file_extension = os.path.splitext(file.filename)[1].lower()
        if file_extension not in ['.csv', '.xls', '.xlsx']:
            raise HTTPException(status_code=400,
                                detail="Invalid file format. Please upload .csv, .xls, or .xlsx files only.")

        # Save uploaded file
        file_path = f"uploads/{file.filename}"
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        except Exception as e:
            logger.error(f"Error saving uploaded file: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to save uploaded file")

        try:
            # Process file based on type
            table_name = os.path.splitext(file.filename)[0].replace(' ', '_').replace('-', '_')

            if file_extension == '.csv':
                logger.debug("Processing CSV file")
                df = pd.read_csv(file_path)
            else:
                logger.debug("Processing Excel file")
                df = read_excel_file(file_path)

            # Create SQL file
            sql_file = create_sql_file(df, table_name)

            return JSONResponse({
                "status": "success",
                "message": "File processed successfully",
                "sql_file": os.path.basename(sql_file)
            })

        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

        finally:
            # Clean up uploaded file
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Cleaned up uploaded file: {file_path}")

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return JSONResponse({
            "status": "error",
            "message": str(e)
        }, status_code=500)


@app.get("/download/{filename}")
async def download_file(filename: str):
    """
    Handle SQL file download
    """
    try:
        file_path = f"downloads/{filename}"
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        return FileResponse(
            file_path,
            media_type="application/sql",
            filename=filename
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail="Error downloading file")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)