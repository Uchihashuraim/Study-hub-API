import os
import re
import httpx
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# 1. Database Configuration
# Render provides a DATABASE_URL environment variable. 
# Fallback to local SQLite for testing on your machine before deploying.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./local_test.db")

# Render's default Postgres URL starts with 'postgres://', which SQLAlchemy 1.4+ deprecated
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL, 
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. Database Model
class PDFRecord(Base):
    __tablename__ = "pdf_files"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    drive_link = Column(String, nullable=False)

# Create tables automatically on startup
Base.metadata.create_all(bind=engine)

app = FastAPI(title="PDF Drive Streamer API")

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Schema for adding new entries
class PDFCreate(BaseModel):
    name: str
    drive_link: str

# Helper to extract the File ID from any standard Google Drive URL
def extract_drive_id(url_or_id: str) -> str:
    pattern = r"(?:id=|/d/)([a-zA-Z0-9_-]{25,})"
    match = re.search(pattern, url_or_id)
    if match:
        return match.group(1)
    return url_or_id.strip() # Assume it's already a raw file ID if no URL pattern matches

# 3. Endpoints

@app.post("/add-pdf")
def add_pdf(item: PDFCreate, db: Session = Depends(get_db)):
    """Add or update a name-link pair in the database."""
    existing = db.query(PDFRecord).filter(PDFRecord.name == item.name).first()
    if existing:
        existing.drive_link = item.drive_link
        db.commit()
        return {"message": f"Updated link for '{item.name}'"}
    
    new_record = PDFRecord(name=item.name, drive_link=item.drive_link)
    db.add(new_record)
    db.commit()
    return {"message": f"Added '{item.name}' to database."}

@app.get("/pdf/{name}")
async def get_pdf(name: str, db: Session = Depends(get_db)):
    """Fetch the PDF from Google Drive by name and stream it to the client."""
    record = db.query(PDFRecord).filter(PDFRecord.name == name).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"No PDF found under the name '{name}'")

    file_id = extract_drive_id(record.drive_link)
    # Direct download endpoint for Google Drive
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

    async def stream_from_drive():
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            async with client.stream("GET", download_url) as response:
                if response.status_code != 200:
                    raise HTTPException(
                        status_code=502, 
                        detail="Could not fetch file from Google Drive. Ensure link sharing is set to 'Anyone with the link'."
                    )
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk

    headers = {
        # 'attachment' prompts a download; change to 'inline' if you want it to open in a browser tab
        "Content-Disposition": f'attachment; filename="{name}.pdf"'
    }

    return StreamingResponse(
        stream_from_drive(), 
        media_type="application/pdf", 
        headers=headers
    )
