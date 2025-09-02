# db.py
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL') or os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError('DATABASE_URL not set. Copy .env.example to .env and set DATABASE_URL.')

engine = create_engine(DATABASE_URL, echo=False)

def init_db():
    with engine.begin() as conn:
        sql = open('models.sql').read()
        conn.execute(text(sql))

if __name__ == '__main__':
    init_db()
    print('DB initialized')
