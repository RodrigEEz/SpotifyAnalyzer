from sqlalchemy import insert, create_engine, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError

def insert_user(email,access_token, refresh_token, table_name, user, password, host, db):
   """Inserts Spotify user email, access token and refresh token for future data extraction"""

   engine = create_engine(f'postgresql://{user}:{password}@{host}:5432/{db}')

   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = metadata.tables[table_name]

   try:
      with engine.connect() as conn:
         stmt = insert(table).values(email=email, refresh_token=refresh_token, access_token=access_token)
         conn.execute(stmt)
         conn.commit()
         return 'Successfully saved user'
   except SQLAlchemyError as e:
      return f'database error: {e}'