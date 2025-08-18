from sqlalchemy import create_engine, URL, insert, MetaData
from sqlalchemy.exc import SQLAlchemyError

def save_user_info(email, access_token, refresh_token, username, password, host, port, database, table_name):
   """
   Save user information in the database.
    
   Args:
      email (str): User's email address.
      access_token (str): Access token for the user.
      refresh_token (str): Refresh token for the user.
   """
   url_object = URL.create(
      drivername="postgresql",
      username=username,
      password=password,
      host=host,
      port=port,
      database=database
   )
   engine = create_engine(url_object)

   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = metadata.tables[table_name]

   stmt = insert(table).values(email=email, access_token=access_token, refresh_token=refresh_token)

   try:
      with engine.connect() as conn:
         with conn.begin():
            conn.execute(stmt)
   except SQLAlchemyError as e:
      print(f"Error saving user info: {e}")
      return f"Error saving user info: {e}"

