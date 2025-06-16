import os
import tempfile
import snowflake.connector
from contextlib import contextmanager


@contextmanager
def get_snowflake_connection():
    """
    Context manager that yields a Snowflake connection using private key authentication.
    Automatically handles cleanup of temporary key file and connection.
    """
    private_key_pem = f"-----BEGIN ENCRYPTED PRIVATE KEY-----\n{os.environ.get('SNOWFLAKE_PRIVATE_KEY')}\n-----END ENCRYPTED PRIVATE KEY-----"
    
    key_file_path = None
    conn = None
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as key_file:
            key_file_path = key_file.name
            key_file.write(private_key_pem)
        
        conn = snowflake.connector.connect(
            account="YNDSYIO-SAVANTAUK",
            user="AZURE_CONNECTOR",
            warehouse="WAREHOUSE_XSMALL",
            database="BRANDVUEMETA_TEST",
            role="SYSADMIN",
            private_key_file=key_file_path,
            private_key_file_pwd=os.environ.get('SNOWFLAKE_KEY_ENCRYPTION_PASSWORD')
        )
        
        yield conn
        
    finally:
        if conn:
            conn.close()
        if key_file_path and os.path.exists(key_file_path):
            os.unlink(key_file_path)
