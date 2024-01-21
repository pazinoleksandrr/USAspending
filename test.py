import psycopg2
import os
import asyncio
import aiofiles

# Database connection parameters
db_params = {
    'dbname': 'test',
    'user': 'postgres',
    'password': 'ejewr8987mbxRGO0HJGX',
    'host': 'localhost',
    'port': '5432'
}

class TxProcessor():
    def __init__(self):
        self.conn = psycopg2.connect(**db_params)
        print('Initialized')

    def start(self):
        asyncio.run(self.read_folders())

    async def read_folders(self):
        files = os.listdir(os.path.join(os.getcwd(), 'transactions'))
        tasks = [self.read_file(os.path.join(os.getcwd(), 'transactions', file)) for file in files]
        await asyncio.gather(*tasks)

    async def read_file(self, filepath):
        filename = os.path.basename(filepath).split('.')[0]  # Extract filename without extension
        table_name = f"transactions_{filename}"  # Create a sanitized table name

        column_names = [f"col{i}" for i in range(1, 371)]
        column_definitions = ", ".join([f"{name} TEXT" for name in column_names])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {column_definitions})"

        with self.conn.cursor() as cursor:
            try:
                # Drop the existing table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.commit()

                # Create the new table
                cursor.execute(create_table_query)
                self.conn.commit()

                # Read and insert data
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as file:
                    content = await file.readlines()
                    for line in content:
                        columns = line.split('\t')
                        # Ensure there are 370 columns, pad with NULLs if necessary
                        columns += [None] * (370 - len(columns))
                        insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * 370)})"
                        cursor.execute(insert_query, tuple(columns[:370]))
                    # Commit after processing each file
                    self.conn.commit()
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                self.conn.rollback()

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    txProcessor = TxProcessor()
    txProcessor.start()
    txProcessor.close()  # Close the connection when done
