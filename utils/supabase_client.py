import os
import streamlit as st
from supabase import create_client, Client


class SupabaseClient:
    """Class to manage Supabase client connection"""

    def __init__(self, supabase_url: str, supabase_key: str):
        """Initialize Supabase client connection with provided URL and key."""
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase URL and Key must be provided.")
        self.url = supabase_url
        self.key = supabase_key
        
        # Initialize client
        self.client = create_client(self.url, self.key)

    # _initialize method is effectively merged into __init__ and no longer needed separately.
    
    def get_client(self) -> Client:
        """Get the Supabase client instance"""
        return self.client

    def query(self, query_str: str, values: dict = None):
        """Execute a SQL query with optional parameters using RPC
        
        IMPORTANT: This method is being used in SQL_Chatbot.py to run SQL queries, but it needs
        to call the 'execute_sql' RPC function, not treat the query_str as an RPC function name.
        """
        # Call our execute_sql method which properly handles the RPC call
        return self.execute_sql(query_str)
    
    def execute_sql(self, sql_query: str):
        """Execute raw SQL against Supabase database using the Supabase RPC function
        
        This method executes the SQL query directly using a custom RPC function and returns
        the results in a format compatible with the application.
        
        Args:
            sql_query: The SQL query to execute
            
        Returns:
            The query results or error information in a format that can be converted to DataFrame
        """
        # Eliminar espacios en blanco y punto y coma al final de la consulta SQL
        # Esto previene el error 'syntax error at or near ";"
        sql_query = sql_query.strip()
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]
            
        print(f"\n=== Executing SQL: ===\n{sql_query}\n==================")
        
        # Define a standard result structure for consistency
        from collections import namedtuple
        ResultContainer = namedtuple('ResultContainer', ['data'])
        
        try:
            # Preparar la llamada a la función RPC de Supabase
            filter_builder = self.client.rpc('execute_sql', {'query': sql_query})
            
            # Ejecutar la llamada y obtener la respuesta
            try:
                # Esto ejecutará la consulta y obtendrá los resultados
                response = filter_builder.execute()
                
                # Manejo específico para respuestas exitosas
                if hasattr(response, 'data'):
                    # Si hay un mensaje de error en los datos
                    if isinstance(response.data, dict) and 'error' in response.data:
                        error_msg = response.data.get('error', 'Unknown error')
                        detail = response.data.get('detail', None)
                        print(f"SQL Error: {error_msg}")
                        if detail:
                            print(f"Detail: {detail}")
                        return ResultContainer(data=[{"error": error_msg, "detail": detail}])
                    
                    # Si hay datos válidos en la respuesta
                    if isinstance(response.data, list) and len(response.data) == 0:
                        # Lista vacía - no hay resultados pero la consulta fue exitosa
                        return ResultContainer(data=[{"message": "La consulta se ejecutó exitosamente pero no retornó resultados"}])
                    else:
                        # Tenemos datos reales, devolverlos tal cual
                        return response
                    
                else:
                    # Si la respuesta no tiene atributo 'data'
                    print("Respuesta sin datos: respuesta no tiene atributo 'data'")
                    return ResultContainer(data=[{"message": "La consulta se ejecutó pero no retornó resultados"}])
                
            except Exception as exec_error:
                # Error al ejecutar la consulta
                error_str = str(exec_error)
                print(f"Error ejecutando la consulta: {error_str}")
                
                # Si el error contiene un mensaje que indica éxito pero sin datos
                if "'message': 'Query executed successfully'" in error_str:
                    return ResultContainer(data=[{"message": "La consulta se ejecutó exitosamente pero no retornó resultados"}])
                
                # Caso contrario, es un error real que debemos manejar
                return ResultContainer(data=[{"error": error_str, "query": sql_query}])
        
        except Exception as e:
            # Error general al preparar o ejecutar la consulta
            print(f"Error general ejecutando SQL: {str(e)}")
            return ResultContainer(data=[{"error": str(e), "query": sql_query}])
    
    def get_table_data(self, table_name: str, columns: list = None, default_columns: list = None, filters: dict = None, limit: int = 250000, batch_size: int = 40000):
            """Get data from a specific table with optional filters using pagination
            
            Args:
                table_name: The name of the table to query
                columns: Specific columns to retrieve (None for all or default)
                default_columns: Default columns to use if columns is None
                filters: Dict of column:value pairs to filter results
                limit: Maximum number of rows to return (default 250,000)
                batch_size: Number of records to fetch in each batch (default 40,000)
                
            Returns:
                pandas.DataFrame: The query results as a DataFrame
            """
            import pandas as pd
            import streamlit as st
            import time
            
            try:
                # Print debug info
                print(f"Accessing table: {table_name}")
                
                # Determine which columns to select
                if columns is not None:
                    select_str = ",".join([f'"{col}"' for col in columns])
                elif default_columns is not None:
                    select_str = ",".join([f'"{col}"' for col in default_columns])
                else:
                    select_str = "*"  # Select all columns if no specific columns provided
                
                # First, get the exact row count to optimize batch processing
                try:
                    # Build base query for counting
                    count_query = self.client.table(table_name).select("*", count="exact")
                    
                    # Apply filters if provided
                    if filters:
                        for column, value in filters.items():
                            count_query = count_query.eq(column, value)
                            
                    # Execute count query with limit=1 to minimize data transfer
                    count_result = count_query.limit(1).execute()
                    total_rows = count_result.count
                    
                    print(f"Total rows in {table_name}: {total_rows}")
                    
                    # Adjust limit to not exceed the actual row count
                    actual_limit = min(limit, total_rows) if total_rows > 0 else limit
                except Exception as e:
                    print(f"Error getting row count: {e}. Using provided limit.")
                    actual_limit = limit
                    total_rows = None
                
                # Initialize an empty DataFrame to store all results
                all_results = pd.DataFrame()
                
                # If we know there are no rows, return empty DataFrame immediately
                if total_rows == 0:
                    print(f"Table {table_name} is empty or all rows filtered out")
                    return all_results
                
                # Calculate the number of batches needed based on actual limit
                num_batches = (actual_limit + batch_size - 1) // batch_size  # Ceiling division
                
                # Fetch data in batches
                for batch in range(num_batches):
                    offset = batch * batch_size
                    current_batch_size = min(batch_size, actual_limit - offset)
                    
                    if current_batch_size <= 0:
                        break  # We've reached the limit
                    
                    # Progress message
                    print(f"Fetching batch {batch + 1}/{num_batches}: offset={offset}, limit={current_batch_size}")
                    
                    # Implement retry logic for each batch
                    max_retries = 3
                    current_try = 0
                    
                    while current_try < max_retries:
                        try:
                            # Build query
                            query = self.client.table(table_name).select(select_str)
                            
                            # Apply filters if provided
                            if filters:
                                for column, value in filters.items():
                                    query = query.eq(column, value)
                            
                            # Apply pagination
                            result = query.range(offset, offset + current_batch_size - 1).execute()
                            
                            # Process results
                            if hasattr(result, 'data') and result.data:
                                # Append this batch to our results
                                batch_df = pd.DataFrame(result.data)
                                all_results = pd.concat([all_results, batch_df], ignore_index=True)
                                
                                # If we got fewer results than requested, we've reached the end
                                if len(result.data) < current_batch_size:
                                    print(f"Reached end of data at {len(all_results)} records")
                                    break
                            else:
                                # No data in this batch
                                if batch == 0:
                                    # If first batch is empty, table is empty
                                    return pd.DataFrame()
                                # Otherwise, we've reached the end of data
                                break
                                
                            # Successful batch, break the retry loop
                            break
                                
                        except Exception as e:
                            error_str = str(e)
                            current_try += 1
                            
                            # Handle timeout errors specifically
                            if '57014' in error_str or 'timeout' in error_str.lower():
                                print(f"Timeout error on batch {batch + 1}, attempt {current_try}/{max_retries}")
                                
                                # If we have retries left, wait and try again with a smaller batch
                                if current_try < max_retries:
                                    # Exponential backoff
                                    wait_time = 2 ** current_try  # 2, 4, 8 seconds...
                                    print(f"Waiting {wait_time} seconds before retry...")
                                    time.sleep(wait_time)
                                    
                                    # Reduce batch size for retry
                                    current_batch_size = current_batch_size // 2
                                    if current_batch_size < 1000:
                                        current_batch_size = 1000  # Minimum batch size
                                    
                                    print(f"Reducing batch size to {current_batch_size} for retry")
                                    continue
                            
                            # For other errors or if we're out of retries
                            if current_try >= max_retries:
                                print(f"Failed after {max_retries} attempts on batch {batch + 1}: {error_str}")
                                # If we have some data, return what we have with a warning
                                if not all_results.empty:
                                    st.warning(f"Se obtuvieron {len(all_results)} registros antes de encontrar un error. Algunos datos pueden faltar.")
                                    return all_results
                                # Otherwise, propagate the error
                                raise
                    
                    # Add a small delay between batches to avoid overwhelming the server
                    if batch < num_batches - 1:
                        time.sleep(0.5)
                
                # Return the combined results
                print(f"Successfully retrieved {len(all_results)} records from {table_name}")
                return all_results
                
            except Exception as e:
                # Catch any other unexpected errors during the process
                error_msg = f"Unexpected error accessing Supabase: {e}"
                print(error_msg)
                
                # Mensaje más amigable para el usuario en caso de timeout
                if '57014' in str(e) or 'timeout' in str(e).lower():
                    st.error("La consulta a la base de datos está tomando demasiado tiempo. Estamos trabajando con una muestra reducida de datos.")
                else:
                    st.error(error_msg)
                    
                return pd.DataFrame()