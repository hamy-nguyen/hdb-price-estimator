from sqlalchemy import create_engine, Integer
import pandas as pd

def clean_onemap():

    def clean_transport_school():
        print("Cleaning transport to school data...")

        engine_hdb = create_engine('mysql://airflow_user:password@localhost:3306/HDB_Data')

        str_sql = f'''
        SELECT * FROM raw_onemap_transport_school
        '''

        df = pd.read_sql(sql=str_sql, con=engine_hdb)

        # fill NAs as 0
        df = df.fillna(0)

        transport_school_cols = [
            'bus', 'mrt', 'mrt_bus', 'mrt_car',
            'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
            'motorcycle_scooter', 'others', 'no_transport_required',
            'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
            'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
            'pvt_chartered_bus_van'
        ]

        dtype_dict = {col: Integer() for col in transport_school_cols}

        df.to_sql(
            'clean_onemap_transport_school',
            con=engine_hdb,
            if_exists='replace',
            index=False,
            dtype=dtype_dict
        )

        engine_hdb.dispose()
        
        print("Cleaned transport to school data saved to clean_onemap_transport_school.")
    
    def clean_transport_work():
        print("Cleaning transport to work data...")

        engine_hdb = create_engine('mysql://airflow_user:password@localhost:3306/HDB_Data')

        str_sql = f'''
        SELECT * FROM raw_onemap_transport_work
        '''
        
        df = pd.read_sql(sql=str_sql, con=engine_hdb)

        df = df.fillna(0)

        transport_work_cols = [
            'bus', 'mrt', 'mrt_bus', 'mrt_car',
            'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
            'motorcycle_scooter', 'others', 'no_transport_required',
            'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
            'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
            'pvt_chartered_bus_van'
        ]

        dtype_dict = {col: Integer() for col in transport_work_cols}

        df.to_sql(
            'clean_onemap_transport_work',
            con=engine_hdb,
            if_exists='replace',
            index=False,
            dtype=dtype_dict
        )

        engine_hdb.dispose()
        
        print("Cleaned transport to work data saved to clean_onemap_transport_work.")

    def clean_tenant():
        print("Cleaning tenancy data...")

        engine_hdb = create_engine('mysql://airflow_user:password@localhost:3306/HDB_Data')

        str_sql = f'''
        SELECT * FROM raw_onemap_tenancy
        '''

        df = pd.read_sql(sql=str_sql, con=engine_hdb)

        df = df.fillna(0)
        
        tenant_cols = ['owner', 'tenant', 'others']

        dtype_dict = {col: Integer() for col in tenant_cols}

        df.to_sql(
            'clean_onemap_tenancy',
            con=engine_hdb,
            if_exists='replace',
            index=False,
            dtype=dtype_dict
        )

        engine_hdb.dispose()
        
        print("Cleaned tenancy data saved to clean_onemap_tenancy.")
    
    def clean_dwelling():
        print("Cleaning dwelling data...")

        engine_hdb = create_engine('mysql://airflow_user:password@localhost:3306/HDB_Data')

        str_sql = f'''
        SELECT * FROM raw_onemap_dwelling
        '''

        df = pd.read_sql(sql=str_sql, con=engine_hdb)

        dwelling_cols = [
            'hdb_1_and_2_room_flats',
            'hdb_3_room_flats',
            'hdb_4_room_flats',
            'hdb_5_room_and_executive_flats',
            'condominiums_and_other_apartments',
            'landed_properties',
            'others'
        ]

        dtype_dict = {col: Integer() for col in dwelling_cols}

        df.to_sql(
            'clean_onemap_dwelling',
            con=engine_hdb,
            if_exists='replace',
            index=False,
            dtype=dtype_dict
        )

        engine_hdb.dispose()
        
        print("Cleaned dwelling data saved to clean_onemap_dwelling.")

    clean_transport_school()
    clean_transport_work()
    clean_tenant()
    clean_dwelling()

if __name__ == "__main__":
    clean_onemap()