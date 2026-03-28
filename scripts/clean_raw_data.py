from sqlalchemy import create_engine
import pandas as pd

def clean_onemap():

    def clean_transport_school(df):
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

        # convert transport_school_cols to numeric
        df[transport_school_cols] = df[transport_school_cols].apply(pd.to_numeric)

        print("Transport to school data after cleaning:")
        print(df)

        return df
    
    def clean_transport_work(df):
        df = df.fillna(0)

        transport_work_cols = [
            'bus', 'mrt', 'mrt_bus', 'mrt_car',
            'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
            'motorcycle_scooter', 'others', 'no_transport_required',
            'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
            'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
            'pvt_chartered_bus_van'
        ]

        df[transport_work_cols] = df[transport_work_cols].apply(pd.to_numeric)

        print("Transport to work data after cleaning:")
        print(df)

        return df

    def clean_tenant(df):
        df = df.fillna(0)
        
        tenant_cols = ['owner', 'tenant', 'others']

        df[tenant_cols] = df[tenant_cols].apply(pd.to_numeric)

        print("Tenancy data after cleaning:")
        print(df)

        return df
    
    def clean_dwelling(df):
        df = df.fillna(0)

        dwelling_cols = [
            'hdb_1_and_2_room_flats',
            'hdb_3_room_flats',
            'hdb_4_room_flats',
            'hdb_5_room_and_executive_flats',
            'condominiums_and_other_apartments',
            'landed_properties',
            'others'
        ]

        df[dwelling_cols] = df[dwelling_cols].apply(pd.to_numeric)

        print("Dwelling data after cleaning:")
        print(df)

        return df
        

    engine_hdb = create_engine('mysql://airflow_user:password@localhost:3306/HDB_Data')

    str_sql = f'''
    SELECT * FROM raw_onemap_transport_school
    '''
    df_transport_school = pd.read_sql(sql=str_sql, con=engine_hdb)

    clean_transport_school(df_transport_school)

    str_sql = f'''
    SELECT * FROM raw_onemap_transport_work
    '''
    df_transport_work = pd.read_sql(sql=str_sql, con=engine_hdb)

    clean_transport_work(df_transport_work)

    str_sql = f'''
    SELECT * FROM raw_onemap_dwelling
    '''
    df_dwelling = pd.read_sql(sql=str_sql, con=engine_hdb)

    clean_dwelling(df_dwelling)

    str_sql = f'''
    SELECT * FROM raw_onemap_tenancy
    '''
    df_tenancy = pd.read_sql(sql=str_sql, con=engine_hdb)

    clean_tenant(df_tenancy)

    engine_hdb.dispose()

if __name__ == "__main__":
    clean_onemap()