

import pandas as pd

df=pd.read_csv('customer_shopping_behavior.csv')


df.head()


df.info()


df.describe()

df.describe(include='all')


df.isnull().sum()


df['Review Rating']=df.groupby('Category')['Review Rating'].transform(lambda x:x.fillna(x.median()))


df.isnull().sum()


df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(' ','_')
df=df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})

df.columns


# create a column age group
labels=['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group']=pd.qcut(df['age'], q=4, labels=labels)


df[['age','age_group']].head(10)


#create a column purchase_frequency_days

frequency_mapping={
    'Fortnightly':14,
    'Weekly':7,
    'Monthly':30,
    'Quarterly':90,
    'Bi-Weekly':14,
    'Annually':365,
    'Every 3 Months':90

}
df['purchase_frequency_days']=df['frequency_of_purchases'].map(frequency_mapping)

df[['purchase_frequency_days','frequency_of_purchases']].head(10)


df[['discount_applied','promo_code_used']].head(10)


(df['discount_applied']==df['promo_code_used']).all()


df=df.drop('promo_code_used', axis=1)


df.columns


pip install pandas sqlalchemy pyodbc


from sqlalchemy import create_engine

# --- CONNECTION PARAMETERS ---
SERVER = 'DESKTOP-N7A1ETL'
DATABASE = 'customer_behaviour'
DRIVER = 'ODBC Driver 17 for SQL Server'  # Check your installed driver
TABLE_NAME = 'Cleaned_Customer_Behaviour_Table'

# --- CONNECTION STRING (using Windows Authentication) ---
# Note: The driver name is URL-encoded by replacing spaces with '+'
connection_string = (
    f'mssql+pyodbc://{SERVER}/{DATABASE}?driver={DRIVER.replace(" ", "+")}'
)

# Create the SQLAlchemy engine
try:
    engine = create_engine(connection_string)

    # Write the CLEANED DataFrame to SQL Server
    df.to_sql(
        name=TABLE_NAME,
        con=engine,
        if_exists='replace',  # Options: 'fail', 'replace', 'append'
        index=False           # Don't write the pandas index as a column
    )
    print(f"\n✅ Successfully imported {len(df)} cleaned rows into MS SQL table '{TABLE_NAME}'.")

except Exception as e:
    print(f"\n❌ An error occurred during database connection or import: {e}")

# %%
df.head()

# %%



