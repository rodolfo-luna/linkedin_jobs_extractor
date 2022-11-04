from linkedin_api import Linkedin
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

sqlite_table = "linkedin_jobs"

# Use your Linkedin account to authenticate.
api = Linkedin('login', 'password')

print('Extracting...')
jobs = api.search_jobs(keywords='java developer', location_name='Portugal')

jobs_df = pd.DataFrame()
for job in jobs:
    jobs_df = pd.concat([jobs_df, pd.DataFrame([job])])

jobs_df = jobs_df[['title', 'applyMethod', 'new', 'formattedLocation']]
jobs_df['extracted_on'] = now
jobs_df['applyMethod'] = jobs_df['applyMethod'].apply(lambda x: x.get('companyApplyUrl'))

engine = create_engine('sqlite:///jobs.db', echo=True)
sqlite_connection = engine.connect()

jobs_df.to_sql(sqlite_table, sqlite_connection, if_exists='append', index=False)
print('-----------')
print('Data saved!')
print('-----------')

sqlite_connection.close()
