import pandas as pd
import logging
import wget
import os


class MeasurementReader():
    def __init__(self, measurement='dew_point'):
        self.measurement = measurement
        self.dl_path = f'dl_{measurement}.csv'
        self.col_name = {
            'pressure_qnh': 'Pressure hPa',
            'pressure_qfe': 'Pressure hPa',
            'temperature': 'Temperature °C',
            'wind': 'Wind direction °',
            'sunshine': 'Sunshine min',
            'dew_point': 'Dew point °C',
        }[measurement]
        self.url = {
            'pressure_qnh': 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-luftdruck-qnh-10min/ch.meteoschweiz.messwerte-luftdruck-qnh-10min_en.csv',
            'pressure_qfe': 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-luftdruck-qfe-10min/ch.meteoschweiz.messwerte-luftdruck-qfe-10min_en.csv',
            'temperature': 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-lufttemperatur-10min/ch.meteoschweiz.messwerte-lufttemperatur-10min_en.csv',
            'wind': 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-windgeschwindigkeit-kmh-10min/ch.meteoschweiz.messwerte-windgeschwindigkeit-kmh-10min_en.csv',
            'sunshine': 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-sonnenscheindauer-10min/ch.meteoschweiz.messwerte-sonnenscheindauer-10min_en.csv',
            'dew_point': 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-taupunkt-10min/ch.meteoschweiz.messwerte-taupunkt-10min_en.csv',
        }[measurement]
        self.db_dir = './db/'
        self.db_file = f'{self.db_dir}/{measurement}.csv'

    def read_last_csv(self):
        '''Read the last CSV that was downloaded, filter some columns out,
        rename measurement column to its date

        output:
            df (DataFrame): Last csv downloaded
        '''
        # Open csv
        df = pd.read_csv(self.dl_path, encoding='latin1',
                         on_bad_lines='skip', sep=';')

        # Drop many columns and last lines
        df = df[['Station', self.col_name, 'Measurement date']]
        df = df[:-5]

        # Drop measurement that don't match the expected date
        dates_in_df = df['Measurement date'].value_counts(ascending=False)
        good_date = dates_in_df.index[0]
        df = df[df['Measurement date'] == good_date]
        df = df.drop('Measurement date', axis=1)

        # Rename column with Measurement to the current date
        df = df.rename({self.col_name: good_date}, axis=1)

        # Set Station as index
        df = df.set_index('Station')

        return df

    def append_to_db(self, df):
        '''Append <df> to a csv database. The database has cities as columns and measurements as row
        (eg: "       ,Paris ,London ,Bern"
             "12/11 ,13     ,14     ,15")
        '''
        # Maybe create dir
        if not os.path.isdir(self.db_dir):
            os.mkdir(self.db_dir)

        # Maybe create db_file
        if not os.path.isfile(self.db_file):
            df.reset_index().T.to_csv(self.db_file, header=False)

        # If DB already exists, merge df and db_file
        else:
            # Read DB
            db = pd.read_csv(self.db_file, index_col=False).T
            db.columns = db.iloc[0]
            db = db.drop(db.index[0])
            db.index.name = 'Station'

            # Merge DB and the new df
            db = pd.concat([db, df], axis=1)

            # Save DB
            db.reset_index().T.to_csv(self.db_file, header=False)

    def update_db(self):
        '''Download new readings and merge them into the DB
        '''
        self.download_new_readings()
        df = self.read_last_csv()
        self.append_to_db(df)
        self.dl_cleanup()

    def download_new_readings(self):
        '''Download the new readings from swiss weather
        '''
        # Maybe delete dl_file
        self.dl_cleanup()

        # Download the file
        logging.info(f"Downloading {self.url} to {self.dl_path}")
        wget.download(self.url, out=self.dl_path)

    def dl_cleanup(self):
        # Maybe clean dl_file
        if os.path.isfile(self.dl_path):
            os.remove(self.dl_path)


MeasurementReader('dew_point').update_db()
MeasurementReader('pressure_qnh').update_db()
MeasurementReader('pressure_qfe').update_db()
MeasurementReader('temperature').update_db()
MeasurementReader('wind').update_db()
MeasurementReader('sunshine').update_db()
