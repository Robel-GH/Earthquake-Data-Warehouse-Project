#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import zipfile
import requests
import certifi
import pandas as pd
import geopandas as gpd
from datetime import datetime
import calendar
from io import StringIO
import re
from shapely.geometry import Point
from requests.exceptions import SSLError

class EarthquakeDataPipeline:
    def __init__(self):
        # Configuration
        self.years = [2020, 2021, 2022, 2023, 2024, 2025]
        self.min_magnitude = 0
        self.base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query.csv"
        
        # File paths
        self.raw_data_file = "earthquakes[2020-2025].csv"
        self.transformed_file = "Earthquake[2020-2025]-Transformed.csv"
        self.county_file = "Earthquake_with_Counties.csv"
        self.final_file = "Earthquake[2020-2025]USA.csv"
        
        # Shapefile configuration
        self.shape_zip_url = "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_20m.zip"
        self.shape_zip_local = "cb_2022_us_county_20m.zip"
        self.shape_dir = "cb_2022_us_county_20m"
        
        # US regions mapping
        self.us_regions = {
            'Northeast': [
                'Connecticut', 'Massachusetts', 'Maine', 'New Hampshire',
                'Rhode Island', 'Vermont', 'New Jersey', 'New York', 'Pennsylvania'
            ],
            'Midwest': [
                'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Michigan', 'Minnesota',
                'Missouri', 'Nebraska', 'North Dakota', 'Ohio', 'South Dakota', 'Wisconsin'
            ],
            'South': [
                'Alabama', 'Arkansas', 'Delaware', 'Florida', 'Georgia', 'Kentucky',
                'Louisiana', 'Maryland', 'Mississippi', 'North Carolina', 'Oklahoma',
                'South Carolina', 'Tennessee', 'Texas', 'Virginia', 'West Virginia'
            ],
            'West': [
                'Alaska', 'Arizona', 'California', 'Colorado', 'Hawaii', 'Idaho',
                'Montana', 'Nevada', 'New Mexico', 'Oregon', 'Utah', 'Washington', 'Wyoming'
            ]
        }
        
        # State mapping dictionary
        self.state_mapping = {
            'CA': 'California', 'NV': 'Nevada', 'OR': 'Oregon', 'AK': 'Alaska',
            'AZ': 'Arizona', 'AR': 'Arkansas', 'CO': 'Colorado', 'CT': 'Connecticut',
            'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii',
            'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine',
            'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
            'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }

    def fetch_month_data(self, year, month):
        """Fetch earthquake data for a specific month and year"""
        start_date = f"{year}-{month:02d}-01"
        last_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{last_day}"
        
        params = {
            "format": "csv",
            "starttime": start_date,
            "endtime": end_date,
            "minmagnitude": self.min_magnitude,
            "orderby": "time"
        }
        
        print(f"Fetching data for {start_date} to {end_date}...")
        response = requests.get(self.base_url, params=params)

        if response.status_code == 200:
            csv_content = response.content.decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
            print(f"✅ Retrieved {len(df)} records.")
            return df
        else:
            print(f"❌ Failed to fetch data for {year}-{month:02d}. Status code: {response.status_code}")
            return pd.DataFrame()

    def load_earthquake_data(self):
        """Step 1: Load earthquake data from USGS"""
        print("=" * 60)
        print("STEP 1: LOADING EARTHQUAKE DATA FROM USGS")
        print("=" * 60)
        
        all_data = []
        
        # Loop through years and months
        for year in self.years:
            for month in range(1, 13):
                df_month = self.fetch_month_data(year, month)
                if not df_month.empty:
                    all_data.append(df_month)

        # Combine all monthly data into one DataFrame
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(self.raw_data_file, index=False)

        print(f"\n🎉 All data saved to '{self.raw_data_file}' — Total records: {len(combined_df)}")
        return combined_df

    def parse_place(self, txt):
        """Parse the place field to extract components"""
        pattern = re.compile(
            r"""
            (?:(?P<distance>\d+\s*km)\s*)?     # optional "3 km"
            (?:(?P<direction>[NSEW]{1,3})\s*   # optional "N" or "ENE"
              of\s*)?
            (?P<nearest>[^,]+?)                # nearest place (up to comma or end)
            (?:,\s*(?P<state>[A-Za-z ]+))?     # optional ", CA" or ", Montana"
            $""",
            re.IGNORECASE | re.VERBOSE
        )
        
        m = pattern.match(txt.strip())
        if not m:
            return {"distance": None, "direction": None, "nearest": txt, "state": None}
        return {
            "distance": m.group("distance"),
            "direction": m.group("direction"),
            "nearest": m.group("nearest").strip(),
            "state": m.group("state")
        }

    def expand_state_name(self, state):
        """Expand state abbreviations to full names"""
        if pd.isna(state):
            return state
        state_clean = state.strip()
        return self.state_mapping.get(state_clean, state_clean)

    def get_country_continent(self, state):
        """Map state to country and continent"""
        if pd.isna(state):
            return 'Unknown', 'Unknown'
        
        state_clean = state.strip()
        
        # US States and Territories
        us_states = {
            'California', 'Alaska', 'Hawaii', 'Texas', 'Washington', 'Montana', 
            'New Mexico', 'Utah', 'Idaho', 'Oregon', 'Wyoming', 'Colorado', 
            'New Jersey', 'Tennessee', 'Missouri', 'Arkansas', 'Arizona', 
            'Kansas', 'Georgia', 'Maine', 'South Carolina', 'Nevada', 
            'North Carolina', 'New York', 'Louisiana', 'Connecticut', 
            'Illinois', 'Oklahoma', 'Kentucky', 'New Hampshire', 'Virginia', 
            'Nebraska', 'South Dakota', 'Minnesota', 'Alabama', 'Massachusetts', 
            'Pennsylvania', 'Maryland', 'West Virginia', 'Mississippi', 
            'Wisconsin', 'Florida', 'Indiana'
        }
        
        us_territories = {
            'Puerto Rico', 'Guam', 'Northern Mariana Islands', 'American Samoa'
        }
        
        if state_clean in us_states or state_clean in us_territories:
            return 'United States', 'North America'
        
        return 'Unknown', 'Unknown'

    def clean_and_transform_data(self, df=None):
        """Step 2: Clean and transform the earthquake data"""
        print("=" * 60)
        print("STEP 2: CLEANING AND TRANSFORMING DATA")
        print("=" * 60)
        
        if df is None:
            df = pd.read_csv(self.raw_data_file)

        print(f"Before Cleaning: {df.shape}")
        
        # Remove rows with missing values
        df = df.dropna()
        print(f"After Cleaning: {df.shape}")

        # Parse place field
        parsed = df["place"].apply(self.parse_place).apply(pd.Series)
        df = pd.concat([df, parsed], axis=1)
        
        print(f"After Transformation Before Dropping: {df.shape}")
        df = df.dropna()
        print(f"After Transformation After Dropping: {df.shape}")

        # Expand state abbreviations
        df['state'] = df['state'].apply(self.expand_state_name)

        # Add country and continent
        df[['country', 'continent']] = df['state'].apply(
            lambda x: pd.Series(self.get_country_continent(x))
        )

        # Rename columns
        df = df.rename(columns={
            'id': 'earthquake_id',
            'direction': 'offset_direction',
            'distance': 'offset_distance',
            'nearest': 'nearest_locality'
        })

        # Drop unnecessary columns
        df = df.drop('place', axis=1)

        # Filter for US data only
        df = df[df['continent'] != 'Unknown']
        df = df[df['country'] == 'United States']

        # Save transformed data
        df.to_csv(self.transformed_file, index=False)
        
        print(f"\nFinal shape after US filtering: {df.shape}")
        print(f"Data saved to {self.transformed_file}")
        
        return df

    def download_shapefile(self, url, local_path):
        """Download county shapefile ZIP"""
        try:
            print("Downloading with certifi-verified SSL…")
            resp = requests.get(url, verify=certifi.where(), timeout=30)
            resp.raise_for_status()
        except (SSLError, requests.exceptions.SSLError):
            print("SSL verification failed. Retrying with verify=False…")
            resp = requests.get(url, verify=False, timeout=30)
            resp.raise_for_status()
        
        with open(local_path, "wb") as f:
            f.write(resp.content)
        print(f"Saved ZIP to {local_path}")

    def extract_zip(self, zip_path, extract_to):
        """Extract ZIP file"""
        print("Extracting shapefile archive…")
        os.makedirs(extract_to, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_to)
        print(f"Extracted into folder: {extract_to}")

    def add_county_data(self, df=None):
        """Step 3: Add county information using spatial join"""
        print("=" * 60)
        print("STEP 3: ADDING COUNTY DATA")
        print("=" * 60)
        
        if df is None:
            df = pd.read_csv(self.transformed_file)

        # Download and extract shapefile if not exists
        if not os.path.exists(self.shape_zip_local):
            self.download_shapefile(self.shape_zip_url, self.shape_zip_local)
        
        if not os.path.exists(self.shape_dir):
            self.extract_zip(self.shape_zip_local, self.shape_dir)

        print("Loading earthquake CSV…")
        if 'latitude' not in df or 'longitude' not in df:
            raise KeyError("CSV must have 'latitude' and 'longitude' columns")
        
        print("Converting to GeoDataFrame…")
        geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
        gdf_eq = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        
        shp_file = os.path.join(self.shape_dir, "cb_2022_us_county_20m.shp")
        print(f"Loading county shapefile from {shp_file}…")
        gdf_counties = gpd.read_file(shp_file).to_crs("EPSG:4326")
        
        print("Performing spatial join…")
        joined = gpd.sjoin(
            gdf_eq, 
            gdf_counties[["NAME", "geometry"]], 
            how="left", 
            predicate="intersects"
        ).rename(columns={"NAME": "county"})
        
        # Convert back to regular DataFrame
        df_with_counties = joined.drop(columns="geometry", errors="ignore")
        
        print(f"Writing output CSV to {self.county_file}…")
        df_with_counties.to_csv(self.county_file, index=False)
        print("County data added successfully!")
        
        return df_with_counties

    def add_regions_and_finalize(self, df=None):
        """Step 4: Add US regions and finalize the dataset"""
        print("=" * 60)
        print("STEP 4: ADDING REGIONS AND FINALIZING")
        print("=" * 60)
        
        if df is None:
            df = pd.read_csv(self.county_file)

        print(f"Data Shape Before Dropping: {df.shape}")
        df = df.dropna()
        print(f"Data Shape After Dropping: {df.shape}")

        # Create reverse mapping (state -> region)
        state_to_region = {}
        for region, states in self.us_regions.items():
            for state in states:
                state_to_region[state] = region
        
        # Add region column
        df['region'] = df['state'].map(state_to_region)
        df['region'] = df['region'].fillna('Non-US')
        
        # Clean up columns
        df = df.drop(['continent', 'country', 'index_right'], axis=1, errors='ignore')
        
        # Filter for US regions only
        df = df[df['region'] != 'Non-US']
        
        # Clean offset_distance column
        df['offset_distance'] = (
            df['offset_distance']
              .str.replace(r'\s*km', '', regex=True)
              .astype(float)
        )
        
        # Save final dataset
        df.to_csv(self.final_file, index=False)
        
        print(f"\nFinal dataset saved to {self.final_file}")
        print(f"Final shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Print summary statistics
        print("\nRegion counts:")
        region_counts = df['region'].value_counts()
        print(region_counts)
        
        print(f"\nTop 10 states by earthquake count:")
        state_counts = df['state'].value_counts().head(10)
        print(state_counts)
        
        print(f"\nTop 10 counties by earthquake count:")
        county_counts = df['county'].value_counts().head(10)
        print(county_counts)
        
        return df

    def run_pipeline(self):
        """Run the complete earthquake data processing pipeline"""
        print("🌍 EARTHQUAKE DATA PROCESSING PIPELINE STARTED")
        print("=" * 60)
        
        try:
            # Step 1: Load raw earthquake data
            df = self.load_earthquake_data()
            
            # Step 2: Clean and transform data
            df = self.clean_and_transform_data(df)
            
            # Step 3: Add county information
            df = self.add_county_data(df)
            
            # Step 4: Add regions and finalize
            df = self.add_regions_and_finalize(df)
            
            print("\n" + "=" * 60)
            print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"Final dataset: {self.final_file}")
            print(f"Total earthquakes processed: {len(df):,}")
            
            return df
            
        except Exception as e:
            print(f"\n❌ Pipeline failed with error: {str(e)}")
            raise


def main():
    """Main function to run the earthquake data pipeline"""
    pipeline = EarthquakeDataPipeline()
    final_df = pipeline.run_pipeline()
    return final_df


if __name__ == "__main__":
    main()