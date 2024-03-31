from bs4 import BeautifulSoup
import pandas as pd
pd.options.mode.chained_assignment = None # Cancel false positive warnings
import os
import numpy as np
import json
import re
import math

# For updating data
from EtlHelper import EtlHelper
etl_helper = EtlHelper()
import requests
import re
from pathlib import Path
import kaggle
from json import JSONDecodeError
import shutil
from tqdm import tqdm
tqdm.pandas()

class DataParser:
    def __init__(self):
        pass

    ### Amenities Data Transformation Functions
    def parse_kml(self, filename) -> pd.DataFrame:
        """For hawkercenters, supermarkets, kindergartens, gyms, park facilities, Retailpharmacylocations
        These have <Point> child of <Placemark> element.
        For MRTLine, <LineString> child with start and end
        For parking lot, coordinates of edges

        Args:
            filename (string): kml file name

        Returns:
            pd.DataFrame: kml data parsed into dataframe with longitude latitude, usually altitude contains all 0
        """
        with open(filename, 'r') as f:
            s = BeautifulSoup(f, 'xml')
            placemarks = s.find_all(str("Placemark"))

            data_dict = {}
            for placemark in placemarks:
                # extract placemark name or id
                placemark_id = placemark.find('name').text if placemark.find('name') else placemark['id']
                data_dict[placemark_id] = {}
                # extract table dataa
                simple_data = placemark.find_all('SimpleData')
                for sd in simple_data:    
                    data_dict[placemark_id].update({sd['name']: sd.text})                
                if placemark.Point:
                    coor = placemark.find('Point').coordinates.text.split(',', 2)
                    # https://developers.google.com/kml/documentation/kml_tut
                    longitude, latitude, altitude = map(float, coor)                    
                    data_dict[placemark_id].update({'Longitude': longitude, 'Latitude': latitude, 'Altitude': altitude})
                
                elif placemark.LineString:
                    # start, end = placemark.LineString.coordinates.text.split(' ')
                    # some has more than 2 for a line
                    coor = placemark.find('LineString').coordinates.text
                    data_dict[placemark_id].update({'coordinates': coor})

                elif placemark.Polygon:
                    coor = placemark.find('Polygon').outerBoundaryIs.LinearRing.coordinates.text
                    data_dict[placemark_id].update({'coordinates': coor})

            df = pd.DataFrame(data_dict).T.reset_index().rename(columns={'index': filename.split(".")[0]})
        print("Retrieve success: {filename}".format(filename = filename))
        return df

    def _clean_carpark(self, df):
        df = self._calculate_mid_pt(df, 'coordinates')
        df = df[['PP_CODE', 'PARKING_PL', 'Latitude', 'Longitude']].groupby(['PP_CODE', 'PARKING_PL'], as_index=False).mean()
        return df

    def _clean_park(self, df):
        target_class = ['FITNESS AREA', 'PLAYGROUND', 'LOOKOUT POINT', 'CAMPSITE', 'ALLOTMENT GARDEN', 'THERAPEUTIC GARDEN']
        target_class = '|'.join(target_class)
        df = df[df['CLASS'].str.contains(target_class, case = False)]
        return df

    def _get_data(self, amenity_name, file_path, amenity_file_type):
        kml = DataParser()
        if amenity_file_type == "csv":
            result_df = pd.read_csv(file_path)
        elif amenity_file_type == "kml":
            result_df = kml.parse_kml(file_path)
            if amenity_name == "Carpark":
                result_df = self._clean_carpark(result_df)
            elif amenity_name == "Park":
                result_df = self._clean_park(result_df)
        return result_df
    
    def _extract_data(self, url_dict, data_folder):
        combined_dict = {}
        kml = DataParser()
        for amenity_name, amenity_details in url_dict.items():
            amenity_file_path = os.path.join(data_folder, amenity_details['file_name'])
            amenity_file_type = amenity_details['file_name'].split('.')[-1]
            combined_dict[amenity_name] = self._get_data(amenity_name, amenity_file_path, amenity_file_type)
        return combined_dict
    
    def _rename_lat_long_cols(self, data_dict):
        data_dict['Mrt Station'] = data_dict['Mrt Station'].rename(columns={'lat':'Latitude', 'lng': 'Longitude'})
        for key, df in data_dict.items():
            df.rename(columns={"Latitude": "lat", "Longitude": "long"}, inplace=True)
        return data_dict

    def _rename_amenity_name_cols(self, amenity_dict):
        amenity_dict['Gym'].rename(columns={'NAME': 'amenity_name'}, inplace = True)
        amenity_dict['Hawker'].rename(columns={'NAME': 'amenity_name'}, inplace = True)
        amenity_dict['Kindergarten'].rename(columns={'NAME': 'amenity_name'}, inplace = True)
        amenity_dict['Mrt Station'].rename(columns={'station_name': 'amenity_name'}, inplace = True)
        amenity_dict['Park'].rename(columns={'NAME': 'amenity_name'}, inplace = True)
        amenity_dict['Pharmacy'].rename(columns={'PHARMACY_NAME': 'amenity_name'}, inplace = True)
        amenity_dict['Supermarket'].rename(columns={'LIC_NAME': 'amenity_name'}, inplace = True)
        amenity_dict['Carpark'].rename(columns={'PP_CODE': 'amenity_name'}, inplace = True)
        return amenity_dict

    def _get_district_name(self, amenity_dict, onemap_access_token):
        for amenity_name, amenity_df in amenity_dict.items():
            if not amenity_df.empty:
                amenity_dict[amenity_name]["district_name"] = amenity_df.progress_apply(lambda x: etl_helper.get_planning_area_name_from_lat_long(x.lat, x.long, onemap_access_token), axis=1)
            else:
                amenity_dict[amenity_name]["district_name"] = None
        return amenity_dict
    
    def _add_amenity_type(self, data_dict):
        for amenity_name, amenity_df in data_dict.items():
            # print("Amenity type added: {amenity_name}".format(amenity_name = amenity_name))
            data_dict[amenity_name]['amenity_type'] = amenity_name
        # Get MRT and LRT amenity type
        data_dict['Mrt Station'] = data_dict['Mrt Station'].drop(['amenity_type'], axis = 1).rename(columns = {'type': 'amenity_type'})
        return data_dict
    
    def _calculate_mid_pt(self, df, coords_col):
        def get_avg(lst):
            return sum(lst) / len(lst)
            
        coords_srs = df[coords_col]
        avg_lat_list = []
        avg_long_list = []
        avg_alt_list = []
        counter = 0
        for coords_str in coords_srs:
            try:
                coord_list = re.split(',| ', coords_str)
                coord_list = [float(value) for value in coord_list]
                lat_list = coord_list[1::3]
                long_list = coord_list[0::3]
                alt_list = coord_list[2::3]
        
                avg_lat_list.append(get_avg(lat_list))
                avg_long_list.append(get_avg(long_list))
                avg_alt_list.append(get_avg(alt_list))
                counter += 1
            except:
                avg_lat_list.append(pd.NA)
                avg_long_list.append(pd.NA)
                avg_alt_list.append(pd.NA)
        df['Latitude'] = avg_lat_list
        df['Longitude'] = avg_long_list
        df['Altitude'] = avg_alt_list
        df = df.drop(columns=coords_col)
        return df
    
    def _save_individual_df(self, data_dict, folder_path):
        for name, df in data_dict.items():
            file_path = "{folder_path}/{name}.csv".format(folder_path=folder_path, name=name)
            df.to_csv(file_path, index=False)
        print("Data saved in {file_path}".format(file_path=file_path))
    
    def _combine_dict_to_df(self, combined_dict, common_cols):
        combined_df = pd.DataFrame()
        df_list = []
        for key, df in combined_dict.items():
            if not df.empty:
                df_common_cols = df[common_cols]
                df_list.append(df_common_cols)
                combined_df = pd.concat(df_list, ignore_index=True)
        # combined_df = combined_df.reset_index().rename(columns={'index':'Amenity_id'})
        return combined_df
    
    def amenity_data_transformation_pipeline(self, url_dict, amenity_src_folder_path, out_folder_path, onemap_access_token):
        amenity_dict = self._extract_data(url_dict, amenity_src_folder_path)
        amenity_dict = self._add_amenity_type(amenity_dict)
        amenity_dict = self._rename_lat_long_cols(amenity_dict)
        amenity_dict = self._rename_amenity_name_cols(amenity_dict)
        amenity_dict = self._get_district_name(amenity_dict, onemap_access_token)
    
        # Save individual transformed amenities (optional)
        # self._save_individual_df(amenity_dict, out_folder_path)
    
        # Combine all amenities into one dataframe and save
        common_cols = ["amenity_name", "amenity_type", "lat", "long", "district_name"]
        combined_df = self._combine_dict_to_df(amenity_dict, common_cols)
        # TODO: remove
        combined_df = combined_df.head(10)
        combined_amenities_file_path = "{folder_path}/Combined_amenities.csv".format(folder_path=out_folder_path)
        combined_df.to_csv(combined_amenities_file_path, index = False)
        return combined_amenities_file_path

    ### UPDATE Amenity Database
    def download_amenity_files(self, output_folder, first_time = True):
        def download_only(amenity_file_name, amenity_file_type, amenity_url, output_folder):
            amenity_file_path = os.path.join(output_folder, amenity_file_name)
            if amenity_file_type == 'csv':
                print(f"DOWNLOADING FILE - {amenity_file_path}")
                kaggle.api.dataset_download_file(amenity_url, amenity_file_name, path = output_folder)
            elif amenity_file_type == 'kml':
                print(f"DOWNLOADING FILE (SIMULATE) - {amenity_file_path}")
            return None
            
        def replace_and_download(amenity_file_name, amenity_file_type, amenity_url, output_folder):
            amenity_file_path = os.path.join(output_folder, amenity_file_name)
            archive_amenity_file_path = amenity_file_path.split('.')[-2] + "_old." + amenity_file_type
            if Path(amenity_file_path).exists():
                archive_amenity_file_path = amenity_file_path.split('.')[-2] + "_old." + amenity_file_type
                print(f"Current Data set to old - {archive_amenity_file_path}")
                os.replace(amenity_file_path, archive_amenity_file_path)
            if amenity_file_type == 'csv':
                print(f"DOWNLOADING FILE - {amenity_file_path}")
                kaggle.api.dataset_download_file(amenity_url, amenity_file_name, path = output_folder)
            elif amenity_file_type == 'kml':
                print(f"DOWNLOADING FILE (SIMULATE) - {amenity_file_path}")
                shutil.copy2(archive_amenity_file_path, amenity_file_path) # SIMULATE DOWNLOAD
            return None
            
        url_dict = {
            'Mrt Station': {'file_name': 'mrt_lrt_data.csv',
                                 'url': 'yxlee245/singapore-train-station-coordinates'},
            'Gym': {'file_name': 'GymsSGKML.kml',
                   'url': ''},
            'Hawker': {'file_name': 'HawkerCentresKML.kml',
                       'url': ''},
            'Kindergarten': {'file_name': 'Kindergartens.kml',
                       'url': ''},
            'Park': {'file_name': 'ParkFacilitiesKML.kml',
                       'url': ''},
            'Carpark': {'file_name': 'URAParkingLotKML.kml',
                       'url': ''},
            'Pharmacy': {'file_name': 'RetailpharmacylocationsKML.kml',
                       'url': ''},
            'Supermarket': {'file_name': 'SupermarketsKML.kml',
                       'url': ''},
        }
        for amenity_name, file_details in url_dict.items():
            amenity_file_name = file_details['file_name']
            amenity_url = file_details['url']
            amenity_file_type = amenity_file_name.split('.')[-1]
            if first_time == True:
                download_only(amenity_file_name, amenity_file_type, amenity_url, output_folder)
            else:
                replace_and_download(amenity_file_name, amenity_file_type, amenity_url, output_folder)
        return url_dict
    
    def _get_differences(self, df1, df2):
        df1_outer = df1.merge(df2,indicator = True, how='left').loc[lambda x : x['_merge']!='both'].drop(columns=['_merge'])
        df2_outer = df2.merge(df1,indicator = True, how='left').loc[lambda x : x['_merge']!='both'].drop(columns=['_merge'])
        return df1_outer, df2_outer
        
    def remove_old_entries_pipeline(self, amenity_combined_df, old_entries_dict):
        for amenity_name, df_old_outer in old_entries_dict.items():
            if not df_old_outer.empty:
                amenity_combined_df = pd.merge(amenity_combined_df, df_old_outer, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
            else:
                print(f"No entries to be removed for {amenity_name}")
        print("Old amenities removed success!")
        return amenity_combined_df
    
    def _transform_new_entries(self, amenity_dict, out_folder_path, onemap_access_token):
        amenity_dict = self._add_amenity_type(amenity_dict)
        amenity_dict = self._rename_lat_long_cols(amenity_dict)
        amenity_dict = self._rename_amenity_name_cols(amenity_dict)
        amenity_dict = self._get_district_name(amenity_dict, onemap_access_token)
        
        # Combine all amenities into one dataframe and save
        common_cols = ["amenity_name", "amenity_type", "lat", "long", "district_name"]
        combined_df = self._combine_dict_to_df(amenity_dict, common_cols)
        file_path = "{folder_path}/Combined_amenities_to_add.csv".format(folder_path=out_folder_path)
        combined_df.to_csv(file_path, index = False)
        return file_path
    
    def add_new_entries(self, amenity_name, amenity_combined_df, df_to_add):
        amenity_combined_df[amenity_name] = pd.concat([amenity_combined_df, df_to_add])
        return amenity_combined_df
    
    def _store_remove_entries(self, target_path, target_file, remove_amenity_dict):
        for amenity_name, amenity_df in remove_amenity_dict.items():
            remove_amenity_dict[amenity_name] = amenity_df.to_json()
        with open(os.path.join(target_path, target_file), 'w') as f:
            json.dump(remove_amenity_dict, f)
        return os.path.join(target_path, target_file)
    
    def load_nested_json_to_df(self, remove_entries_json_path):
        def recurse_json(d):
            try:
                if isinstance(d, dict):
                    loaded_d = d
                else:
                    loaded_d = json.loads(d)
                for k, v in loaded_d.items():
                    loaded_d[k] = recurse_json(v)
            except (JSONDecodeError, TypeError):
                return d
            return loaded_d
        with open(remove_entries_json_path) as f:
            old_entries_dict = json.load(f)
        for amenity_name, amenity_df in old_entries_dict.items():
            old_entries_dict[amenity_name] = pd.DataFrame(recurse_json(amenity_df))
        return old_entries_dict
    
    def transform_amenity_files_pipeline(self, output_folder, url_dict, onemap_access_token):
        new_amenity_dict = {}
        remove_amenity_dict = {}
        for amenity_name, amenity_details in url_dict.items():
            amenity_file_name, amenity_url = amenity_details['file_name'], amenity_details['url']
            amenity_file_type = amenity_file_name.split('.')[-1]
            amenity_file_path = os.path.join(output_folder, amenity_file_name)
            archive_amenity_file_path = amenity_file_path.split('.')[-2] + "_old." + amenity_file_type
            
            amenity_df, amenity_df_old = self._get_data(amenity_name, amenity_file_path, amenity_file_type), self._get_data(amenity_name, archive_amenity_file_path, amenity_file_type)
            df_to_add, df_to_remove = self._get_differences(amenity_df, amenity_df_old)
            new_amenity_dict[amenity_name] = df_to_add
            remove_amenity_dict[amenity_name] = df_to_remove
    
        new_combined_df_path = self._transform_new_entries(new_amenity_dict, output_folder, onemap_access_token)
        # remove_entries_json_path = self._store_remove_entries(output_folder, "amenities_to_remove.json", remove_amenity_dict)
        # return new_combined_df_path, remove_entries_json_path
        return new_combined_df_path #, remove_entries_json_path

    ### HDB Data Transformation Functions
    def parse_hdb(self, file_path) -> pd.DataFrame:
        
        """parse hdb resale data

        Args:
            file_path: hdb dataset file path

        Returns:
            dataframe: cleaned data with specified columns in db
        """
        with open(file_path) as file:
            hdb_dict = json.load(file)['Result']
        hdb_df = pd.json_normalize(hdb_dict)
         # drop rows with NA long / lat / planning_area
        hdb_df = hdb_df[~hdb_df.long.str.contains("NA")]
        hdb_df = hdb_df[~hdb_df.lat.str.contains("NA")]
        hdb_df = hdb_df[~hdb_df.planning_area.str.contains("NA")]

        hdb_df.insert(1, 'lease_duration', 99)
        hdb_df.insert(1, 'type_of_sale', 'HDB Resale')
        try:
            hdb_df.insert(0, "transaction_year", None)
        except:
            print("year column already added")
        year_month_col = hdb_df["month"].str.split("-")
        hdb_df["transaction_year"] = year_month_col.str[0].astype(np.int64)
        hdb_df["month"] = year_month_col.str[1].astype(np.int64)
        # hdb_df.rename(columns{'month':'transaction_month'}, inplace=True)

        storey_cols = hdb_df["storey_range"].str.split(" ")
        hdb_df["floor_range_start"] = storey_cols.str[0].astype(np.int64)
        hdb_df["floor_range_end"] = storey_cols.str[2].astype(np.int64)
        hdb_df = hdb_df.drop('storey_range', axis=1)
        hdb_df['remaining_lease'] = hdb_df['remaining_lease'].apply(lambda x: self._convert_format(x))
        hdb_df['address'] = hdb_df['block'] + " " + hdb_df['street_name']
        hdb_df['project_name'] = hdb_df['address']
        # standardise column names
        hdb_df.rename(columns={'month': 'transaction_month', 'town': 'town_hdb', 'flat_type': 'property_type', 'street_name': 'street', 'floor_area_sqm': 'floor_area', 'lease_commence_date': 'lease_year', 'resale_price': 'price', 'planning_area': 'district_name'}, inplace=True)
        # define property_id from duplicated property-specific info
        
        hdb_df = hdb_df.drop(columns=['_id'])
        hdb_df.dropna(subset=['district_name', 'project_name'], inplace=True)
        return hdb_df


    def _convert_format(self, remaining_lease):
        """helper function to convert lease format

        Args:
            remaining_lease : xx years xx months

        Returns:
            string: xxYxxM
        """
        rl = remaining_lease.split()
        years = rl[0] + 'Y'
        if len(rl) > 2:
            months = rl[2] + 'M'
        else:
            months = '00M'
        return years + months


    ### URA Data Transformation Functions        
    # Flatten "Transaction" column from URA dataset
    def _unnest(self, df, col) -> pd.DataFrame:
        """flattern `transaction`, define project_id and property_id

        Args:
            df (DataFrame): combined URA data batches
            col (_type_): `Transaction`

        Returns:
            DataFrame: check project_id and property_id definition
        """
        col_flat = pd.DataFrame([[i, x] 
                           for i, y in df[col].items() 
                               for x in y], columns=['Property_index', col])
        col_flat = col_flat.set_index('Property_index')[col]
        col_flat_df = pd.DataFrame(list(col_flat), index = col_flat.index)
        df = df.drop(columns=[col])
        df = df.merge(col_flat_df, left_index=True, right_index=True)
        # autoincrement
        # df['project_id'] = df.groupby(['project']).ngroup()
        df.rename(columns={'planning_area': 'district_name'}, inplace=True)
        return df

    # Extract lease_year and lease_duration from "tenure" column in URA dataset
    def _extract_lease_year_and_duration(self, df, target_col):
        df_result = df.copy()

        # Extract lease_duration
        pattern = r'(\d+) yrs'
        # df_result['lease_duration'] = df_result[target_col].apply(lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else 9999).astype(int)
        df_result['lease_duration'] = df_result[target_col].apply(lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else "")
        df_result['lease_duration'] = df_result['lease_duration'].apply(lambda x: int(x) if x.isdigit() else pd.NA)
        
        # Extract lease_year
        pattern = r'from (\d+)'
        # df_result['lease_year'] = df_result[target_col].apply(lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else -1).astype(int)
        df_result['lease_year'] = df_result[target_col].apply(lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else "")
        df_result['lease_year'] = df_result['lease_year'].apply(lambda x: int(x) if x.isdigit() else pd.NA)

        # TODO for discussion set lease_year and lease_duration for 'Freehold' tenure
        # df_result[df_result['lease_duration'].isna()]['tenure'].unique()

        return df_result

    # Extract floor_range_start and floor_range_end from "floorRange" column in URA dataset
    def _extract_floor_range(self, df, target_col):
        def convert_basement_to_negative(floor_range_start, floor_range_end):
            floor_range_start = floor_range_start.str.replace('B', '-')
            floor_range_end = floor_range_end.str.replace('B', '-')
            return floor_range_start, floor_range_end
        df_result = df.copy()
        floor_range = df_result[target_col].str.split('-')
        try:
            floor_range_start = floor_range.str[0]
            floor_range_end = floor_range.str[1]
            floor_range_start, floor_range_end = convert_basement_to_negative(floor_range_start, floor_range_end)
        except:
            floor_range_start = pd.NA
            floor_range_end = pd.NA
        df_result['floor_range_start'] = floor_range_start
        df_result['floor_range_end'] = floor_range_end
        
        df_result['floor_range_start'] = pd.to_numeric(df_result['floor_range_start'], errors='coerce').astype('Int64')
        df_result['floor_range_end'] = pd.to_numeric(df_result['floor_range_end'], errors='coerce').astype('Int64')

        return df_result

    # Extract transaction_month and transaction_year function from "contractDate" column in URA dataset
    def _extract_transaction_month_and_year(self, df, target_col):
        df_result = df.copy()
        df_result['transaction_month'] = df_result[target_col].str.slice(0, 2).astype(int)
        df_result['transaction_year'] = 2000 + df_result[target_col].str.slice(-2).astype(int)
        return df_result

    # Replace values for typeOfSale column in URA dataset
    def _convert_type_of_sale(self, df, target_col):
        df_result = df.copy()
        df_result = df_result.replace({target_col: {'1': 'New sale', '2': 'Sub sale', '3': 'Resale'}})
        return df_result

    # Save URA dataset from dataframe to json format
    def _save_ura_dataset(self, df, file_path):
        df_dict = df.to_dict('records')
        json_data = json.dumps({'Result': df_dict})
        with open(file_path, 'w') as file:
            file.write(json_data)
        print("Save success: {file_path}".format(file_path=file_path))

    # URA Data transformation pipeline
    def URA_data_transformation_pipeline(self, file_path):
        """URA data transformation steps

        Args:
            folder (string path): folder to read data from 
            file_name (string): data file in input folder
            file_type (filetype): URA json files

        Returns:
            DataFrame: URA dataframe based on the db definition
        """
        ura_df_final = []
        with open(file_path, 'r') as file:
            # Load JSON data from the file
            try:
                data = json.load(file)['Result']
            except KeyError:
                data = json.load(data)
            ura_df_final.extend(data)            
        ura_df_final = pd.DataFrame(ura_df_final)
        # drop rows with NA long / lat / planning_area
        ura_df_final = ura_df_final[~ura_df_final.long.str.contains("NA")]
        ura_df_final = ura_df_final[~ura_df_final.lat.str.contains("NA")]
        ura_df_final = ura_df_final[~ura_df_final.planning_area.str.contains("NA")]
        # ura_df_final = remove_properties_without_latlong(file_name, ura_df, 'lat', 'long')
        ura_df_final = self._unnest(ura_df_final, "transaction")
        # ura_df_final = ura_df_final.drop(columns=["nettPrice"])
        ura_df_final = self._extract_lease_year_and_duration(ura_df_final, 'tenure')
        ura_df_final = self._extract_floor_range(ura_df_final, 'floorRange')
        ura_df_final = self._extract_transaction_month_and_year(ura_df_final, 'contractDate')
        ura_df_final = self._convert_type_of_sale(ura_df_final, 'typeOfSale')
        
        # naming and type of data according to db definition
        common_cols_dict = {
            # URA_col_name: common_col_name
            "area": "floor_area",
            "typeOfSale": "type_of_sale",
            "propertyType": "property_type",
            "project": "project_name",
            "street": "address"
        }
        ura_df_final = ura_df_final.rename(columns = common_cols_dict)

        dtype_dict = {'x': 'float', 
              'y': 'float', 
              'lat': 'float', 
              'long': 'float',
              'floor_area': 'float', 
              'noOfUnits': 'int', 
              'price': 'float', 
              'lease_year': 'Int32', 
              'lease_duration': 'Int32'
              }
        # Change data types for each column
        ura_df_final = ura_df_final.astype(dtype_dict)
        ura_df_final = ura_df_final.drop(columns='district')
        # rename street to address

        ura_df_final.dropna(subset=['district_name', 'project_name'], inplace=True)
        return ura_df_final

if __name__ == "__main__":
    kml = DataParser()
    # Execute Amenity data transformation pipeline
    
    # amenity_out_folder_path = './Amenity Data [Final]'
    amenity_out_folder_path = './Data'
    amenity_combined_df = kml.amenity_data_transformation_pipeline(amenity_out_folder_path)

    # Execute URA data transformation pipeline

    # URA_folder = './URA Data [Final]'
    URA_folder = './Data'

    URA_file_name = 'privatepropertypricesadded', 
                         
    URA_file_type = 'json'
    URA_combined_df = kml.URA_data_transformation_pipeline(URA_folder, URA_file_name, URA_file_type)
    URA_combined_df.to_csv('./Data/URA_combined_df.csv')
    # print(URA_combined_df.head())
    print(URA_combined_df.info())

    # Execute HDB data transformation pipeline
    hdb = kml.parse_hdb('./Data/hdb_resale_full.csv')
    hdb.to_csv('./Data/hdb_clean.csv')
    # print(hdb.head())
    print(hdb.info())
