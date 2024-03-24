# TODO: Convert pipeline to respective DAG Tasks
kml = DataParser()

# Task 1 - Download amenity files
amenity_url_dict = kml.download_amenity_files()
print("Task 1 Complete!\n")

# Task 2 - transform new amenity data
ONEMAP_USERNAME = os.environ['ONEMAP_USERNAME']
ONEMAP_PASSWORD = os.environ['ONEMAP_PASSWORD']
onemap_access_token = one_map_authorise(ONEMAP_USERNAME, ONEMAP_PASSWORD)
new_combined_df_path, remove_entries_json_path = kml.transform_amenity_files_pipeline(amenity_url_dict, onemap_access_token)
print("Task 2 Complete!\n")

# Task 3 - Combine new amenity data with combined_df
amenity_combined_df = pd.read_csv('Data/Combined_amenities.csv').drop(columns=["Unnamed: 0"])
new_combined_df = pd.read_csv(new_combined_df_path)
# old_entries_dict = load_nested_json_to_df(remove_entries_json_path)
# amenity_combined_df = remove_old_entries_pipeline(amenity_combined_df, old_entries_dict)
if not new_combined_df.empty:
    amenity_combined_df = pd.concat([amenity_combined_df, new_combined_df])
print("Task 3 Complete!")