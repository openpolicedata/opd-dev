from arcgis.features import FeatureLayerCollection

url = 'https://services1.arcgis.com/bqfNVPUK3HOnCFmA/arcgis/rest/services/Police_Calls_for_Service/FeatureServer'
layer_collection = FeatureLayerCollection(url)

# pandas version for both cases is 1.4.3
# Fails with version 2.0.1
# Works with version 2.0.0
layer_collection.layers[0].query(where="DateTimeOfCall >= '2021-01-01' AND  DateTimeOfCall < '2021-12-31T23:59:59.999'",
    result_offset=2000, result_record_count=2000, return_all_records=False, as_df=True)