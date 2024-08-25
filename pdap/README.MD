# Overview
opd_pdap_sync.py currently compares the current OPD and PDAP datasets to find:
- Datasets in OPD not in PDAP
- Datasets in OPD and PDAP where the PDAP URL needs updated (such as when a dataset is moved)
- Label mismatches between OPD and PDAP
- Updates to PDAP coverage start and end dates based on corresponding OPD datasets

In the future, it will also be used to identify incident-level datasets in PDAP that are not OPD and datasets in PDAP that have dead links (beyond those with a counterpart in OPD).

# Output Files
opd_pdap_sync outputs 2 CSV files:
1. New datasets file: List of datasets in OPD detected to not be in PDAP
2. Updates file: List of datasets in PDAP that may need updates to fields based on corresponding data in OPD.

In the case of the Updates file, suggested updates likely fall into a few categories:
- Updates that likely should be made to PDAP because OPD actively ensures these values are correct (URLs in some cases, coverage start and stops dates, data access types)
- Updates that perhaps should be made depending on how PDAP defines its field values (record types)
- Updates that perhaps should be made to make it easier to keep PDAP and OPD in sync

## Fields Describing Suggested PDAP Updates
Below are definition of the fields in the output files. The fields often indicate the current values for PDAP datasets that have been identified to be likely matches with OPD datasets. In cases where more than 1 match in PDAP is found, some fields are left empty because it is assumed that additional work will already be necessary to identify which dataset(s) correspond to OPD's dataset and which might be different. In many cases, it was found that there are multiple entries in the PDAP for the same dataset or multiple entries for different years of datasets (to match OPD's original dataset submission).

- `new`: If true, dataset was not found in PDAP
- `stanford_new`: If true, this agency is not found in the Stanford Open Policing Project entry for PDAP.
- `OPD Posted Removed Data`: If true, this is data that a department took down but OPD re-posted on GitHub. It is not currently part of PDAP. It may be useful to have a larger discussion to strategize about what to do about datasets that are taken down by PDs. OPD regularly backs up data.
- `update_source_url`: If populated, current PDAP source URL(s) do not match OPD. This field contains a description of any potential issue with the URL(s) as well as the URL(s) itself. It would be useful to have a discussion about how PDAP defines source_url. A corresponding `source_url` field contains a suggested source_url field identified by OPD. It is assume that the PDAP source_url is meant to be a general link to find all datasets for a record_type from a PD (not a link to an API URL or CSV file and not a link to the specific year's dataset where there are annual datasets at different URLs for each year).
- `update_opd_scraper`: If true, we are requested that OPD be added as a scraper for this dataset
- `update_coverage_start`: If populated with the current PDAP value, it is recommended to update to the OPD value in `coverage_start`. OPD's values are regularly checked and updated using a Python script
- `update_coverage_end`: If populated with the current PDAP value, it is recommended to update to the OPD value in `coverage_end`. OPD's values are regularly checked and updated using a Python script
- `update_readme`: If populated, an update to the readme URL is recommended to the OPD value in `readme_url`. A value of `add` in update_readme is for empty fields and update is for replacing a current value. The current value is in `pdap_readme_url`
- `update_access_type`: If populated an update of the access_type to OPD's value in `access_type` is recommended. Field contains current value of access_type in PDAP.
- `update_data_portal_type`: If populated an update of the data_portal_type to OPD's value in `data_portal_type` is recommended. Field contains current value of data_portal_type in PDAP.
- `update_data_portal_type`: If populated an update of the data_portal_type to OPD's value in `data_portal_type` is recommended. Field contains current value of data_portal_type in PDAP.
- `update_agency_aggregation`: If populated with PDAP's current value, it is recommended to update to the OPD value in `agency_aggregation`
- `update_record_format`: If populated with PDAP's current value, it is recommended to update to the OPD value in `record_format`
- `update_supplying_entity`: If populated with PDAP's current value, it is recommended to update to the OPD value in `supplying_entity`
- `update_agency_originated`: If populated with PDAP's current value, it is recommended to update to the OPD value in `agency_originated`
- `update_agency_supplied`: If populated with PDAP's current value, it is recommended to update to the OPD value in `agency_supplied`
- `update_detail_level`: If populated with PDAP's current value, it is recommended to update to the OPD value in `detail_level`
- `update_stanford_not_aggregated`: Populated with name of state if the state is marked as being aggregated data in the agency for Stanford Open Policing but is in fact only the data for the State Police (or equivalent)
- `airtable_uid`: PDAP airtable_uid of any matches identified with OPD dataset