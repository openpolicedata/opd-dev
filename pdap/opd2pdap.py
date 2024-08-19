# Code for converting OPD standards to PDAP standards
import pandas as pd

us_state_to_abbrev = {"Alabama": "AL","Alaska": "AK","Arizona": "AZ","Arkansas": "AR","California": "CA","Colorado": "CO","Connecticut": "CT",
    "Delaware": "DE","Florida": "FL","Georgia": "GA","Hawaii": "HI","Idaho": "ID","Illinois": "IL","Indiana": "IN","Iowa": "IA","Kansas": "KS",
    "Kentucky": "KY","Louisiana": "LA","Maine": "ME","Maryland": "MD","Massachusetts": "MA","Michigan": "MI","Minnesota": "MN","Mississippi": "MS",
    "Missouri": "MO","Montana": "MT","Nebraska": "NE","Nevada": "NV","New Hampshire": "NH","New Jersey": "NJ","New Mexico": "NM","New York": "NY",
    "North Carolina": "NC","North Dakota": "ND","Ohio": "OH","Oklahoma": "OK","Oregon": "OR","Pennsylvania": "PA","Rhode Island": "RI","South Carolina": "SC",
    "South Dakota": "SD","Tennessee": "TN","Texas": "TX","Utah": "UT","Vermont": "VT","Virginia": "VA","Washington": "WA","West Virginia": "WV",
    "Wisconsin": "WI","Wyoming": "WY","District of Columbia": "DC","American Samoa": "AS","Guam": "GU","Northern Mariana Islands": "MP",
    "Puerto Rico": "PR","United States Minor Outlying Islands": "UM","U.S. Virgin Islands": "VI",
}

def to_pdap(df_opd_red):
    df_opd = df_opd_red.copy()

    # PDAP uses state abbreviations
    df_opd["state"] = df_opd["State"].apply(lambda x: us_state_to_abbrev[x])

    # Rename columns to PDAP
    df_opd.rename(columns={"readme":"readme_url"}, inplace=True)
    
    # Convert table type from all caps to PDAP format
    df_opd["record_type"] = df_opd["TableType"].apply(lambda x: x.title())

    # Convert OPD record type names to PDAP
    opd_to_pdap_record_types = {
        "Arrests" : "Arrest Records", 
        "Calls For Service":"Calls for Service",
        "Complaints":"Complaints & Misconduct",
        'Crashes':'Accident Reports',
        'Employee':'Personnel Records',
        'Incidents':'Incident Reports',
        'Lawsuits':'Court Cases',
        "Officer-Involved Shootings":"Officer Involved Shootings", 
        'Traffic Citations':'Citations',
        "Use Of Force" : "Use of Force Reports",
        }
    df_opd["record_type"] = df_opd["record_type"].apply(lambda x: opd_to_pdap_record_types[x] if x in opd_to_pdap_record_types else x)

    # Check that all record type mappings worked
    for k,v in opd_to_pdap_record_types.items():
        if v not in df_opd["record_type"].unique():
            raise KeyError(f"Key {k} not found in OPD record types to update to PDAP type {v}")

    df_opd["agency_supplied"] = "yes"

    # All our data are individual records
    df_opd["detail_level"] = "Individual record"
    df_opd["scraper_url"] = "https://pypi.org/project/openpolicedata/"

    # access_type can currently only be API or web page. Should something like Downloadable File be available for files to indicate that the data doesn't 
    # have to be scraped or copied from a web page? We're including Downloadable File as an option here in case someone wants to use it
    download = "Download"
    data_type_to_access_type = {"Socrata":"API,"+download, "ArcGIS":"API,"+download,
        "CSV":download,"Excel":download,"Carto":"API,"+download,'HTML':'Web page',
        "CKAN":"API,"+download}

    for k in df_opd.index:
        data_types = df_opd.loc[k, "DataType"].split(',')
        access_types = [data_type_to_access_type[x.strip()] for x in data_types]
        df_opd.loc[k, "access_type"] = ",".join(set(access_types))

        portal_type = [data_types[k] for k in range(len(data_types)) if access_types[k]!=download]
        df_opd.loc[k, "data_portal_type"] = ",".join(set(portal_type))

        # PDAP uses XLS instead of Excel
        record_format = [data_types[k].replace('Excel','XLS') for k in range(len(data_types)) if access_types[k]==download]
        df_opd.loc[k, "record_format"] = ",".join(set(record_format))

    return df_opd