import os
import sys
from datetime import datetime
import time
import requests
import urllib
import pandas as pd
pd.options.mode.chained_assignment = None

import stanford

if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    output_dir = os.path.join(".","data","pdap")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    output_dir = os.path.join("..","data","pdap")
import openpolicedata as opd

kstart = 0

df_stanford = stanford.get_stanford()

us_state_to_abbrev = {"Alabama": "AL","Alaska": "AK","Arizona": "AZ","Arkansas": "AR","California": "CA","Colorado": "CO","Connecticut": "CT",
    "Delaware": "DE","Florida": "FL","Georgia": "GA","Hawaii": "HI","Idaho": "ID","Illinois": "IL","Indiana": "IN","Iowa": "IA","Kansas": "KS",
    "Kentucky": "KY","Louisiana": "LA","Maine": "ME","Maryland": "MD","Massachusetts": "MA","Michigan": "MI","Minnesota": "MN","Mississippi": "MS",
    "Missouri": "MO","Montana": "MT","Nebraska": "NE","Nevada": "NV","New Hampshire": "NH","New Jersey": "NJ","New Mexico": "NM","New York": "NY",
    "North Carolina": "NC","North Dakota": "ND","Ohio": "OH","Oklahoma": "OK","Oregon": "OR","Pennsylvania": "PA","Rhode Island": "RI","South Carolina": "SC",
    "South Dakota": "SD","Tennessee": "TN","Texas": "TX","Utah": "UT","Vermont": "VT","Virginia": "VA","Washington": "WA","West Virginia": "WV",
    "Wisconsin": "WI","Wyoming": "WY","District of Columbia": "DC","American Samoa": "AS","Guam": "GU","Northern Mariana Islands": "MP",
    "Puerto Rico": "PR","United States Minor Outlying Islands": "UM","U.S. Virgin Islands": "VI",
}

# If None, will use default file from GitHub
opd_source_file = None
opd_source_file = r"C:\Users\matth\repos\opd-data\opd_source_table.csv"
if opd_source_file!=None:
    opd.datasets.datasets = opd.datasets._build(opd_source_file)
df_opd_orig = opd.datasets.query()

df_tracking = pd.read_csv(r"C:\Users\matth\repos\opd-data\police_data_source_tracking.csv")
df_pdap = pd.read_csv(r"C:\Users\matth\Downloads\PDAP Data Sources (5).csv")
# df_pdap = pd.read_csv(r"https://github.com/Police-Data-Accessibility-Project/data-sources-mirror/raw/main/csv/data_sources.csv")
df_pdap_agencies = pd.read_csv(r"https://github.com/Police-Data-Accessibility-Project/data-sources-mirror/raw/main/csv/agencies.csv")
df_outages = pd.read_csv(r"C:\Users\matth\repos\opd-data\outages.csv")

# Code for using PDAP data source table directly from GitHub. There appear to be some issues with this table. For example, detail
# level does not have information about if a table has Individual records. On GitHub, values are federal, state, county, etc.
# PDAP sources table is a little different than the one on airtable. Needs to be merged with agences to get some of the needed values
# df_pdap["agency_described"] = df_pdap["agency_described"].apply(lambda x: x[2:-2])
# # Confirm that all agencies described match a uid
# for k,x in enumerate(df_pdap["agency_described"]):
#     if (df_pdap_agencies["airtable_uid"]==x).sum()!=1:
#         raise ValueError(f"UID in position {k}")
# df_pdap = df_pdap.merge(df_pdap_agencies, left_on="agency_described", right_on="airtable_uid", how="outer", suffixes=("","_a"))
# df_pdap["agency_described"] = df_pdap["name_a"]

df_opd_orig["site_outage"] = False
for k in range(len(df_outages)):
    df_opd_orig["site_outage"] = df_opd_orig["site_outage"] | \
        ((df_opd_orig["State"]==df_outages["State"][k]) & (df_opd_orig["SourceName"]==df_outages["SourceName"][k]) & \
        (df_opd_orig["Agency"]==df_outages["Agency"][k])  & (df_opd_orig["TableType"]==df_outages["TableType"][k]) & \
        (df_opd_orig["Year"]==df_outages["Year"][k]))

# Does PDAP contain Stanford Open Policing Data
pdap_has_stanford = df_pdap["source_url"].str.contains("stanford.edu").any()

# Only keep Individual record data
df_pdap = df_pdap[df_pdap["detail_level"] == "Individual record"]

# Rename columns to PDAP
# PDAP has a source_url which points to a general link for accessing the data.
# Unfortunately, we do not store this URL
# We have set source_url for cases where we were able to find it in the PDAP table (for sources that are already 
# there for which we might be able to fill in some empty fields) and for cases where we can derive it from URLs
# that we do store.
# OPD uses several URLs that may help someone manually identify source_url
# api_url is the URL used to request data from the API or to download the file
# readme_url is a URL to the readme. Sometimes, this is identical to the source_url but other times, it is not. 
# We have identified Socrata cases where readme_url = source_url and set source_url for those cases
# general_source_url is a link to all record_types from the source not any specific one
df_opd_orig.rename(columns={"Description":"description", "readme":"readme_url",
    "URL":"api_url"}, inplace=True)

# Convert state to abbreviation
df_opd_orig["state"] = df_opd_orig["State"].apply(lambda x: us_state_to_abbrev[x])
# Convert table type from all caps to PDAP format
split_type = df_opd_orig["TableType"].apply(lambda x: x.title().split(" - "))
df_opd_orig["record_type"] = split_type.apply(lambda x: x[0])
df_opd_orig["record_subtype"] = split_type.apply(lambda x: x[1] if len(x)>1 else "")
# Convert OPD record type names to PDAP
# NOTE: There are still other record types that don't match or haven't been matched
opd_to_pdap_record_types = {"Arrests" : "Arrest Records", "Use Of Force" : "Use of Force Reports",
    "Officer-Involved Shootings":"Officer Involved Shootings", "Complaints":"Complaints & Misconduct",
    "Calls For Service":"Call for Service"}
df_opd_orig["record_type"] = df_opd_orig["record_type"].apply(lambda x: opd_to_pdap_record_types[x] if x in opd_to_pdap_record_types else x)

# Check that all record type mappings worked
for k,v in opd_to_pdap_record_types.items():
    if v not in df_opd_orig["record_type"].unique():
        raise KeyError(f"Key {k} not found in OPD record types to update to PDAP type {v}")

# Drop datasets that have multiple years in separate rows in oru table
# df_opd = df_opd_orig.drop_duplicates(subset=["state","SourceName","Agency","record_type"])
df_opd = df_opd_orig.copy()

# Initialize columns
# access_restrictions included for future use if data requires a token to access
# possible_pdap_name_match is the name of a source from PDAP that might be a match
init_to_empty = ["data_portal_type", "agency_aggregation", "coverage_start", "coverage_end", "access_type","data_portal_type",
    "access_restrictions", "access_restrictions_notes","record_format", "source_url", "general_source_url","supplying_entity",
    "agency_originated", "error_msgs","possible_pdap_name_match"]
for x in init_to_empty:
    df_opd[x] = ""
    
# May need to update this in the future if we ever have non-agency supplied data
init_to_true = ["record_download_option_provided"]
for x in init_to_true:
    df_opd[x] = True

df_opd["agency_supplied"] = "yes"
df_opd["record_type_changed"] = False

# All our data are individual records
df_opd["detail_level"] = "Individual record"
df_opd["scraper_url"] = "https://pypi.org/project/openpolicedata/"

cur_year = datetime.now().year

# access_type can currently only be API or web page. Should something like Downloadable File be available for files to indicate that the data doesn't 
# have to be scraped or copied from a web page? We're including Downloadable File as an option here in case someone wants to use it
downloadable_file = "Downloadable File"
data_type_to_access_type = {"Socrata":"API", "ArcGIS":"API","CSV":downloadable_file,"Excel":downloadable_file,"Carto":"API"}
for k in df_opd.index:
    if k<kstart:
        continue

    if df_opd.loc[k,"DataType"]=="Socrata":
        df_opd.loc[k,"api_url"] = df_opd.loc[k,"api_url"]+"/resource/"+df_opd.loc[k,"dataset_id"]+".json"
    elif df_opd.loc[k,"DataType"]=="Carto":
        username = df_opd.loc[k,"api_url"]
        if username.startswith("https://"):
            username = username.replace("https://", "")

        if ".carto" in username:
            username = username[:username.find(".carto")]

        df_opd.loc[k,"api_url"] = "https://" + username + ".carto.com/api/v2/sql?q=SELECT * FROM " + df_opd.loc[k,"dataset_id"]

    cur_row = df_opd.loc[k]

    print("{}: {} {}".format(k, df_opd.loc[k,"SourceName"], df_opd.loc[k,"record_type"]))

    if "stanford.edu" in df_opd.loc[k,"api_url"]:
        if pdap_has_stanford:
            raise ValueError("Stanford in PDAP needs setup")
        match = (df_stanford["state"]==cur_row["State"]) & (df_stanford["source"]==cur_row["SourceName"]) & (df_stanford["agency"]==cur_row["Agency"])
        if match.sum()!=1:
            raise ValueError("Unable to find the correct # of Stanford matches")
        df_opd.loc[k, "state"] = cur_row["State"]
        df_opd.loc[k, "coverage_start"] = df_stanford[match]["start_date"].iloc[0].strftime('%m/%d/%Y')
        df_opd.loc[k, "coverage_end"] = df_stanford[match]["end_date"].iloc[0].strftime('%m/%d/%Y')
        df_opd.loc[k, "access_type"] = downloadable_file
        df_opd.loc[k, "record_format"] = "CSV"
        df_opd.loc[k, "source_url"] = "https://openpolicing.stanford.edu/data/"
        df_opd.loc[k, "supplying_entity"] = "Stanford Open Policing Project"
        df_opd.loc[k, "agency_originated"] = "yes"
        df_opd.loc[k, "agency_supplied"] = "no"
        continue
    
    src = opd.Source(df_opd.loc[k,"SourceName"], df_opd.loc[k, "State"])

    for attempt in range(0,2):
        if df_opd_orig.loc[k, "site_outage"]:
            break
        try:
            if df_opd.loc[k, "Year"] == opd.defs.MULTI:
                if (src.datasets["TableType"] == cur_row["TableType"]).sum()>1:
                    # Going to need to read this in manually to determine coverage range
                    year_vals = list(src.datasets[src.datasets["TableType"] == cur_row["TableType"]]["Year"])
                    year_ints = [x for x in year_vals if not isinstance(x,str)]
                    year_ints.sort()
                    if year_ints == [x for x in range(year_ints[0],year_ints[-1]+1)]:
                        years_req = [year_ints[-1]+1, cur_year]
                    else:
                        # Find the gap
                        gap_found = False
                        for j in range(len(year_ints)-1):
                            is_gap = year_ints[j+1] - year_ints[j] > 1
                            if is_gap and not gap_found:
                                gap_found = True
                                years_req = [year_ints[j]+1, year_ints[j+1]-1]
                            elif is_gap:
                                raise NotImplementedError("Gap already found")
                        
                        if not gap_found:
                            raise ValueError("No gap found")
                        
                    table = src.load(df_opd.loc[k,"TableType"], years_req)
                    if table.table is None or len(table.table)==0:
                        raise ValueError("Empty table")
                    df_opd.loc[k,"coverage_start"] = table.table[cur_row["date_field"]].min().strftime('%m/%d/%Y')
                    if years_req[-1] < cur_year-2:
                        # Assuming this isn't a current dataset
                        df_opd.loc[k,"coverage_end"] = table.table[cur_row["date_field"]].max().strftime('%m/%d/%Y')

                    break
                
                years = src.get_years(table_type=df_opd.loc[k,"TableType"], force=True)
                if pd.notnull(df_opd.loc[k,"date_field"]):
                    # Fill out start date with date from data
                    nrows = 1 if data_type_to_access_type[df_opd.loc[k, "DataType"]]=="API" else None
                    table = src.load(year=years[0], table_type=df_opd.loc[k,"TableType"], nrows=nrows)
                    if len(table.table)==0:
                        raise ValueError("No records found in first year")
                    df_opd.loc[k,"coverage_start"] = table.table[cur_row["date_field"]].min().strftime('%m/%d/%Y')
                    if years[-1] < cur_year-2:
                        # Assuming this isn't a current dataset
                        table = src.load(year=years[-1], table_type=df_opd.loc[k,"TableType"])
                        df_opd.loc[k,"coverage_end"] = table.table[cur_row["date_field"]].max().strftime('%m/%d/%Y')
                else:
                    df_opd.loc[k,"coverage_start"] = f"1/1/{years[0]}"
                    if years[-1] < cur_year-2:
                        # Assuming this isn't a current dataset
                        df_opd.loc[k,"coverage_end"] = f"12/31/{years[-1]}"
            else:
                df_opd.loc[k,"coverage_start"] = "1/1/{}".format(cur_row["Year"])
                df_opd.loc[k,"coverage_end"] = "12/31/{}".format(cur_row["Year"])

            break
        except (requests.exceptions.ReadTimeout, KeyError, requests.exceptions.HTTPError) as e:
            if len(e.args)>0 and any([x in str(e.args[0]) for x in ["Error Code 500","Read timed out","404 Client Error"]]):
                if attempt==1:
                    df_opd.loc[k, "error_msgs"] = f"Error getting date range: {e.args[0]}"
                else:
                    print("Pausing for 5 minutes")
                    time.sleep(300)
            else:
                raise e
        except opd.exceptions.OPD_DataUnavailableError as e:
            if attempt==1:
                df_opd.loc[k, "error_msgs"] = f"Error getting date range: {e.args[0]}"
            else:
                print("Pausing for 5 minutes")
                time.sleep(300)
        except Exception as e:
            raise

    # Find all rows in our table corresponding to this record_type and source
    # matches = (df_opd_orig["state"]==df_opd.loc[k,"state"]) & (df_opd_orig["SourceName"]==df_opd.loc[k,"SourceName"]) & \
    #     (df_opd_orig["Agency"]==df_opd.loc[k,"Agency"]) & (df_opd_orig["record_type"]==df_opd.loc[k,"record_type"])
    # matches = df_opd_orig[matches]

    # data_types = list(set(matches["DataType"].unique()))
    data_types = [cur_row["DataType"]]
    access_types = [data_type_to_access_type[x] for x in data_types]

    # Replacing web page access type with API in some cases
    df_opd.loc[k,"access_type"] = ",".join(set(access_types))

    portal_type = [data_types[k] for k in range(len(data_types)) if access_types[k]=="API"]
    df_opd.loc[k, "data_portal_type"] = ",".join(portal_type)

    record_format = [data_types[k] for k in range(len(data_types)) if access_types[k]==downloadable_file]
    df_opd.loc[k, "record_format"] = ",".join(record_format)

    if df_opd.loc[k, "Year"] == opd.defs.MULTI and pd.notnull(df_opd.loc[k, "readme_url"]) and df_opd.loc[k, "DataType"]=="Socrata":
        url = df_opd.loc[k, "readme_url"].strip("/")
        if url.endswith(df_opd.loc[k, "dataset_id"]):
            df_opd.loc[k, "source_url"] = url

    add_ons = ["", "Police Department", "Department of Justice", "Department of Justice", "Metropolitan Police Department", "Office of the Attorney General"]
    matches = (df_tracking["Source"].str.strip() == df_opd.loc[k,"SourceName"]) & (df_tracking["State"] == df_opd.loc[k,"State"])
    for add in add_ons:
        matches = (df_tracking["Source"].str.strip() == (df_opd.loc[k,"SourceName"]+" "+add).strip()) & \
            (df_tracking["State"] == df_opd.loc[k,"State"])
        if matches.any():
            break

    if not matches.any():
        if not matches.any():
            matches = (df_tracking["Source"] == df_opd.loc[k,"SourceName"].strip(" City")+" Police Department") & (df_tracking["State"] == df_opd.loc[k,"State"])
            if not matches.any():
                matches = (df_tracking["Source"] == df_opd.loc[k,"State"] + " " + df_opd.loc[k,"SourceName"])
                if not matches.any():
                    matches = (df_tracking["Source"].str.strip() == df_opd.loc[k,"AgencyFull"]) & (df_tracking["State"] == df_opd.loc[k,"State"])
                    if not matches.any():
                        raise ValueError("Unable to find tracking match")

    matches = df_tracking[matches]
    if len(matches)>1:
        raise ValueError("Multiple tracking matches")

    url = matches["Open Data Website"].iloc[0].strip("/")
    if df_opd.loc[k, "Year"] == opd.defs.MULTI and df_opd.loc[k, "DataType"]=="Socrata" and url.endswith(df_opd.loc[k, "dataset_id"]):
        df_opd.loc[k, "source_url"] = url
    df_opd.loc[k, "general_source_url"] = matches["Open Data Website"].iloc[0]

    if df_opd.loc[k, "Agency"] == opd.defs.MULTI or (df_opd.loc[k, "SourceName"]==df_opd.loc[k, "State"] and df_opd.loc[k, "Agency"]=="NONE"):
        # This is aggregated data
        df_opd.loc[k, "agency_supplied"] = "no"
        df_opd.loc[k, "agency_aggregation"] = "state"
        df_opd.loc[k, "agency_described"] = df_opd.loc[k, "State"] + " Aggregated - " + df_opd.loc[k, "state"]
        df_opd.loc[k, "municipality"] = "All Police Departments"
        # This is the best that we can do right now. It really should be the name of the state agency that publishes the data
        if df_opd.loc[k, "State"]=="Virginia":
            df_opd.loc[k, "supplying_entity"] = "Virginia State Police"
        elif df_opd.loc[k, "State"]=="California":
            df_opd.loc[k, "supplying_entity"] = "California Department of Justice"
        elif df_opd.loc[k, "State"]=="New Jersey":
            df_opd.loc[k, "supplying_entity"] = "New Jersey Office of the Attorney General"
        elif df_opd.loc[k, "State"]=="Connecticut":
            df_opd.loc[k, "supplying_entity"] = "Connecticut Racial Profiling Prohibition Project"
        else:
            raise ValueError("Unknown multi-agency")
        df_opd.loc[k, "agency_originated"] = "yes"

    # Convert agency name to most likely PDAP agency name
    # df_opd_orig["agency_described"] = df_opd_orig.apply(lambda x: 
    #     x["Agency"].strip() + " Police Department - " + x["state"] if x["Agency"]!=opd.defs.MULTI else x["Agency"],
    #     axis = 1)

    # Get PDAP rows for this state and record type
    pdap_matches_in_state = df_pdap[df_pdap["state"]==df_opd.loc[k, "state"]]
    pdap_matches = pdap_matches_in_state[pdap_matches_in_state["record_type"]==df_opd.loc[k, "record_type"]]
    stops_type_update = False
    if len(pdap_matches)==0 and df_opd.loc[k, "record_type"]=="Stops":
        stops_type_update = True
        # We know that some stops tables (contains both traffic and pedestrian stops) are labeled traffic stops
        pdap_matches = pdap_matches_in_state[pdap_matches_in_state["record_type"]=="Traffic Stops"]
    pdap_matches = pdap_matches[~pdap_matches["source_url"].str.endswith(".pdf")]
    if df_opd.loc[k, "Agency"] == opd.defs.MULTI:
        pdap_matches = pdap_matches[pdap_matches["agency_described"] == df_opd.loc[k, "agency_described"]]
    else:
        pdap_matches = pdap_matches[pdap_matches["agency_described"].str.startswith(df_opd.loc[k, "SourceName"])]
        if len(pdap_matches)>0:
            pdap_matches_orig = pdap_matches.copy()
            pdap_matches = pdap_matches[pdap_matches["agency_described"].str.startswith(df_opd.loc[k, "AgencyFull"])]
            if len(pdap_matches)==0:
                pdap_matches = pdap_matches_orig[pdap_matches_orig["agency_described"].str.startswith(df_opd.loc[k, "AgencyFull"].replace(" Department",""))]
                if len(pdap_matches)==0:
                    raise ValueError("Check PDAP match")
        if len(pdap_matches)==1:
            pass
            # if "County" in df_opd.loc[k, "SourceName"] and pdap_matches["county"].iloc[0]!=df_opd.loc[k, "SourceName"]:
            #     raise ValueError("county does not match")
            # elif pdap_matches["municipality"].iloc[0]!=df_opd.loc[k, "SourceName"] and df_opd.loc[k, "SourceName"] not in ["Austin","Seattle"]: # Austin listed as Cedar Park for some reason
            #     raise ValueError("municipality does not match")
        elif len(pdap_matches)>1:
            # Check if all source URLs are the same
            if (pdap_matches["source_url"]==pdap_matches["source_url"].iloc[0]).all():
                print("{} matches for {} table for {}".format(len(pdap_matches), pdap_matches["record_type"].iloc[0], pdap_matches["agency_described"].iloc[0]))
                pdap_matches = pdap_matches.iloc[0].to_frame().transpose()
            else:
                if pd.notnull(df_opd.loc[k,"dataset_id"]):
                    pdap_matches = pdap_matches[pdap_matches["source_url"].str.contains(df_opd.loc[k,"dataset_id"])]
                ignore = False
                if len(pdap_matches)==0 and df_opd.loc[k,"SourceName"]=="Philadelphia" and df_opd.loc[k,"readme_url"]!=None:
                    url_matches = pdap_matches["source_url"].str.contains(urllib.parse.urlparse(df_opd.loc[k,"readme_url"]).netloc)
                    if url_matches.any():
                        raise NotImplementedError("Not handling this case yet")
                    else:
                        ignore = True

                if len(pdap_matches)!=1 and not ignore:
                    raise ValueError("Unexpected # of matches")
    
    if len(pdap_matches)==1:
        if stops_type_update:
            df_opd.loc[k, "record_type_changed"] = True
        df_opd.loc[k, "possible_pdap_name_match"] = pdap_matches["name"].iloc[0]
    elif len(pdap_matches)>1:
        raise ValueError("Unexpected # of matches")


# Drop OPD columns
df_opd.drop(columns=["State","SourceName","Agency","TableType","Year","DataType","date_field","dataset_id","agency_field","min_version"], inplace=True)

# Resort columns to match PDAP
new_cols = [x for x in df_pdap.columns if x in df_opd.columns]
extra_cols = [x for x in df_opd.columns if x not in new_cols]
new_cols.extend(extra_cols)

df_opd = df_opd[new_cols]

output_file = os.path.join(output_dir, "OPD_PDAP_Submission_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".csv")
df_opd.to_csv(output_file, index=False)