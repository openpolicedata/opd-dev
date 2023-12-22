import usaddress
import pandas as pd
import re
from collections import OrderedDict
from openpolicedata.defs import states as states_dict
from openpolicedata.utils import split_words

STREET_NAMES = usaddress.STREET_NAMES
STREET_NAMES.update(['la','bl','exwy','anue','corridor'])
# Not sure that these are ever street names
STREET_NAMES.remove('fort') 
STREET_NAMES.remove('center')

_states = list(states_dict.keys())
for v in states_dict.values():
    _states.append(v)

def find_address_col(df_test):
    addr_col = [x for x in df_test.columns if "LOCATION" in x.upper()]
    for col in addr_col:
        tags = df_test[col].apply(lambda x: tag(x, col, error='ignore'))
        tags = tags[tags.apply(lambda x: isinstance(x[1],str) and x[1]!='Null')]
        if tags.apply(lambda x: x[1]).isin(['Street Address','Intersection','Block Address', 'Street Name', 
                                            'StreetDirectional', 'County', 'Building', 'Bridge']).all():
            return [col]
        else:
            return []
    addr_col = [x for x in df_test.columns if x.upper() in ["STREET"]]
    if len(addr_col):
        return addr_col
    addr_col = [x for x in df_test.columns if 'address' in split_words(x,case='lower')]
    return addr_col

_default_delims = ['^', r"\s", "$", ',']

# Based on https://stackoverflow.com/questions/30045106/python-how-to-extend-str-and-overload-its-constructor
class ReText(str):
    def __new__(cls, value, name=None, opt=False, delims=_default_delims, lookahead=None):
        lookahead = lookahead if lookahead else delims
        if isinstance(value, list):
            value_list = value
            value = ''
            for v in value_list:  # Append values in list
                if isinstance(v,str):
                    value+=v
                else:
                    # List of possible values
                    value+=rf'({"|".join(sorted(list(v), key=len, reverse=True))})'

        value = str(value)        
        if name != None:
            value = rf"(?P<{name}>{value}"
            value+=r"(?=(" + "|".join(lookahead) +")))"

            if opt and value[-1]!='?':
                value+=r"?" 
        elif opt and value[-1]!='?':
            if value[0]!='(':
                value = '('+value+')'
            else:
                # Check if string is enclose in parentheses
                num_open = 0
                slash_last = False
                for c in value[:-1]:
                    if c=='(' and not slash_last: # Ignore r'\('
                        num_open+=1
                    elif c==')' and not slash_last:  # Ignore r'\)'
                        num_open-=1
                        if num_open==0:
                            # Parenthesis at beginning does not enclose entire string
                            value = '('+value+')'
                    slash_last = c=='\\'

                    
            value+=r"?" 

        # explicitly only pass value to the str constructor
        self = super(ReText, cls).__new__(cls, value)
        self.opt = opt
        self.delims = delims
        return self

    def __add__(self, other):
        x = str(self)
        if isinstance(other, ReText):
            # This can probably be handled better in the case where ReText are nested
            x+=r"("+ "|".join(self.delims) + ")"
            if other.opt:
                x+=r"*"
            else:
                x+=r"+"
            return ReText(x+str(other), opt=other.opt, delims=other.delims)
        else:
            return x+other
        
    def __radd__(self, other):
        return  ReText(other+str(self), opt=self.opt, delims=self.delims)
        
    def ordinal(self, ord):
        ord = 'Second' if ord==2 else ord
        return ReText(self.replace(r"(?P<", r"(?P<"+ord), opt=self.opt, delims=self.delims)

_building_name = ReText([r'[a-z]+\s+[a-z ]+\s+',['center','hospital','shelter','motel']], 'BuildingName')
_opt_building_name = ReText(_building_name, opt=True)
_p_building = re.compile("^"+_building_name+"$", re.IGNORECASE)

_bridge_name = ReText(r'[a-z ]+\sbridge', 'BridgeName')
_over = ReText('over', 'Preposition')
_body_of_water = ReText([r'[a-z ]+\s',['creek','river']], 'BodyOfWater')
_p_bridge = re.compile("^"+_bridge_name+_over+_body_of_water+"$", re.IGNORECASE)

# Prevent street names from being in place name    
_opt_place = ReText(r"(?<=\s)((?!(?<=\s)("+"|".join(STREET_NAMES)+r")\s)[a-z ])+", 'PlaceName', delims=[r'\s',','])
_opt_state = ReText([_states], 'StateName')
_opt_zip = ReText(r'\d{5}+', 'ZipCode', opt=True)
_opt_place_line = ReText("("+_opt_place+_opt_state+")",opt=True)+_opt_zip

post_type_delims = _default_delims.copy()
post_type_delims.extend([r'\n'])
_block_num = ReText(r"\d+[0X]{2}", "BlockNumber")
_block_ind = ReText([["Block of",'BLK', 'block', 'of']], "BlockIndicator")
_block_ind2 = ReText('between', "BlockIndicator")
_opt_street_dir = ReText([usaddress.DIRECTIONS, r'\.?'], 'StreetNamePreDirectional', opt=True)
_street_name = ReText(r"i?[\w \.\-']+?", "StreetName")  # i- for interstates

_pre_street_names = STREET_NAMES.copy()
_pre_street_names.remove("st")
_pre_street_names.remove("la")
_pre_street_names.remove("garden")
_pre_type = r"("+ReText([_states])+r'(?=\s))?\s*'+str(ReText([_pre_street_names, r'\.?']))
_opt_pre_type = r'\s*('+ReText(_pre_type, 'StreetNamePreType')+r"(?!\s("+"|".join(STREET_NAMES)+r")))?\s*"
_opt_post_type = ReText([STREET_NAMES, r'\.?'], 'StreetNamePostType', opt=True, delims=post_type_delims)
_opt_post_dir = ReText([usaddress.DIRECTIONS, r'\.?'], 'StreetNamePostDirectional', opt=True, delims=post_type_delims)
# post_type2 = ReText(r'\w+\.', 'StreetNamePostType', opt=False, delims=post_type_delims)
_street_match = _opt_street_dir+_opt_pre_type+_street_name+_opt_post_type+_opt_post_dir
_opt_address_num = ReText(r"[\dX]+", 'AddressNumber', opt=True)
_street_match_w_addr = _opt_address_num+_street_match

_and_str_block = ReText([['and',r'&']], 'BlockRangeSeparator')
_opt_near = ReText('near', 'Distance', opt=True)
_opt_cross_street = ReText((_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir).replace(r"(?P<StreetName", r"(?P<CrossStreetName"), opt=True)
_p_block = re.compile("^"+_block_num+_block_ind+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir+
                      _opt_place_line+"$", re.IGNORECASE)
_p_block2 = re.compile("^"+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir+_block_ind2+
                       (_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir).replace(r"(?P<StreetName", r"(?P<CrossStreetName")+_and_str_block+
                       (_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir).replace(r"(?P<StreetName", r"(?P<SecondCrossStreetName")+
                       "$", re.IGNORECASE)
_p_block3 = re.compile("^"+_block_num+_block_ind+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir+
                      _opt_place_line+_opt_near+_opt_cross_street+"$", re.IGNORECASE)

_dir = ReText([usaddress.DIRECTIONS, r'\.?'], 'Direction')
_opt_dist = ReText(r"\d+ miles?", 'Distance', opt=True)
_of = ReText('of', 'Preposition')
_p_directional = re.compile('^'+_street_match+_opt_dist+_dir+_of+_street_match.replace(r"(?P<StreetName", r"(?P<CrossStreetName")+"$", re.IGNORECASE)

_and_str = ReText([['and',r'&', r'/', 'at']], 'IntersectionSeparator')
_county_delims = _default_delims.copy()
_county_delims.append(r'\)')
  # Place name in parentheses
_opt_place_in_paren = ReText(r"(\("+ReText(r"[a-z \.\n]+",'PlaceName', delims=_county_delims)+r"\))", opt=True, delims=["$"])
_p_intersection = re.compile("^"+_street_match_w_addr+_and_str+
                             (_street_match_w_addr).ordinal(2)+
                             _opt_place_line+"$", re.IGNORECASE)
_p_intersection2 = re.compile("^"+_street_match_w_addr+_and_str+
                             (_street_match_w_addr).ordinal(2)+
                             _opt_place_in_paren+"$", re.IGNORECASE)
_opt_place_end = ReText(r"(?<=\s)((?!(?<=\s)("+"|".join(STREET_NAMES)+r")\s)[a-z ])+", 'PlaceName', delims=['$'])
_p_intersection4 = re.compile("^"+_street_match_w_addr+_and_str+
                             (_street_match_w_addr).ordinal(2)+r",\s*"+
                             _opt_place_end+"$", re.IGNORECASE)
_int_type = ReText("transition from", "IntersectionType")
_directions_expanded = list(usaddress.DIRECTIONS.copy())
_directions_expanded.extend(['northbound','southbound','eastbound','westbound',r'n\s*/?\s*b',r'w\s*/?\s*b',r'w\s*/?\s*b',r'e\s*/?\s*b'])
_dir2 = ReText([_directions_expanded], 'Direction')
_opt_place2 = ReText(r"(?<=\s)[a-z ]+", 'PlaceName')
_p_intersection3 = re.compile("^"+_street_match+_int_type+_dir2+_street_match.ordinal(2)+_opt_place2+"$", re.IGNORECASE)

_corners = ['northeast','northwest','southeast','southwest',r'n\s*/?\s*e',r'n\s*/?\s*w',r's\s*/?\s*e',r's\s*/?\s*w']
_dir3 = ReText([_corners], 'Direction')
_int_type2 = ReText("corner", "IntersectionType")
_p_intersection5 = re.compile("^"+_dir3+_int_type2+_of+_street_match_w_addr+_and_str+
                             (_street_match_w_addr).ordinal(2)+"$", re.IGNORECASE)

occ_delims = _default_delims.copy()
occ_delims.append(",")
occ_look = occ_delims.copy()
occ_look.extend([r"#",r"\d"])
_opt_occupancy_type = ReText([['APT','apartment']], 'OccupancyType',opt=True,lookahead=occ_look)
_opt_occupancy_id = ReText(r"#?\s?(?<=(APT\s|.APT|..[\sT]#|T\s#\s))[a-z]?\-?\d+", 'OccupancyIdentifier', opt=True)
_p_address = re.compile("^"+_opt_building_name+_opt_address_num+_opt_building_name.ordinal(2)+_street_match+
                        _opt_occupancy_type+_opt_occupancy_id+
                        _opt_place_line+"$", re.IGNORECASE)
_p_address2 = re.compile("^"+_opt_building_name+_street_match_w_addr+
                        _opt_occupancy_type+_opt_occupancy_id+
                        _opt_place_in_paren+"$", re.IGNORECASE)
_opt_place3 = ReText(r"(?<=\s)[a-z]+", 'PlaceName')
_p_address3 = re.compile("^"+_opt_building_name+_opt_address_num+_opt_building_name.ordinal(2)+_street_match+
                        _opt_occupancy_type+_opt_occupancy_id+r",\s*"+
                        _opt_place3+"$", re.IGNORECASE)

_address_ambiguous = ReText("space [\da-z]+", 'Ambiguous')
_p_address_w_ambiguous = re.compile("^"+_opt_address_num+_street_match+_address_ambiguous+"$",re.IGNORECASE)

_address_ambiguous2 = ReText(".+", 'Ambiguous')
_p_street_plus_ambiguous = re.compile('^'+_street_match+", at"+_address_ambiguous2+"$", re.IGNORECASE)

_multiple_address = re.compile(r"Location #\d+\s"+_street_match_w_addr, re.IGNORECASE)

_p_county = re.compile("^"+ReText(r"[a-z]+\s*[a-z]*\sCounty", 'PlaceName')+"$", re.IGNORECASE)
_p_unknown = re.compile("^"+ReText(r"unknown\b.+",'Unknown')+"$", re.IGNORECASE)

_latitude = ReText([r"\-?", [str(x) for x in range(0,91)], r'\.\d+'], 'Latitude')
_longitude = ReText([r"\-?", [str(x) for x in range(0,181)], r'\.\d+'], 'Longitude')
_p_coords = re.compile("^"+_latitude+_longitude+"$", re.IGNORECASE)


def _clean_groupdict(m):
    m = m.groupdict()
    s = dir = None
    for k,v in m.items():
        if 'Directional' in k:
            dir = (k,v)
            s = None
        elif "PostType" in k:
            if  v==None and (dir!=None and dir[1]!=None) and (s!=None and s[1].lower() in STREET_NAMES):
                # Street name is a direction which fooled the regular expression
                m[k] = s[1]
                m[s[0]] = dir[1]
                m[dir[0]] = None
            s = dir = None
        elif "StreetName" in k:
            s = (k,v)
        else:
            s = dir = None

        if v and k=='SecondBuildingName':
            if 'BuildingName' in m and m['BuildingName']:
                raise NotImplementedError()
            m['BuildingName'] = v
            m[k] = None

    return OrderedDict({k:v for k,v in m.items() if v is not None})


def _address_search(p, x):
    if m:=p.search(x):
        m = _clean_groupdict(m)

    return m

def _check_result(result, usa_result, col_name=None, address_string=None):
    if result==usa_result:
        return True
    if result[1]=='Intersection':
        if usa_result[1]=='Ambiguous' and 'IntersectionSeparator' in result[0]:
            # usaddress misintepretation of street intersection without post type
            # such as 'Columbia and North Fessenden'
            return True
        if 'StreetName' not in usa_result[0] or usa_result[0]['StreetName'].lower().startswith('and') or \
            result[0]['IntersectionSeparator']=='at' or \
            (result[0]['IntersectionSeparator']=='/' and ('IntersectionSeparator' not in usa_result[0] or usa_result[0]['IntersectionSeparator']!='/')) or \
            (usa_result[1]=='Intersection' and 'SecondStreetName' not in usa_result[0] and 'Recipient' in usa_result[0]):
            # usaddress missed street OR
            # usaddress included separator in street name OR
            # usaddress does not recognize / as separator
            return True
        # Check if usaddress included direction in street name
        dir = None
        for k,v in result[0].items():
            if k in usa_result[0] and usa_result[0][k] in [v, "("+v+")"]:
                pass
            elif "Directional" in k and k not in usa_result[0]:
                dir = v
                continue
            elif "StreetName" in k and dir!=None and usa_result[0][k]==dir+" "+v:  # Check if direction included with street
                pass
            else:
                return False
            dir = None

        return True
    elif result[1]=='Street Address':
        if (usa_result[1]=='Ambiguous' and 'Recipient' in usa_result[0] and usa_result[0]['Recipient'].endswith('Hwy')) or \
            (usa_result[1]=='Ambiguous' and list(usa_result[0].keys()) == ['Recipient'] and \
                col_name and col_name.lower() in ['street','street name'] and list(result[0].keys()) == ['StreetName']) or \
            any([x in usa_result[0] and '\n' in usa_result[0][x] for x in ["StreetName",'PlaceName','StreetNamePostType','OccupancyIdentifier']]) or \
            (all([x in usa_result[0].values() for x in result[0].values()]) and list(usa_result[0].keys())[-1]=='OccupancyIdentifier') or \
            (list(usa_result[0].keys())==['BuildingName'] and all([x in result[0].keys() for x in ["AddressNumber",'StreetName','StreetNamePostType']])) or \
            ('StateName' in usa_result[0] and usa_result[0]['StateName'] in ['INN', 'INN)']) or \
            ("Ambiguous" in result[0] and result[0]['Ambiguous'].lower().startswith('space')):
            # usa_address unable to get President George Bush Hwy
            # usa_address has trouble with \n
            return True
        skip_state = False
        skip_next = False
        for k,v in result[0].items():
            if skip_next:
                skip_next = False
                continue
            # usaddress might include newline character as part of name
            k_usa = "Recipient" if k=='BuildingName' else k
            k_usa = 'Recipient' if k_usa=='PlaceName' and 'PlaceName' not in usa_result[0] else k_usa
            if k_usa not in usa_result[0] or usa_result[0][k_usa] not in [v, v+'\n', "("+v+")"]:
                if k=="PlaceName" and k in usa_result[0] and "StateName" in result[0] and \
                    "StateName" not in usa_result[0] and usa_result[0][k]==v+" "+result[0]['StateName']:
                    # usaddress included state with place name
                    skip_state = True
                elif (skip_state and k=="StateName") or \
                    (k=='OccupancyIdentifier' and k in usa_result[0] and v.replace("#","# ")==usa_result[0][k]):
                    pass
                elif k.endswith("StreetName") and k in usa_result[0] and (m:=k+"PreDirectional") in usa_result[0] and \
                    m not in result[0] and usa_result[0][m].lower() not in usaddress.DIRECTIONS and \
                    v==usa_result[0][m]+" "+usa_result[0][k]:  # Non-directional value marked as directional by usaddress
                    pass
                elif k.endswith("StreetName") and k in usa_result[0] and usa_result[0][k]!= v and v.startswith(usa_result[0][k]) and \
                    (m:=k+"PostType") in result[0] and m in usa_result[0] and usa_result[0][m].lower() not in STREET_NAMES and \
                    (v+" "+result[0][m]).replace(usa_result[0][k]+" ","") == usa_result[0][m]:
                    # usaddress put part of street name in post type
                    skip_next = True
                else:
                    return False
            
        return True
    else:
        raise NotImplementedError()


def tag(address_string, col_name, error='raise'):
    assert error in ['raise','ignore']
    if pd.isnull(address_string):
        return ({}, "Null")
    
    # Ensure that /'s are surrounded by spaces
    address_string = address_string.strip().replace("/"," / ")
    m1=m2=m3=m4=m5=None
    if m1:=_address_search(_p_unknown, address_string):
        return (m1, 'Null')
    if (m1:=_address_search(_p_block, address_string)) or \
        (m2:=_address_search(_p_block2, address_string)) or \
        (m3:=_address_search(_p_block3, address_string)):
        return _get_address_result([m1,m2,m3], "Block Address")
    elif (m1:=_address_search(_p_directional, address_string)):
        return (m1, "StreetDirectional")
    elif (m1:=_address_search(_p_intersection, address_string)) or \
        (m2:=_address_search(_p_intersection2, address_string)) or \
        (m3:=_address_search(_p_intersection3, address_string)) or \
        (m4:=_address_search(_p_intersection4, address_string)) or \
        (m5:=_address_search(_p_intersection5, address_string)):
        return _get_address_result([m1,m2,m3,m4,m5], "Intersection", address_string)
    elif m1:=_address_search(_p_county, address_string):
        return (m1, "County")
    elif (m1:=_address_search(_p_bridge, address_string)):
        return _get_address_result(m1, "Bridge", address_string, type_check='Ambiguous')
    elif (m1:=_address_search(_p_building, address_string)):
        return _get_address_result(m1, "Building", address_string, type_check='Ambiguous')
    elif (m1:=_address_search(_p_address, address_string)) or \
        (m2:=_address_search(_p_address2, address_string)) or \
        (m3:=_address_search(_p_address_w_ambiguous, address_string)) or \
        (m4:=_address_search(_p_address3, address_string)):
        return _get_address_result([m1,m2,m3,m4], "Street Address", address_string, col_name=col_name)
    elif m1:=[_clean_groupdict(x) for x in re.finditer(_multiple_address, address_string)]:
        results = [_get_address_result(x, "Street Address") for x in m1]
        return [x[0] for x in results], [x[1] for x in results]
    elif m1:=_address_search(_p_street_plus_ambiguous, address_string):
        return _get_address_result(m1, "Street Address", address_string, check_ambiguous=True)
    elif m1:=_address_search(_p_coords, address_string):
        return _get_address_result(m1, "Coordinates")
    else:
        raise NotImplementedError()
    
def _get_address_result(results, name, address_string=None, type_check=None, col_name=None, check_ambiguous=False):
    if not isinstance(results, list):
        results = [results]
    for r in results:
        if r:
            result = [r, name]
            break

    if address_string or type_check:     
        try:
            usa_result = usaddress.tag(address_string)
        except usaddress.RepeatedLabelError as e:
            return result
        except:
            raise
        if type_check:
            if usa_result[1]!=type_check:
                raise NotImplementedError()
        elif check_ambiguous and usa_result[1]=='Ambiguous':
            pass
        elif address_string and not _check_result(result, usa_result, col_name, address_string):
            raise NotImplementedError()
    return result