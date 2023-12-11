import usaddress
import pandas as pd
import re
from collections import OrderedDict
from openpolicedata.defs import states as states_dict
from openpolicedata.utils import split_words

STREET_NAMES = usaddress.STREET_NAMES
STREET_NAMES.update(['la','bl','exwy'])

_states = list(states_dict.keys())
for v in states_dict.values():
    _states.append(v)

def find_address_col(df_test):
    addr_col = [x for x in df_test.columns if "LOCATION" in x.upper()]
    if len(addr_col):
        if len(addr_col)>1:
            raise NotImplementedError()
        tags = df_test[addr_col[0]].apply(lambda x: tag(x, addr_col[0], error='ignore'))
        tags = tags[tags.apply(lambda x: x[1]!='Null')]
        if tags.apply(lambda x: x[1]).isin(['Street Address','Intersection','Block Address', 'Street Name']).all():
            return addr_col
        else:
            return []
    addr_col = [x for x in df_test.columns if x.upper() in ["STREET"]]
    if len(addr_col):
        return addr_col
    addr_col = [x for x in df_test.columns if 'address' in split_words(x,case='lower')]
    return addr_col

_default_delims = ['^', r"\s", "$"]

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

        # explicitly only pass value to the str constructor
        self = super(ReText, cls).__new__(cls, value)
        self.opt = opt
        self.delims = delims
        return self

    def __add__(self, other):
        x = str(self)
        if isinstance(other, ReText):
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
    
    
_opt_place = ReText(r'[a-z]+', 'PlaceName', delims=[r'\s',','])
_opt_state = ReText([_states], 'StateName')
_opt_zip = ReText(r'\d{5}+', 'ZipCode', opt=True)
_opt_place_line = ReText("("+_opt_place+_opt_state+")",opt=True)+_opt_zip

post_type_delims = _default_delims.copy()
post_type_delims.append(',')
_block_num = ReText(r"\d+[0X]{2}", "BlockNumber")
_block_ind = ReText([[r"Block of",'BLK', 'block']], "BlockIndicator")
_opt_street_dir = ReText([usaddress.DIRECTIONS, r'\.?'], 'StreetNamePreDirectional', opt=True)
_street_name = ReText(r"[\w\s']+?", "StreetName")

_pre_type = r"("+ReText([_states])+r'(?=\s))?\s*'+str(ReText([STREET_NAMES, r'\.?']))
_opt_pre_type = r'\s*('+ReText(_pre_type, 'StreetNamePreType')+r"(?!\s("+"|".join(STREET_NAMES)+r")))?\s*"
_opt_post_type = ReText([STREET_NAMES, r'\.?'], 'StreetNamePostType', opt=True, delims=post_type_delims)
_opt_post_dir = ReText([usaddress.DIRECTIONS, r'\.?'], 'StreetNamePostDirectional', opt=True, delims=post_type_delims)
# post_type2 = ReText(r'\w+\.', 'StreetNamePostType', opt=False, delims=post_type_delims)

_p_block = re.compile("^"+_block_num+_block_ind+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir+_opt_place_line+"$", re.IGNORECASE)

_opt_address_num = ReText(r"[\dX]+", 'AddressNumber', opt=True)
and_str = ReText([['and',r'&', r'/']], 'IntersectionSeparator')
_p_intersection = re.compile("^"+_opt_address_num+_opt_pre_type+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir+and_str+
                             (_opt_address_num+_opt_pre_type+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir).ordinal(2)+
                             _opt_place_line+"$", re.IGNORECASE)

occ_delims = _default_delims.copy()
occ_delims.append(",")
occ_look = occ_delims.copy()
occ_look.extend([r"#",r"\d"])
_opt_occupancy_type = ReText('APT', 'OccupancyType',opt=True,lookahead=occ_look)
_opt_occupancy_id = ReText(r"#?(?<=(APT\s|.APT|..[\sT]#))[a-z]?\d+", 'OccupancyIdentifier', opt=True)
_p_address = re.compile("^"+_opt_address_num+_opt_pre_type+_opt_street_dir+_street_name+_opt_post_type+_opt_post_dir+_opt_occupancy_type+_opt_occupancy_id+
                        _opt_place_line+"$", re.IGNORECASE)
# _p_address2 = re.compile("^"+_opt_address_num+_opt_street_dir+_street_name+post_type2+_opt_post_dir+_opt_occupancy_type+_opt_place_line+"$", re.IGNORECASE)

_p_county = re.compile("^"+ReText(r"[a-z]+\s*[a-z]*\sCounty", 'PlaceName')+"$", re.IGNORECASE)

def _address_search(p, x):
    if m:=p.search(x):
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

        m = OrderedDict({k:v for k,v in m.items() if v is not None})

    return m

def _check_result(result, usa_result):
    if result==usa_result:
        return True
    if result[1]=='Intersection':
        if usa_result[1]=='Ambiguous' and 'IntersectionSeparator' in result[0]:
            # usaddress misintepretation of street intersection without post type
            # such as 'Columbia and North Fessenden'
            return True
        if usa_result[0]['StreetName'].lower().startswith('and') or \
            (result[0]['IntersectionSeparator']=='/' and usa_result[0]['IntersectionSeparator']!='/'):
            # usaddress included separator in street name OR
            # usaddress does not recognize / as separator
            return True
        # Check if usaddress included direction in street name
        dir = None
        for k,v in result[0].items():
            if k in usa_result[0] and v==usa_result[0][k]:
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
        if usa_result[1]=='Ambiguous' and 'Recipient' in usa_result[0] and usa_result[0]['Recipient'].endswith('Hwy'):
            # usa_address unable to get President George Bush Hwy
            return True
        skip_state = False
        skip_next = False
        for k,v in result[0].items():
            if skip_next:
                skip_next = False
                continue
            # usaddress might include newline character as part of name
            if k not in usa_result[0] or usa_result[0][k] not in [v, v+'\n']:
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
        return ((), ("Null"))
    
    # Ensure that /'s are surrounded by spaces
    address_string = address_string.strip().replace("/"," / ")
    if m:=_address_search(_p_block, address_string):
        return (m, "Block Address")
    elif m:=_address_search(_p_intersection, address_string):
        result = (m, "Intersection")
        try:
            try:
                usa_result = usaddress.tag(address_string)
            except usaddress.RepeatedLabelError as e:
                return result
        except:
            raise
        if not _check_result(result, usa_result):
            raise NotImplementedError()
        return result
    elif m:=_address_search(_p_county, address_string):
        return (m, "County")
    elif (m:=_address_search(_p_address, address_string)):# or \
        #(m2:=_address_search(_p_address2, address_string)):
        # if m:
        result = (m, "Street Address")
        # else:
        #     result = (m2, "Street Address")
        try:
            usa_result = usaddress.tag(address_string)
        except:
            raise

        if not _check_result(result, usa_result):
            if usa_result[1]=='Ambiguous' and col_name.lower() in ['street','street name']:
                return (result[0],'Street Name')
            raise NotImplementedError()
        return result
    else:
        raise NotImplementedError()