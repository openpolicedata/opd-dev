import usaddress
import re
from collections import OrderedDict

road_re = "|".join(usaddress.STREET_NAMES)

def tag(address_string, col_name):
    try:
        result = usaddress.tag(address_string)
    except usaddress.RepeatedLabelError as e:
        throw = True
        if "/" in address_string:
            result = (OrderedDict(), "Intersection")
            add_second = False
            addon = ""
            for v,k in e.args[1]:
                if k in result[0]:
                    add_second = True
                    # Check if this value is after the separator
                    if address_string.find("/") > address_string.find(v):
                        raise
                    result[0]['IntersectionSeparator'] = '/'
                    addon = 'Second'

                if len(v)>1 and v.endswith("/"):
                    v = v[:-1]
                result[0][addon+k] =v

            if not add_second:
                raise
        else:
            raise
    except Exception:
        raise

    if result[1]=='Street Address' and result[0]['StreetName'].startswith('and '):
        m = re.search(r"^(?P<AddressNumber>[\dX]+\s)?(?P<StreetName>.+) (?P<StreetNamePostType>" + road_re + \
                      r") (and|&) (?P<SecondAddressNumber>[\dX]+\s)?(?P<SecondStreetName>.+) (?P<SecondStreetNamePostType>" + \
                      road_re + r")$", address_string, re.IGNORECASE)
        if m:
            return (OrderedDict(m.groupdict()), "Intersection")
        else:
            raise NotImplementedError()
    elif " and " in address_string or " & " in address_string or "/" in address_string:
        assert result[1] in ['Intersection']
    elif result[1] in ['Ambiguous']:
        if col_name.lower()=="street" and re.search(r"[A-Z]+", address_string, re.IGNORECASE):
            return (OrderedDict({'StreetName':address_string}), "Street Address")
        else:
            raise NotImplementedError()
    else:
        assert result[1] in ['Street Address']

    return result