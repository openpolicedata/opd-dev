import pytest

# TODO: Streets that are directions or single letter
# '1100 Park Anue\nOrange Park, Florida'
# President George Bush Hwy
# 'SE 82nd Ave & SE Monterey (Clackamas County)'
# St Helens Way
@pytest.mark.parametrize('number', ['1','1234','34XX'])
def test_address():
    pass