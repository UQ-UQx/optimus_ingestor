EDX_DATABASES = {
    'default': {'dbname': 'api', 'mongoname': 'UQx/Think101x/1T2014', 'icon': 'fa-settings'},
    'personcourse': {'dbname': 'Person_Course', 'icon': 'fa-settings'},
    'Course_Event': {'dbname': 'Course_Event', 'icon': 'fa-settings', 'year': ''},
    'think_101x_1T2014': {'dbname': 'UQx_Think101x_1T2014', 'mongoname': 'UQx/Think101x/1T2014', 'discussiontable': 'UQx-Think101x-1T2014-prod', 'icon': 'fa-heart', 'year': '2014'},
    'hypers_301x_1T2014': {'dbname': 'UQx_HYPERS301x_1T2014', 'mongoname': 'UQx/HYPERS301x/1T2014', 'discussiontable': 'UQx-HYPERS301x-1T2014-prod', 'icon': 'fa-plane', 'year': '2014'},
    'tropic_101x_1T2014': {'dbname': 'UQx_TROPIC101x_1T2014', 'mongoname': 'UQx/TROPIC101x/1T2014', 'discussiontable': 'UQx-TROPIC101x-1T2014-prod', 'icon': 'fa-tree', 'year': '2014'},
    'bioimg_101x_1T2014': {'dbname': 'UQx_BIOIMG101x_1T2014', 'mongoname': 'UQx/BIOIMG101x/1T2014', 'discussiontable': 'UQx-BIOIMG101x-1T2014-prod', 'icon': 'fa-desktop', 'year': '2014'},
    'crime_101x_3T2014': {'dbname': 'UQx_Crime101x_3T2014', 'mongoname': 'UQx/Crime101x/3T2014', 'discussiontable': 'UQx-Crime101x-3T2014-prod', 'icon': 'fa-gavel', 'year': '2014'},
    'world_101x_3T2014': {'dbname': 'UQx_World101x_3T2014', 'mongoname': 'UQx/World101x/3T2014', 'discussiontable': 'UQx-World101x-3T2014-prod', 'icon': 'fa-map-marker', 'year': '2014'},
    'write_101x_3T2014': {'dbname': 'UQx_Write101x_3T2014', 'mongoname': 'UQx/Write101x/3T2014', 'discussiontable': 'UQx-Write101x-3T2014-prod', 'icon': 'fa-pencil', 'year': '2014'},

    'hypers_301x_1T2015': {'dbname': 'UQx_HYPERS301x_1T2015', 'mongoname': 'UQx/HYPERS301x/1T2015', 'discussiontable': 'UQx-HYPERS301x-1T2015-prod', 'icon': 'fa-plane', 'year': '2015'},
    'bioimg_101x_1T2015': {'dbname': 'UQx_BIOIMG101x_1T2015', 'mongoname': 'UQx/BIOIMG101x/1T2015', 'discussiontable': 'UQx-BIOIMG101x-1T2015-prod', 'icon': 'fa-desktop', 'year': '2015'},
    'denial_101x_1T2015': {'dbname': 'UQx_Denial101x_1T2015', 'mongoname': 'UQx/Denial101x/1T2015', 'discussiontable': 'UQx-Denial101x-1T2015-prod', 'icon': 'fa-recycle', 'year': '2015'},
    'learn_101x_1T2015': {'dbname': 'UQx_Learn101x_1T2015', 'mongoname': 'UQx/Learn101x/1T2015', 'discussiontable': 'UQx-Learn101x-1T2015-prod', 'icon': 'fa-mortar-board', 'year': '2015'},
    'tropic_101x_2T2015': {'dbname': 'UQx_Tropic101_2T2015', 'mongoname': 'UQx/TROPIC101x/2T2015', 'discussiontable': 'UQx-TROPIC101x-2T2015-prod', 'icon': 'fa-tree', 'year': '2015'},
    'world_101x_2T2015': {'dbname': 'UQx_World101x_2T2015', 'mongoname': 'UQx/World101x/2T2015', 'discussiontable': 'UQx-World101x-2T2015-prod', 'icon': 'fa-map-marker', 'year': '2015'},
    'denial_101x_2T2015': {'dbname': 'UQx_Denial101x_2T2015', 'mongoname': 'UQx/Denial101x/2T2015', 'discussiontable': 'UQx-Denial101x-2T2015-prod', 'icon': 'fa-recycle', 'year': '2015'},
    'write_101x_2T2015': {'dbname': 'UQx_Write101x_2T2015', 'mongoname': 'UQx/Write101x/2T2015', 'discussiontable': 'UQx-Write101x-2T2015-prod', 'icon': 'fa-pencil', 'year': '2015'},

    'biol1020_2015_S1': {'dbname': 'UQx_BIOL1020_2015_S1', 'mongoname': 'UQx/BIOL1020/2015/S1', 'discussiontable': '', 'icon': 'fa-mortar-board', 'year': '2015'},

}

for DB in EDX_DATABASES:
    EDX_DATABASES[DB]['id'] = DB