EDX_DATABASES = {
    'default': {'dbname': 'api', 'mongoname': 'UQx/Think101x/1T2014', 'icon': 'fa-settings'},
    'personcourse': {'dbname': 'Person_Course', 'icon': 'fa-settings'},
    'think_101x_1T2014': {'dbname': 'UQx_Think101x_1T2014', 'mongoname': 'UQx/Think101x/1T2014', 'discussiontable': 'UQx-Think101x-1T2014-prod', 'icon': 'fa-heart'},
    'hypers_301x_1T2014': {'dbname': 'UQx_HYPERS301x_1T2014', 'mongoname': 'UQx/HYPERS301x/1T2014', 'discussiontable': 'UQx-HYPERS301x-1T2014-prod', 'icon': 'fa-plane'},
    'tropic_101x_1T2014': {'dbname': 'UQx_TROPIC101x_1T2014', 'mongoname': 'UQx/TROPIC101x/1T2014', 'discussiontable': 'UQx-TROPIC101x-1T2014-prod', 'icon': 'fa-tree'},
    'bioimg_101x_1T2014': {'dbname': 'UQx_BIOIMG101x_1T2014', 'mongoname': 'UQx/BIOIMG101x/1T2014', 'discussiontable': 'UQx-BIOIMG101x-1T2014-prod', 'icon': 'fa-desktop'},
    'crime_101x_3T2014': {'dbname': 'UQx_Crime101x_3T2014', 'mongoname': 'UQx/Crime101x/3T2014', 'discussiontable': 'UQx-Crime101x-3T2014-prod', 'icon': 'fa-gavel'},
    'world_101x_3T2014': {'dbname': 'UQx_World101x_3T2014', 'mongoname': 'UQx/World101x/3T2014', 'discussiontable': 'UQx-World101x-3T2014-prod', 'icon': 'fa-map-marker'},
    'write_101x_3T2014': {'dbname': 'UQx_Write101x_3T2014', 'mongoname': 'UQx/Write101x/3T2014', 'discussiontable': 'UQx-Write101x-3T2014-prod', 'icon': 'fa-pencil'},
    'sense_101x_3T2014': {'dbname': 'UQx_Sense101x_3T2014', 'mongoname': 'UQx/Sense101x/3T2014', 'discussiontable': 'UQx-Sense101x-3T2014-prod', 'icon': 'fa-power-off'},
}

for DB in EDX_DATABASES:
    EDX_DATABASES[DB]['id'] = DB