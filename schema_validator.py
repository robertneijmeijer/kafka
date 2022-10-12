from schema import Schema, SchemaError, Optional, And, Or

schema_val = {
    "name": str,
    "description": str,
    "status": str,

    "consumers": {
        "name": str,
        "description": str,
        "type" : str
    },

    "containers": {
        "name": str,
        "sysnonyms": str,
        "description": str,
        "technology": str,
        "parentSystem": str,
        "ciDataOwner": str,
        "productOwner": str,
        "applicationType": str,
        "hostedAt": str,
        "deploymentModel": str,
        "personalData": bool,
        "confidentiality": str,
        "mcv": str,
        "maxSeverityLevel": int,
        "sox": bool,
        "icfr": bool,
        "assignementGroup": str,
        "operationalStatus": str,
        "environments": str,
        "relationships": {
            "type": str,
            "container": {
                "name": str,
            },
        },
        "components": {
            "name": str,
            "description": str,
            "exposedAPIs": {
                "name": str,
                "description": str,
                "type": str,
                "status": str,
            },
            "consumedAPIs": {
                "name": str,
                "description": str,
                "status": str
            }
        },
    }
}