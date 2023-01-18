from datetime import timedelta

import pandas as pd
import yaml

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    PushSource,
    RequestSource,
    SnowflakeSource,
)

from feast.types import Float64, Int64, Bool, String

# Define an entity for the customer. You can think of an entity as a primary key used to
# fetch features.
customer = Entity(name="customer", join_keys=["CUSTOMERID"])

# Defines a data source from which feature values can be retrieved. Sources are queried when building training
# datasets or materializing features into an online store.
project_name = yaml.safe_load(open("feature_store.yaml"))["project"]

cust_demographics_source = SnowflakeSource(
    # The Snowflake table where features can be found
    database=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"],
    table=f"DEMOGRAPHICS",
    # The event timestamp is used for point-in-time joins and for ensuring only
    # features within the TTL are returned
    timestamp_field="EVENT_TIMESTAMP",
    # The (optional) created timestamp is used to ensure there are no duplicate
    # feature rows in the offline store or when building training datasets
    created_timestamp_column="CREATED",
)

# Feature views are a grouping based on how features are stored in either the
# online or offline store.
demographics_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="cust_demographics",
    # The list of entities specifies the keys required for joining or looking
    # up features from this feature view. The reference provided in this field
    # correspond to the name of a defined entity (or entities)
    entities=[customer],
    # The timedelta is the maximum age that each feature value may have
    # relative to its lookup time. For historical features (used in training),
    # TTL is relative to each timestamp provided in the entity dataframe.
    # TTL also allows for eviction of keys from online stores and limits the
    # amount of historical scanning required for historical feature values
    # during retrieval
    ttl=timedelta(weeks=52 * 10),  # Set to be very long for example purposes only
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="GENDER", dtype=String),
        Field(name="SENIORCITIZEN", dtype=Bool),
        Field(name="PARTNER", dtype=Bool),
        Field(name="DEPENDENTS", dtype=Bool)
    ],
    source=cust_demographics_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"source": "customer_demographics"},
)

cust_services_source = SnowflakeSource(
    database=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"],
    table=f"SERVICES",
    timestamp_field="EVENT_TIMESTAMP",
    created_timestamp_column="CREATED",
)

services_fv = FeatureView(
    name="cust_services",
    entities=[customer],
    ttl=timedelta(weeks=52 * 10),
    schema=[
        Field(name="PHONESERVICE", dtype=String),
        Field(name="MULTIPLELINES", dtype=String),
        Field(name="INTERNETSERVICE", dtype=String),
        Field(name="ONLINESECURITY", dtype=String),
        Field(name="ONLINEBACKUP", dtype=String),
        Field(name="DEVICEPROTECTION", dtype=String),
        Field(name="TECHSUPPORT", dtype=String),
        Field(name="STREAMINGTV", dtype=String),
        Field(name="STREAMINGMOVIES", dtype=String),
        Field(name="CONTRACT", dtype=String),
        Field(name="PAPERLESSBILLING", dtype=String),
        Field(name="PAYMENTMETHOD", dtype=String),
        Field(name="MONTHLYCHARGES", dtype=Float64),
        Field(name="TOTALCHARGES", dtype=Float64),
        Field(name="TENUREMONTHS", dtype=Float64),
        Field(name="CHURNVALUE", dtype=Float64)
    ],
    source=cust_services_source,
    tags={"source": "customer_services"},
)


# A feature service is an object that represents a logical group of features from one or more feature views. 
# Feature Services allows features from within a feature view to be used as needed by an ML model. 
# Users can expect to create one feature service per model version, allowing for tracking of the features used by models.

customer_fs = FeatureService(
    name="customer_info",
    features=[demographics_fv, services_fv]
)
