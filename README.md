# **Clean, Test, and Model Data in Cloud Blob Storage to Create a Staging Area for Data to be Consumed by Downstream Business Applications**

*This repo is an example of how one could create a single source of truth within the data lake to be consumed by downstream business applications like CDPs, Data Warehouses, Customer Engagement Platforms, etc.* 

[Small Data Manifesto](https://motherduck.com/blog/small-data-manifesto/)

### Technologies 

- [DLT](https://dlthub.com/docs/intro): *DLT makes the L in ETL trivial with some pretty slick functionality baked in that you likely wouldn't think of when building one off ETL jobs*
- [DBT](https://docs.getdbt.com/docs/introduction): *DBT focuses on the T in ETL and makes it possible to bring analysts with domain knowledge into the data producing engineering workflow.*
- [DuckDB](https://duckdb.org/docs/api/python/overview): *Light-weight analytics database engine that is very efficient and super fast and makes local development and single machine deployments possible*

### Dataset

- **fullVisitorId** - A unique identifier for each user of the Google Merchandise Store.
  * *example*: np.int64(3162355547410993243)

- **visitId** - An identifier for this session. This is part of the value usually stored as the _utmb cookie. This is only unique to the user. For a completely unique ID, you should use a combination of fullVisitorId and visitId.

- **visitNumber** - The session number for this user. If this is the first session, then this is set to 1.

- **visitStartTime** - The timestamp (expressed as POSIX time).

- **channelGrouping** - The channel via which the user came to the Store.
  * *example*: 'Organic Search'

- **date** - The date on which the user visited the Store.
  * example*: 20170801

- **device** - The specifications for the device used to access the Store.

```
{
  "browser": "Chrome",
  "operatingSystem": "Windows",
  "isMobile": false,
  "deviceCategory": "desktop"
}
```

- **geoNetwork** - This section contains information about the geography of the user.

```
{
  "continent": "Americas",
  "subContinent": "Northern America",
  "country": "United States",
  "region": "California",
  "metro": "San Francisco-Oakland-San Jose",
  "city": "San Francisco"
}
```

- **socialEngagementType** - Engagement type, either "Socially Engaged" or "Not Socially Engaged".

- **totals** - This section contains aggregate values across the session.

```
{
  "visits": 1,
  "hits": 4,
  "pageviews": 3,
  "timeOnSite": 301,
  "bounces": 0,
  "transactions": 1,
  "transactionRevenue": 50000
}
```

- **trafficSource** - This section contains information about the Traffic Source from which the session originated.

```{
  "source": "google",
  "medium": "organic",
  "keyword": "(not provided)",
  "isTrueDirect": false
}
```

- **hits** - A nested json object containing the events captured during the session

```
[
 {
   "hitNumber": 1,
   "time": 0,
   "hour": 16, 
   "minute": 4,
   "isInteraction": true,
   "type": "PAGE",
   "page": {
     "pagePath": "/google+redesign/shop+by+brand/youtube",
     "hostname": "shop.googlemerchandisestore.com", 
     "pageTitle": "YouTube | Google Merchandise Store"
   },
   "eCommerceAction": {
     "action_type": "0",
     "step": 1
   }
 },
 {
   "hitNumber": 2,
   "time": 286,
   "hour": 16,
   "minute": 8, 
   "isInteraction": true,
   "type": "EVENT",
   "eventInfo": {
     "eventCategory": "Enhanced Ecommerce",
     "eventAction": "Product Click",
     "eventLabel": null
   }
 }
]
```