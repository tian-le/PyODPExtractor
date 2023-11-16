# PyODPExtractor
## _Data acquisition from SAP ODP extractors via Python_

Traditionally, most SAP ECC/CRM/SRM users would use SAP BW as their analytical platform. However, it is a trend that organizations migrating their BW systems to modern cloud-based database platforms such as Snowflake, Redshift, etc.

To migrate data analytical platform, the first step would be extracting data from SAP transaction systems. BW extractors are used in SAP ECC/CRM/SRM systems to supply master and transaction data. However, BW extractors are not widely supported directly by other database systems. To extract data from SAP transaction systems, possible ways are:
- Use SAP SLT to replicate ECC tables to other databases\
Data source is changed from BW extractor to ECC tables. License is needed.
- Use SAP BusinessObjects DataServices which supports BW ODP extractors well\
Another platform is envolved. License is needed.

SAP offers ODP extractors APIs to third party system. The APIs are offered in form of RFC function modules. These APIs can be called by Java program via JCo. However, I write some Python scripts to get data from ODP extractors, and APIs are called via [PyRFC](https://github.com/SAP/PyRFC).

The platform migration is easier since ODP extractor would have same structure as BW extractor. Features like delta extraction is also supported by ODP extractors. Data extracted from ODP extractors can be saved in staging tables of third party database systems, just like the PSA tables in BW. Then we could focus on migrating ABAP based transformation rules to database scripts.