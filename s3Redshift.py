import boto3
import os
import pandas as pd
import pandas_redshift as pr
import awswrangler as wr
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pathlib import Path
import ast
from warnings import filterwarnings

boto3.setup_default_session(region_name="ap-southeast-1")
session = boto3.Session()
load_dotenv()
filterwarnings("ignore", category = UserWarning, message = ".*pandas only supports SQLAlchemy connectable.*")



def create_wrangler_connection():
    return wr.redshift.connect(
        connection=os.getenv("redshift_prod_glue_name"),
        ssl=pr.connect_to_redshift(
            dbname=os.getenv("redshift_prod_dbname"),
            host=os.getenv("redshift_prod_host"),
            user=os.getenv("redshift_prod_user"),
            password=os.getenv("redshift_prod_password"),
        ),
    )


def create_sqlalchemy_connection():
    connection_str = f"""{os.getenv("redshift_prod_dbtype")}://{os.getenv("redshift_prod_user")}:{(os.getenv("redshift_prod_password")).replace('@','%40')}@{os.getenv("redshift_prod_host")}:{os.getenv("redshift_prod_port")}/{os.getenv("redshift_prod_dbname")}"""
    engine = create_engine(connection_str, isolation_level = "AUTOCOMMIT")
    conn = engine.connect()
    return engine, conn


def get_company_info():

    company_info_query = """select companyid, bbgcode,isincode,listingcountry from ntacommon.companyinfo"""
    
    try:
        engine,conn = create_sqlalchemy_connection()
        company_info_df = pd.read_sql_query(company_info_query, con=conn)
        company_info_df.columns = ["companyid", "bbgcode","isincode","country"]

    except Exception as e:
        print(e)
    finally:
        engine.dispose()

    return company_info_df

    
def write_to_s3(s3, file, path_to_S3):
    object = s3.Object('nta-broker-research', path_to_S3)
    result = object.put(Body=file)
    res = result.get('ResponseMetadata')
    if res.get('HTTPStatusCode') != 200:
        print('File Not Uploade`d')


def write_to_redshift(df, dtype, schema, table, primary_keys):
    wrconn = create_wrangler_connection()
    wr.redshift.copy(
        df = df,
        path = os.getenv("redshift_prod_bucketpath") + os.getenv("tmp_path"),
        con = wrconn,
        schema = schema,
        table = table,
        index = False,
        mode = "upsert",
        primary_keys = primary_keys,
        iam_role = os.getenv("redshift_prod_iam_role"),
        dtype = dtype,
    )


# this function returns dataframe, not integer companyid
def get_snowflake_companyid(conn, company, country):
    query = f"""
        select snowflake_companyid from public.tmp_snowflake_mapping map 
        left join ntacommon.companyinfo ci on nta_companyid = companyid
        where snowflake_companyid = {company} and listingcountry = '{country}';
    """
    df = pd.read_sql_query(query, con = conn.connection)
    return df["snowflake_companyid"]

# this function returns dataframe, not integer companyid
def get_missing_report_df(conn):
    start_date = os.getenv("start_date")
    missing_broker = os.getenv("missing_broker")
    if missing_broker == '':
        missing_broker = """('')"""
    query = f"""
                SELECT brt.*
                FROM report.broker_research_test brt
                LEFT JOIN report.broker_research br 
                ON brt.snowflake_companyid = br.snowflake_companyid 
                AND brt.versionid = br.versionid
                LEFT JOIN public.tmp_snowflake_mapping map
                ON brt.snowflake_companyid = map.snowflake_companyid
                LEFT JOIN ntacommon.companyinfo ci on map.nta_companyid = ci.companyid and brt.country = ci.listingcountry
                WHERE brt.contributor not in {missing_broker}
                and br.snowflake_companyid IS NULL and ((map.snowflake_companyid IS NOT NULL AND ci.companyid IS NOT NULL) OR brt.snowflake_companyid = -1)
                and brt.versionid not in (897914408, 1054671450, 1054663014) and brt.date_published >= '{start_date}';
            """
    df = pd.read_sql_query(query, con = conn.connection)
    return df


def get_company_list(conn, country):
    query = f"""
        select a.ciqticker, b.snowflake_companyid
        from ntacommon.companyinfo a
        left join public.tmp_snowflake_mapping b
        on a.companyid = b.nta_companyid
        where a.price_active = true and a.companytype = 'nonfin' and a.ciqticker not like 'D_CIQ%%' and snowflake_companyid is not null
        and a.listingcountry = '{country}';
    """
    df = pd.read_sql_query(query, con = conn.connection)
    return df["ciqticker"].to_list(), df["snowflake_companyid"].astype(int).to_list()


def allPdfS3(s3, dir, country, snowflake_companyid = -1):
    for file in Path(dir).glob("*.pdf"):
        path_to_S3 = f"{country}/{snowflake_companyid}/{file.name}"
        with open(file, "rb") as f:
            write_to_s3(s3, f, path_to_S3)
        file.unlink()


def removeCompany(conn, snowflake_companyid):
    query = f"""
        DELETE FROM report.company_track
        WHERE snowflake_companyid = {snowflake_companyid};
    """
    conn.execute(query)


def main():
    """
    This is typically how we start our processes that require looping through CapitalIQ companyids.
    """
    engine, conn = create_sqlalchemy_connection()
    wrconn = create_wrangler_connection()

    nta_query = """select b.snowflake_companyid, b.nta_companyid from ntacommon.companyinfo a
    left join public.tmp_snowflake_mapping b
    on a.companyid = b.nta_companyid
    where a.price_active = true and a.companytype = 'nonfin' and a.ciqticker not like 'D_CIQ%%' and snowflake_companyid is not null;"""
    df_nta = pd.read_sql_query(nta_query, con=conn.connection)
    companyid_list = [int(i) for i in list(df_nta["snowflake_companyid"])]
    # print(df_nta)
    # Loop...

if __name__ == "__main__":
    engine, conn = create_sqlalchemy_connection()
    ciq, sf = get_company_list(conn, "South Korea")
    boto3.setup_default_session(region_name="ap-southeast-1")
    session = boto3.Session()
    s3 = session.resource('s3')

    dtype = ast.literal_eval(os.getenv("dtype"))
    download_path = Path(os.getenv("download_path_daily"))
    csv_path = Path(download_path) / "BrokerResearch.csv"
    df = pd.read_csv(csv_path, encoding = "utf-16")
    write_to_redshift(df, dtype, "report", "broker_research", os.getenv("primary_keys").strip("][").split(", "))
    csv_path.unlink()
