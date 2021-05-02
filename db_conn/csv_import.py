import pandas as pd
from db_connect import db_conn

# CREATE TABLE claim_fraud_detection (months_as_customer integer, age integer, policy_number integer, policy_bind_date Date, policy_state NVARCHAR(MAX), policy_csl NVARCHAR(MAX), policy_deductable Integer, policy_annual_premium Numeric(17,2), umbrella_limit Integer, insured_zip Integer, insured_sex NVARCHAR(MAX), insured_education_level NVARCHAR(MAX), insured_occupation NVARCHAR(MAX), insured_hobbies NVARCHAR(MAX), insured_relationship NVARCHAR(MAX), capital_gains Integer, capital_loss Integer, incident_date Date, incident_type NVARCHAR(MAX), collision_type	NVARCHAR(MAX), incident_severity NVARCHAR(MAX), authorities_contacted NVARCHAR(MAX), incident_state NVARCHAR(MAX), incident_city NVARCHAR(MAX), incident_location NVARCHAR(MAX), incident_hour_of_the_day Integer, number_of_vehicles_involved	Integer, property_damage NVARCHAR(MAX), bodily_injuries Integer, witnesses Integer, police_report_available NVARCHAR(MAX), total_claim_amount Integer, injury_claim Integer, property_claim Integer, vehicle_claim Integer,auto_make NVARCHAR(MAX), auto_model NVARCHAR(MAX), auto_year Integer, fraud_reported NVARCHAR(MAX))

data = pd.read_csv("/home/vikesh/Documents/Data_science/claim_fraud_detection/insurance_claims.csv")

df=pd.DataFrame(data, columns = data.columns)


def import_csv():
    conn=db_conn()
    cursor=conn.cursor()
    for row in df.itertuples():
        cursor.execute('''
                INSERT INTO ds_datasets.dbo.claim_fraud_detection (months_as_customer , age , policy_number , policy_bind_date , policy_state , policy_csl , policy_deductable , policy_annual_premium , umbrella_limit , insured_zip , insured_sex , insured_education_level , insured_occupation , insured_hobbies , insured_relationship , capital_gains , capital_loss , incident_date , incident_type , collision_type	, incident_severity , authorities_contacted , incident_state , incident_city , incident_location , incident_hour_of_the_day , number_of_vehicles_involved	, property_damage , bodily_injuries , witnesses , police_report_available , total_claim_amount , injury_claim , property_claim , vehicle_claim ,auto_make , auto_model , auto_year , fraud_reported)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
                row.months_as_customer,
                row.age,
                row.policy_number,
                row.policy_bind_date,
                row.policy_state,
                row.policy_csl,
                row.policy_deductable,
                row.policy_annual_premium,
                row.umbrella_limit,
                row.insured_zip,
                row.insured_sex,
                row.insured_education_level,
                row.insured_occupation,
                row.insured_hobbies,
                row.insured_relationship,
                row.capital_gains,
                row.capital_loss,
                row.incident_date,
                row.incident_type,
                row.collision_type,
                row.incident_severity,
                row.authorities_contacted,
                row.incident_state,
                row.incident_city,
                row.incident_location,
                row.incident_hour_of_the_day,
                row.number_of_vehicles_involved,
                row.property_damage,
                row.bodily_injuries,
                row.witnesses,
                row.police_report_available,
                row.total_claim_amount,
                row.injury_claim,
                row.property_claim,
                row.vehicle_claim,
                row.auto_make,
                row.auto_model,
                row.auto_year,
                row.fraud_reported
                )
    conn.commit()
    return "Csv Imported" 

# print(import_csv())