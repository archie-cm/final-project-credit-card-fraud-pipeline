
  
    

    create or replace table `data-fellowship-9-project`.`final_project`.`fact__tables`
    
    
    OPTIONS()
    as (
      with fact__tables as (
    SELECT * 
    FROM `data-fellowship-9-project`.`final_project`.`stg__credit_card`
),
parameters AS (
  SELECT
    DATE '2020-01-01' start_date,
    DATE '2020-12-31' finish_date 
)

SELECT
    DATE_FROM_UNIX_DATE(CAST(start + (finish - start) * RAND() AS INT64)) AS DATE_APPLY,
    fact__tables.ID,
    ie.ID_EDUCATION,
    ih.ID_HOUSING,
    ii.ID_INCOME,
    ij.ID_JOB,
    im.ID_MARITAL,
    fact__tables.CODE_GENDER,
    fact__tables.FLAG_OWN_CAR,
    fact__tables.FLAG_OWN_REALTY,
    fact__tables.CNT_CHILDREN,
    fact__tables.AMT_INCOME_TOTAL,
    fact__tables.YEARS_BIRTH,
    fact__tables.YEARS_EMPLOYED,
    fact__tables.FLAG_MOBIL,
    fact__tables.FLAG_WORK_PHONE,
    fact__tables.FLAG_PHONE,
    fact__tables.FLAG_EMAIL,
    fact__tables.CNT_FAM_MEMBERS
FROM
    fact__tables, parameters, UNNEST([STRUCT(UNIX_DATE(start_date) AS start, UNIX_DATE(finish_date) AS finish)])
    INNER JOIN `data-fellowship-9-project`.`final_project`.`dim__EDUCATION` ie ON fact__tables.NAME_EDUCATION_TYPE = ie.NAME_EDUCATION_TYPE 
    INNER JOIN `data-fellowship-9-project`.`final_project`.`dim__HOUSING` ih ON fact__tables.NAME_HOUSING_TYPE = ih.NAME_HOUSING_TYPE
    INNER JOIN `data-fellowship-9-project`.`final_project`.`dim__INCOME` ii ON fact__tables.NAME_INCOME_TYPE = ii.NAME_INCOME_TYPE
    INNER JOIN `data-fellowship-9-project`.`final_project`.`dim__MARITAL` im ON fact__tables.NAME_FAMILY_STATUS = im.NAME_FAMILY_STATUS
    LEFT JOIN `data-fellowship-9-project`.`final_project`.`dim__JOB` ij ON fact__tables.OCCUPATION_TYPE = ij.OCCUPATION_TYPE
    );
  