

  create or replace view `data-fellowship-9-project`.`final_project`.`partitioned__dates`
  OPTIONS()
  as 

  SELECT
    *
  FROM
    `data-fellowship-9-project`.`final_project`.`fact__tables`;

