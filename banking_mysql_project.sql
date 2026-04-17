-- ============================================================
--   BANKING DATA ANALYST PROJECT  |  MySQL 
--   Dataset : bank clients with demographics,
--             financial products & loyalty data
--   Concepts : DDL · DML · Data Cleaning · EDA ·
--              Aggregations · JOINs · Subqueries ·
--              CASE · Date Functions · String Functions
-- ============================================================


-- ============================================================
-- SECTION 1 : DATABASE & TABLE SETUP (DDL)
-- ============================================================

CREATE DATABASE IF NOT EXISTS banking_project;
USE banking_project;

DROP TABLE IF EXISTS banking_clients;

CREATE TABLE banking_clients (
    client_id              VARCHAR(15)    PRIMARY KEY,
    full_name              VARCHAR(100)   NOT NULL,
    age                    INT,
    location_id            INT,
    joined_bank            DATE,
    banking_contact        VARCHAR(100),
    nationality            VARCHAR(50),
    occupation             VARCHAR(100),
    fee_structure          VARCHAR(10),          
    loyalty_class          VARCHAR(20),          
    estimated_income       DECIMAL(12,2),
    superannuation_savings DECIMAL(12,2),
    num_credit_cards       INT,
    credit_card_balance    DECIMAL(12,2),
    bank_loans             DECIMAL(14,2),
    bank_deposits          DECIMAL(14,2),
    checking_accounts      DECIMAL(14,2),
    saving_accounts        DECIMAL(14,2),
    foreign_currency_acct  DECIMAL(12,2),
    business_lending       DECIMAL(14,2),
    properties_owned       INT,
    risk_weighting         INT,                 
    br_id                  INT,
    gender_id              INT,                 
    ia_id                  INT
);


-- ============================================================
-- SECTION 2 : LOAD DATA
-- ============================================================

-- LOAD DATA LOCAL INFILE '/path/to/Banking.csv'
-- INTO TABLE banking_clients
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (client_id, full_name, age, location_id,
--  @joined_raw, banking_contact, nationality, occupation,
--  fee_structure, loyalty_class, estimated_income,
--  superannuation_savings, num_credit_cards, credit_card_balance,
--  bank_loans, bank_deposits, checking_accounts, saving_accounts,
--  foreign_currency_acct, business_lending, properties_owned,
--  risk_weighting, br_id, gender_id, ia_id)
-- SET joined_bank = STR_TO_DATE(@joined_raw, '%d-%m-%Y');


-- ============================================================
-- SECTION 3 : DATA CLEANING
-- ============================================================

--  Check for NULL values in key columns
SELECT
    SUM(CASE WHEN client_id        IS NULL THEN 1 ELSE 0 END) AS null_client_id,
    SUM(CASE WHEN full_name        IS NULL THEN 1 ELSE 0 END) AS null_name,
    SUM(CASE WHEN age              IS NULL THEN 1 ELSE 0 END) AS null_age,
    SUM(CASE WHEN estimated_income IS NULL THEN 1 ELSE 0 END) AS null_income,
    SUM(CASE WHEN loyalty_class    IS NULL THEN 1 ELSE 0 END) AS null_loyalty,
    SUM(CASE WHEN fee_structure    IS NULL THEN 1 ELSE 0 END) AS null_fee
FROM banking_clients;


--  Check for duplicate client IDs
SELECT client_id, COUNT(*) AS cnt
FROM banking_clients
GROUP BY client_id
HAVING cnt > 1;


--  Check for invalid age values
SELECT COUNT(*) AS invalid_age_count
FROM banking_clients
WHERE age < 18 OR age > 100;


-- Check for negative financial figures
SELECT COUNT(*) AS negative_values
FROM banking_clients
WHERE estimated_income < 0
   OR bank_loans < 0
   OR bank_deposits < 0;


-- Check distinct values in categorical columns
SELECT DISTINCT fee_structure  FROM banking_clients;
SELECT DISTINCT loyalty_class  FROM banking_clients;
SELECT DISTINCT nationality    FROM banking_clients;
SELECT DISTINCT risk_weighting FROM banking_clients ORDER BY risk_weighting;


-- Trim whitespace in text columns
UPDATE banking_clients
SET full_name     = TRIM(full_name),
    nationality   = TRIM(nationality),
    occupation    = TRIM(occupation),
    fee_structure = TRIM(fee_structure);


-- Standardise fee_structure to proper casing (High / Mid / Low)
UPDATE banking_clients
SET fee_structure = CONCAT(
    UPPER(SUBSTRING(fee_structure, 1, 1)),
    LOWER(SUBSTRING(fee_structure, 2))
);


-- Add a readable gender column
ALTER TABLE banking_clients
ADD COLUMN gender VARCHAR(10);

UPDATE banking_clients
SET gender = CASE gender_id
                 WHEN 1 THEN 'Male'
                 WHEN 2 THEN 'Female'
                 ELSE 'Unknown'
             END;


--  Add an age_group column for easier segmentation
ALTER TABLE banking_clients
ADD COLUMN age_group VARCHAR(10);

UPDATE banking_clients
SET age_group = CASE
    WHEN age BETWEEN 18 AND 25 THEN '18-25'
    WHEN age BETWEEN 26 AND 35 THEN '26-35'
    WHEN age BETWEEN 36 AND 45 THEN '36-45'
    WHEN age BETWEEN 46 AND 55 THEN '46-55'
    WHEN age BETWEEN 56 AND 65 THEN '56-65'
    ELSE '65+'
END;


--  Add a wealth_score column
ALTER TABLE banking_clients
ADD COLUMN wealth_score DECIMAL(16,2);

UPDATE banking_clients
SET wealth_score = ROUND(
    bank_deposits + saving_accounts + superannuation_savings
    - bank_loans - credit_card_balance, 2
);


-- ============================================================
-- SECTION 4 : EXPLORATORY DATA ANALYSIS (EDA)
-- ============================================================

-- Total records and basic overview
SELECT
    COUNT(*)                        AS total_clients,
    COUNT(DISTINCT nationality)     AS distinct_nationalities,
    COUNT(DISTINCT occupation)      AS distinct_occupations,
    COUNT(DISTINCT loyalty_class)   AS distinct_loyalty_tiers,
    MIN(joined_bank)                AS earliest_join,
    MAX(joined_bank)                AS latest_join
FROM banking_clients;


--  Age summary statistics
SELECT
    MIN(age)                AS min_age,
    MAX(age)                AS max_age,
    ROUND(AVG(age), 1)      AS avg_age,
    ROUND(STDDEV(age), 1)   AS stddev_age
FROM banking_clients;


-- Client count by age group
SELECT age_group,
       COUNT(*)                        AS total_clients,
       ROUND(AVG(estimated_income), 2) AS avg_income
FROM banking_clients
GROUP BY age_group
ORDER BY age_group;


-- Gender distribution with percentage
SELECT gender,
       COUNT(*)                                              AS total,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM banking_clients), 2) AS pct
FROM banking_clients
GROUP BY gender;


-- Nationality breakdown
SELECT nationality,
       COUNT(*)                        AS total_clients,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(bank_loans), 2)       AS avg_loans,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits
FROM banking_clients
GROUP BY nationality
ORDER BY total_clients DESC;


--  Fee structure distribution
SELECT fee_structure,
       COUNT(*)                        AS clients,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits,
       ROUND(AVG(risk_weighting), 2)   AS avg_risk
FROM banking_clients
GROUP BY fee_structure
ORDER BY avg_income DESC;


-- Loyalty classification breakdown
SELECT loyalty_class,
       COUNT(*)                        AS clients,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits,
       ROUND(AVG(risk_weighting), 2)   AS avg_risk
FROM banking_clients
GROUP BY loyalty_class
ORDER BY avg_income DESC;


-- Income distribution summary
SELECT
    ROUND(MIN(estimated_income), 2)    AS min_income,
    ROUND(MAX(estimated_income), 2)    AS max_income,
    ROUND(AVG(estimated_income), 2)    AS avg_income,
    ROUND(STDDEV(estimated_income), 2) AS stddev_income
FROM banking_clients;


--  Credit card usage overview
SELECT
    ROUND(AVG(num_credit_cards), 2)    AS avg_cards,
    MAX(num_credit_cards)              AS max_cards,
    ROUND(AVG(credit_card_balance), 2) AS avg_cc_balance,
    ROUND(MAX(credit_card_balance), 2) AS max_cc_balance
FROM banking_clients;


-- Risk weighting distribution
SELECT risk_weighting,
       COUNT(*)                        AS clients,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(bank_loans), 2)       AS avg_loans
FROM banking_clients
GROUP BY risk_weighting
ORDER BY risk_weighting;


-- ============================================================
-- SECTION 5 : FILTERING & CONDITIONAL QUERIES
-- ============================================================

--  Top 10 highest income clients
SELECT client_id, full_name, occupation, nationality,
       estimated_income, loyalty_class
FROM banking_clients
ORDER BY estimated_income DESC
LIMIT 10;


--  Clients with zero bank loans
SELECT client_id, full_name, nationality, estimated_income, loyalty_class
FROM banking_clients
WHERE bank_loans = 0
ORDER BY estimated_income DESC;


--  Clients with high risk (risk_weighting = 4 or 5)
SELECT client_id, full_name, nationality, risk_weighting,
       bank_loans, credit_card_balance, loyalty_class
FROM banking_clients
WHERE risk_weighting IN (4, 5)
ORDER BY risk_weighting DESC, bank_loans DESC;


-- Clients who joined in the last 10 years
SELECT client_id, full_name, joined_bank, nationality,
       estimated_income, loyalty_class
FROM banking_clients
WHERE joined_bank >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR)
ORDER BY joined_bank DESC;


-- Financial stress flag: CC balance > 20% of income
SELECT client_id, full_name, estimated_income, credit_card_balance,
       ROUND(credit_card_balance / estimated_income * 100, 2) AS cc_pct_of_income
FROM banking_clients
WHERE (credit_card_balance / estimated_income) > 0.20
ORDER BY cc_pct_of_income DESC;


-- Clients with multiple credit cards AND high CC balance
SELECT client_id, full_name, num_credit_cards,
       credit_card_balance, estimated_income, risk_weighting
FROM banking_clients
WHERE num_credit_cards > 1
  AND credit_card_balance > 5000
ORDER BY credit_card_balance DESC;


--  Foreign currency account holders
SELECT client_id, full_name, nationality,
       foreign_currency_acct, estimated_income
FROM banking_clients
WHERE foreign_currency_acct > 0
ORDER BY foreign_currency_acct DESC
LIMIT 15;


-- ============================================================
-- SECTION 6 : AGGREGATION & GROUP BY ANALYSIS
-- ============================================================

--  Total and average deposits by loyalty class
SELECT loyalty_class,
       COUNT(*)                        AS clients,
       ROUND(SUM(bank_deposits), 2)    AS total_deposits,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits
FROM banking_clients
GROUP BY loyalty_class
ORDER BY total_deposits DESC;


--  Average loan-to-income ratio by nationality
SELECT nationality,
       COUNT(*)                                                    AS clients,
       ROUND(AVG(bank_loans / NULLIF(estimated_income, 0)), 2)    AS avg_loan_to_income
FROM banking_clients
GROUP BY nationality
ORDER BY avg_loan_to_income DESC;


--  Properties owned: wealth and risk profile
SELECT properties_owned,
       COUNT(*)                        AS clients,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits,
       ROUND(AVG(bank_loans), 2)       AS avg_loans,
       ROUND(AVG(risk_weighting), 2)   AS avg_risk
FROM banking_clients
GROUP BY properties_owned
ORDER BY properties_owned;


--  Banking contact workload
SELECT banking_contact,
       COUNT(*)                        AS num_clients,
       ROUND(AVG(estimated_income), 2) AS avg_client_income,
       ROUND(AVG(risk_weighting), 2)   AS avg_risk
FROM banking_clients
GROUP BY banking_contact
ORDER BY num_clients DESC
LIMIT 10;


-- Year-wise new client acquisition
SELECT YEAR(joined_bank)       AS join_year,
       COUNT(*)                AS new_clients,
       ROUND(AVG(estimated_income), 2) AS avg_income
FROM banking_clients
GROUP BY join_year
ORDER BY join_year;


--  Business lending by nationality
SELECT nationality,
       COUNT(*)                            AS clients_with_biz_lending,
       ROUND(SUM(business_lending), 2)     AS total_biz_lending,
       ROUND(AVG(business_lending), 2)     AS avg_biz_lending
FROM banking_clients
WHERE business_lending > 0
GROUP BY nationality
ORDER BY total_biz_lending DESC;


--  Average CC balance as % of income by age group
SELECT age_group,
       COUNT(*)                                                           AS clients,
       ROUND(AVG(credit_card_balance / NULLIF(estimated_income,0) * 100), 2) AS avg_cc_pct
FROM banking_clients
GROUP BY age_group
ORDER BY age_group;


--  Top 5 occupations by average wealth score
SELECT occupation,
       COUNT(*)                          AS clients,
       ROUND(AVG(wealth_score), 2)       AS avg_wealth_score,
       ROUND(AVG(estimated_income), 2)   AS avg_income
FROM banking_clients
GROUP BY occupation
HAVING clients >= 5
ORDER BY avg_wealth_score DESC
LIMIT 5;


-- ============================================================
-- SECTION 7 : SUBQUERIES  
-- ============================================================

-- Clients earning above the overall average income
SELECT client_id, full_name, nationality,
       estimated_income, loyalty_class
FROM banking_clients
WHERE estimated_income > (SELECT AVG(estimated_income) FROM banking_clients)
ORDER BY estimated_income DESC;


--  Clients with deposits above the overall average
SELECT client_id, full_name, bank_deposits, loyalty_class
FROM banking_clients
WHERE bank_deposits > (SELECT AVG(bank_deposits) FROM banking_clients)
ORDER BY bank_deposits DESC;


-- Clients whose loans exceed the average loan amount
SELECT client_id, full_name, bank_loans, nationality, risk_weighting
FROM banking_clients
WHERE bank_loans > (SELECT AVG(bank_loans) FROM banking_clients)
ORDER BY bank_loans DESC;


-- High income AND high risk clients
--      (income in top 25% by finding the cutoff with subquery)
SELECT client_id, full_name, estimated_income, risk_weighting, loyalty_class
FROM banking_clients
WHERE estimated_income > (
        SELECT estimated_income
        FROM banking_clients
        ORDER BY estimated_income DESC
        LIMIT 1 OFFSET 749         -- approx top 25% of 3000 rows
      )
  AND risk_weighting >= 4
ORDER BY estimated_income DESC;


-- Nationality with the highest average income (subquery in FROM)
SELECT nationality, avg_income
FROM (
    SELECT nationality,
           ROUND(AVG(estimated_income), 2) AS avg_income
    FROM banking_clients
    GROUP BY nationality
) AS nat_avg
ORDER BY avg_income DESC
LIMIT 1;


--  Loyalty classes that have more than 700 clients
SELECT loyalty_class, total_clients
FROM (
    SELECT loyalty_class, COUNT(*) AS total_clients
    FROM banking_clients
    GROUP BY loyalty_class
) AS loyalty_counts
WHERE total_clients > 700;


-- ============================================================
-- SECTION 8 : CASE STATEMENTS & DERIVED LABELS
-- ============================================================

--  Wealth category label based on wealth_score
SELECT client_id, full_name, wealth_score,
       CASE
           WHEN wealth_score >= 2000000  THEN 'Ultra Wealthy'
           WHEN wealth_score >= 1000000  THEN 'Wealthy'
           WHEN wealth_score >= 500000   THEN 'Comfortable'
           WHEN wealth_score >= 0        THEN 'Moderate'
           ELSE 'In Debt'
       END AS wealth_category
FROM banking_clients
ORDER BY wealth_score DESC;


-- Loan risk label
SELECT client_id, full_name, bank_loans, estimated_income,
       CASE
           WHEN bank_loans = 0                                        THEN 'No Loan'
           WHEN bank_loans / NULLIF(estimated_income,0) < 3          THEN 'Low Risk Loan'
           WHEN bank_loans / NULLIF(estimated_income,0) BETWEEN 3 AND 6 THEN 'Medium Risk Loan'
           ELSE 'High Risk Loan'
       END AS loan_risk_label
FROM banking_clients
ORDER BY bank_loans DESC;


-- Client tenure category
SELECT client_id, full_name, joined_bank,
       TIMESTAMPDIFF(YEAR, joined_bank, CURDATE()) AS years_with_bank,
       CASE
           WHEN TIMESTAMPDIFF(YEAR, joined_bank, CURDATE()) < 3  THEN 'New'
           WHEN TIMESTAMPDIFF(YEAR, joined_bank, CURDATE()) < 8  THEN 'Established'
           ELSE 'Long-term'
       END AS tenure_category
FROM banking_clients
ORDER BY years_with_bank DESC;


-- Count clients by wealth category
SELECT
    CASE
        WHEN wealth_score >= 2000000 THEN 'Ultra Wealthy'
        WHEN wealth_score >= 1000000 THEN 'Wealthy'
        WHEN wealth_score >= 500000  THEN 'Comfortable'
        WHEN wealth_score >= 0       THEN 'Moderate'
        ELSE 'In Debt'
    END AS wealth_category,
    COUNT(*) AS clients
FROM banking_clients
GROUP BY wealth_category
ORDER BY clients DESC;


-- ============================================================
-- SECTION 9 : DATE FUNCTIONS
-- ============================================================

-- How many years each client has been with the bank
SELECT client_id, full_name, joined_bank,
       TIMESTAMPDIFF(YEAR, joined_bank, CURDATE()) AS years_with_bank
FROM banking_clients
ORDER BY years_with_bank DESC;


-- Clients who joined each month (seasonality check)
SELECT MONTHNAME(joined_bank)  AS join_month,
       MONTH(joined_bank)      AS month_num,
       COUNT(*)                AS new_clients
FROM banking_clients
GROUP BY join_month, month_num
ORDER BY month_num;


--  Clients who joined in the last 5 years vs older — comparison
SELECT
    CASE
        WHEN YEAR(joined_bank) >= YEAR(CURDATE()) - 5 THEN 'Last 5 Years'
        ELSE 'Before Last 5 Years'
    END                                AS cohort,
    COUNT(*)                           AS clients,
    ROUND(AVG(estimated_income), 2)    AS avg_income,
    ROUND(AVG(bank_deposits), 2)       AS avg_deposits,
    ROUND(AVG(risk_weighting), 2)      AS avg_risk
FROM banking_clients
GROUP BY cohort;


-- Most recent 20 clients to join
SELECT client_id, full_name, joined_bank, nationality, loyalty_class
FROM banking_clients
ORDER BY joined_bank DESC
LIMIT 20;


-- ============================================================
-- SECTION 10 : STRING FUNCTIONS
-- ============================================================

-- Extract first name and last name
SELECT client_id,
       full_name,
       SUBSTRING_INDEX(full_name, ' ', 1)   AS first_name,
       SUBSTRING_INDEX(full_name, ' ', -1)  AS last_name
FROM banking_clients
LIMIT 20;


-- Clients whose name starts with a specific letter (e.g. 'A')
SELECT client_id, full_name, nationality, estimated_income
FROM banking_clients
WHERE full_name LIKE 'A%'
ORDER BY full_name;


--  Find occupation keywords (e.g. all engineers)
SELECT client_id, full_name, occupation, estimated_income
FROM banking_clients
WHERE occupation LIKE '%Engineer%'
ORDER BY estimated_income DESC;


-- Length of occupation title
SELECT occupation,
       COUNT(*)               AS clients,
       LENGTH(occupation)     AS title_length
FROM banking_clients
GROUP BY occupation, title_length
ORDER BY title_length DESC
LIMIT 10;


-- Concatenate a client label
SELECT client_id,
       CONCAT(full_name, ' (', nationality, ' - ', loyalty_class, ')') AS client_label
FROM banking_clients
LIMIT 20;


-- ============================================================
-- SECTION 11 : FINAL BUSINESS INSIGHT QUERIES
-- ============================================================

-- Q1  Which loyalty class generates the most total deposits?
SELECT loyalty_class,
       ROUND(SUM(bank_deposits), 2)  AS total_deposits,
       COUNT(*)                       AS client_count,
       ROUND(AVG(bank_deposits), 2)  AS avg_deposits
FROM banking_clients
GROUP BY loyalty_class
ORDER BY total_deposits DESC;


-- Q2  Are high-fee clients also high depositors?
SELECT fee_structure,
       COUNT(*)                        AS clients,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(risk_weighting), 2)   AS avg_risk
FROM banking_clients
GROUP BY fee_structure
ORDER BY avg_deposits DESC;


-- Q3  Multi-property owners — are they wealthier with lower risk?
SELECT properties_owned,
       COUNT(*)                        AS clients,
       ROUND(AVG(estimated_income), 2) AS avg_income,
       ROUND(AVG(bank_deposits), 2)    AS avg_deposits,
       ROUND(AVG(risk_weighting), 2)   AS avg_risk,
       ROUND(AVG(wealth_score), 2)     AS avg_wealth_score
FROM banking_clients
GROUP BY properties_owned
ORDER BY properties_owned DESC;


-- Q4  Superannuation savings vs income — by age group
SELECT age_group,
       COUNT(*)                                AS clients,
       ROUND(AVG(estimated_income), 2)         AS avg_income,
       ROUND(AVG(superannuation_savings), 2)   AS avg_super,
       ROUND(AVG(superannuation_savings /
             NULLIF(estimated_income,0) * 100), 2) AS super_pct_income
FROM banking_clients
GROUP BY age_group
ORDER BY age_group;


-- Q5  Bottom 10 clients by wealth score (most financially vulnerable)
SELECT client_id, full_name, nationality, estimated_income,
       bank_loans, credit_card_balance, wealth_score, risk_weighting
FROM banking_clients
ORDER BY wealth_score ASC
LIMIT 10;


-- Q6  Average savings account balance by fee structure and gender
SELECT fee_structure, gender,
       COUNT(*)                        AS clients,
       ROUND(AVG(saving_accounts), 2)  AS avg_savings
FROM banking_clients
GROUP BY fee_structure, gender
ORDER BY fee_structure, avg_savings DESC;


-- Q7  Clients with both high loans AND high deposits (complex profile)
SELECT client_id, full_name, bank_loans, bank_deposits,
       estimated_income, loyalty_class, risk_weighting
FROM banking_clients
WHERE bank_loans    > (SELECT AVG(bank_loans)    FROM banking_clients)
  AND bank_deposits > (SELECT AVG(bank_deposits) FROM banking_clients)
ORDER BY bank_loans DESC
LIMIT 20;


-- ============================================================
-- END OF PROJECT
-- ============================================================
