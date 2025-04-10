-- SQL Project: Data Cleaning on Layoffs Dataset
-- Goal: Clean and standardize the layoffs dataset for accurate analysis

USE world_layoffs;

SELECT * 
FROM layoffs;

-- Data Cleaning Goals:
-- 1️. Remove Duplicate Records  
-- 2️. Standardize the Data (trim spaces, fix inconsistencies)  
-- 3️. Handle Null or Blank Values  
-- 4️. Drop Irrelevant Columns  


-- STEP 1: Removing Duplicate Records

-- Creating a staging table for safe data cleaning
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- Copying all data from original to staging
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Identifying duplicate rows using ROW_NUMBER()
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num
FROM layoffs_staging;

-- A detailed duplicate check
WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Inspecting a duplicate company
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Attempting to delete duplicate rows directly (but won't work this way in MySQL)
-- So we create another staging table to properly remove them

-- Creating a new staging table with a row number column

CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
);

-- Inserting into new staging table with row numbers
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

-- Remove rows where row_num > 1 (i.e. duplicates)
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;



-- STEP 2: Standardize the Data

-- Trimming unwanted spaces from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Checking distinct industries
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Found entries like 'Crypto Currency', 'CryptoCurrency', etc., which all refer to the same domain.
-- Standardising crypto-related entries
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

-- Reviewing distinct locations and countries
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Found 'United States', 'United States.' which are same but '.' make a difference
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States';

-- Trimming trailing periods in country names
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Converting Date to Proper Format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Converting date string to actual DATE format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Altering the column to proper DATE datatype
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;



-- STEP 3: Handling Null Values

-- Finding rows with NULL total_laid_off
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

-- Checking rows with both total_laid_off and percentage_laid_off NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Converting empty industry values to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Finding and filling missing industries using known values from same company
SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Rechecking companies with missing industries
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;



-- STEP 4: Remove Irrelevant Rows & Columns

-- Finding rows with both layoff fields missing
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Removing those useless rows
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Dropping the helper column(row_num) used for duplicate detection
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final cleaned table
SELECT * 
FROM layoffs_staging2;

-- Data cleaning is completed. The dataset is now ready for analysis.
