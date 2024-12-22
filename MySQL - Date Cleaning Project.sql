-- Data Cleansing

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standarize the Data
-- 3. Null Values and Blank Values
-- 4. Remove any Columns


CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs; 


SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, 
 industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
 ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

Select *
FROM layoffs_staging;

-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging;

UPDATE layoffs_staging
SET company = TRIM(company);

Select *
FROM layoffs_staging
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging
Set industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

Select DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging
ORDER BY 1;


Select DISTINCT country, TRIM(country)
FROM layoffs_staging
WHERE country LIKE 'United States%';


UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';



Select `date`
FROM layoffs_staging;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE; 



SELECT * 
FROM layoffs_staging;


SELECT * 
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging
WHERE industry IS NULL
OR industry = '';


UPDATE layoffs_staging
SET industry = NULL 
WHERE industry = '';


SELECT *
FROM layoffs_staging
WHERE company = 'Airbnb';



SELECT *
FROM layoffs_staging st1
JOIN layoffs_staging st2
	ON st1.company = st2.company
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;

UPDATE layoffs_staging st1
JOIN layoffs_staging st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE st1.industry IS NULL
AND st2.industry IS NOT NULL;




SELECT *
FROM layoffs_staging;


SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging;

